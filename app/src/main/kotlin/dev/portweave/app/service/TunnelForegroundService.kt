package dev.portweave.app.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.Binder
import android.os.IBinder
import dev.portweave.app.MainActivity
import dev.portweave.app.PortWeaveApplication
import dev.portweave.app.R
import dev.portweave.tunnel.engine.TunnelState
import dev.portweave.tunnel.engine.orchestration.StartLease
import java.util.UUID
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

class TunnelForegroundService : Service() {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private lateinit var runner: SshRuntimeRunner
    private val binder = LocalBinder()
    private var notificationJob: Job? = null
    private lateinit var networkMonitor: NetworkMonitor
    private var networkJob: Job? = null
    private lateinit var powerMonitor: RuntimePowerMonitor
    private var lastNotificationModel: TunnelNotificationModel? = null
    private val container get() = (application as PortWeaveApplication).container

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
        networkMonitor = NetworkMonitor(this)
        powerMonitor = RuntimePowerMonitor(this, container.workPolicy)
        runner = SshRuntimeRunner(
            context = this,
            scope = scope,
            repository = container.configRepository,
            credentialStore = container.credentialStore,
            backend = container.sshTunnelBackend,
            stcpBackend = container.stcpBackend,
            stateStore = container.runtimeStateStore,
            eventRepository = container.eventRepository,
            desiredRoots = container.desiredRuntimeStore::snapshot,
            networkStatus = networkMonitor.status,
        )
        networkJob = scope.launch {
            networkMonitor.status.drop(1).collectLatest(runner::onNetworkStatus)
        }
        scope.launch {
            container.eventRepository.prune(container.settingsStore.settings.first().logRetentionDays)
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        enterForeground()
        when (intent?.action) {
            ActionStart -> intent.getStringExtra(ExtraConfigId)?.toUuidOrNull()?.let { id ->
                scope.launch {
                    container.desiredRuntimeStore.add(id)
                    if (!runner.startAndAwait(id)) {
                        container.desiredRuntimeStore.remove(id)
                        stopIfIdle()
                    }
                }
            }
            ActionStop -> intent.getStringExtra(ExtraConfigId)?.toUuidOrNull()?.let { id ->
                scope.launch {
                    stopAndAwait(id)
                }
            }
            ActionStopAll -> scope.launch {
                container.desiredRuntimeStore.clear()
                runner.stopAll()
                leaveForeground()
            }
            ActionRetry -> scope.launch {
                runner.retryDesired().forEach { container.desiredRuntimeStore.remove(it) }
                stopIfIdle()
            }
            else -> scope.launch {
                val desired = container.desiredRuntimeStore.snapshot()
                runner.restoreDesired().forEach { container.desiredRuntimeStore.remove(it) }
                if (desired.isEmpty()) stopIfIdle()
            }
        }
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder = binder

    inner class LocalBinder : Binder() {
        val states get() = container.runtimeStateStore.states

        suspend fun acquireHostKeyDiscovery(configId: UUID, requestId: UUID): Boolean =
            runner.startAndAwait(configId, StartLease.HostKeyDiscovery(requestId))

        suspend fun releaseHostKeyDiscovery(configId: UUID, requestId: UUID) {
            runner.release(configId, StartLease.HostKeyDiscovery(requestId))
            stopIfIdle()
        }

        suspend fun stopAndAwait(configId: UUID) {
            this@TunnelForegroundService.stopAndAwait(configId)
        }
    }

    override fun onDestroy() {
        networkJob?.cancel()
        networkMonitor.close()
        powerMonitor.close()
        CoroutineScope(SupervisorJob() + Dispatchers.IO).launch { runner.shutdown() }
        scope.cancel()
        super.onDestroy()
    }

    private fun enterForeground() {
        val model = TunnelNotificationModel.from(container.runtimeStateStore.states.value.values)
        if (lastNotificationModel == null) {
            val initial = notification(model)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
                startForeground(NotificationId, initial, ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE)
            } else {
                startForeground(NotificationId, initial)
            }
            container.workPolicy.counters.increment("foregroundEntries")
        } else if (model != lastNotificationModel) {
            notificationManager.notify(NotificationId, notification(model))
            container.workPolicy.counters.increment("notificationUpdates")
        }
        lastNotificationModel = model
        if (notificationJob == null) {
            val policy = container.workPolicy
            notificationJob = scope.launch(Dispatchers.Main.immediate) {
                // Nobody can see the notification while the screen is off, so updates wait for SCREEN_ON;
                // in the background they are coalesced so reconnect flapping does not count as frequent refresh.
                combine(
                    container.runtimeStateStore.states
                        .map { TunnelNotificationModel.from(it.values) }
                        .distinctUntilChanged(),
                    policy.interactive,
                ) { model, screenOn -> model.takeIf { screenOn } }
                    .filterNotNull()
                    .conflate()
                    .collect { next ->
                        if (!policy.interactive.value || next == lastNotificationModel) return@collect
                        lastNotificationModel = next
                        notificationManager.notify(NotificationId, notification(next))
                        policy.counters.increment("notificationUpdates")
                        if (!policy.visible.value) {
                            withTimeoutOrNull(BackgroundNotificationIntervalMillis) { policy.visible.first { it } }
                        }
                    }
            }
        }
    }

    private suspend fun leaveForeground() = withContext(Dispatchers.Main.immediate) {
        notificationJob?.cancel()
        notificationJob = null
        lastNotificationModel = null
        stopForeground(STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    private suspend fun stopIfIdle() {
        if (container.desiredRuntimeStore.snapshot().isEmpty()) {
            leaveForeground()
        }
    }

    private suspend fun stopAndAwait(configId: UUID) {
        val graph = dev.portweave.core.model.DependencyGraph(container.configRepository.snapshot())
        val affectedRoots = container.desiredRuntimeStore.snapshot().filter { rootId ->
            runCatching { graph.resolve(rootId) }.getOrDefault(emptyList()).any { it.id == configId }
        }
        affectedRoots.forEach { container.desiredRuntimeStore.remove(it) }
        runner.stopTree(configId)
        stopIfIdle()
    }

    private fun notification(model: TunnelNotificationModel): Notification {
        val ready = model.ready
        val active = model.active
        val openIntent = PendingIntent.getActivity(
            this,
            0,
            Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )
        val stopIntent = PendingIntent.getService(
            this,
            1,
            Intent(this, TunnelForegroundService::class.java).setAction(ActionStopAll),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )
        val retryIntent = PendingIntent.getService(
            this,
            2,
            Intent(this, TunnelForegroundService::class.java).setAction(ActionRetry),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )
        val builder = Notification.Builder(this, ChannelId)
            .setSmallIcon(R.drawable.ic_launcher)
            .setContentTitle("PortWeave · $active 个映射运行中")
            .setContentText("$ready 个就绪 · ${active - ready} 个正在连接或恢复")
            .setContentIntent(openIntent)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setVisibility(Notification.VISIBILITY_PRIVATE)
            .addAction(Notification.Action.Builder(null, "停止全部", stopIntent).build())
        if (model.recoverable) builder.addAction(Notification.Action.Builder(null, "立即重试", retryIntent).build())
        return builder.build()
    }

    private fun createNotificationChannel() {
        notificationManager.createNotificationChannel(
            NotificationChannel(ChannelId, "Port mappings", NotificationManager.IMPORTANCE_LOW).apply {
                description = "PortWeave 正在运行的本地端口映射"
                setShowBadge(false)
            },
        )
    }

    private val notificationManager get() = getSystemService(NotificationManager::class.java)

    companion object {
        private const val ActionStart = "dev.portweave.action.START"
        private const val ActionStop = "dev.portweave.action.STOP"
        private const val ActionStopAll = "dev.portweave.action.STOP_ALL"
        private const val ActionRetry = "dev.portweave.action.RETRY"
        private const val ExtraConfigId = "config_id"
        private const val ChannelId = "port_mappings"
        private const val NotificationId = 1001
        private const val BackgroundNotificationIntervalMillis = 10_000L

        fun start(context: Context, configId: UUID) {
            val intent = Intent(context, TunnelForegroundService::class.java)
                .setAction(ActionStart)
                .putExtra(ExtraConfigId, configId.toString())
            context.startForegroundService(intent)
        }

        fun stop(context: Context, configId: UUID) {
            val intent = Intent(context, TunnelForegroundService::class.java)
                .setAction(ActionStop)
                .putExtra(ExtraConfigId, configId.toString())
            context.startService(intent)
        }
    }
}

private fun String.toUuidOrNull(): UUID? = runCatching { UUID.fromString(this) }.getOrNull()
