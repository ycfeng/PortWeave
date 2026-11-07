package dev.portweave.app.service

import android.content.Context
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.diagnostics.EventLevel
import dev.portweave.core.diagnostics.EventRepository
import dev.portweave.core.diagnostics.TunnelEvent
import dev.portweave.core.diagnostics.TunnelEventCode
import dev.portweave.core.model.DependencyGraph
import dev.portweave.core.model.DependencyGraphException
import dev.portweave.core.model.SshAuthMethod
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelSpec
import dev.portweave.core.security.CredentialStore
import dev.portweave.core.security.CredentialUnavailableException
import dev.portweave.tunnel.engine.BackendHandle
import dev.portweave.tunnel.engine.BlockReason
import dev.portweave.tunnel.engine.StartStage
import dev.portweave.tunnel.engine.TunnelError
import dev.portweave.tunnel.engine.TunnelErrorCode
import dev.portweave.tunnel.engine.TunnelState
import dev.portweave.tunnel.engine.orchestration.OrchestratedNodeStarter
import dev.portweave.tunnel.engine.orchestration.OrchestrationResult
import dev.portweave.tunnel.engine.orchestration.StartLease
import dev.portweave.tunnel.engine.orchestration.TunnelOrchestrator
import dev.portweave.tunnel.engine.ssh.SshBackend
import dev.portweave.tunnel.engine.ssh.SshBackendException
import dev.portweave.tunnel.engine.ssh.SshCredentials
import dev.portweave.tunnel.engine.stcp.FrpcStcpBackend
import dev.portweave.tunnel.engine.stcp.StcpBackendException
import dev.portweave.tunnel.engine.stcp.StcpCredentials
import java.util.UUID
import kotlin.math.min
import kotlin.random.Random
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import dev.portweave.tunnel.engine.RecoveringBackendHandle
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** App adapter around the dependency-aware engine orchestrator. */
class SshRuntimeRunner(
    context: Context?,
    private val scope: CoroutineScope,
    private val repository: ConfigRepository,
    private val credentialStore: CredentialStore,
    private val backend: SshBackend,
    private val stateStore: RuntimeStateStore,
    private val stcpBackend: FrpcStcpBackend? = null,
    private val randomDelayMillis: (Long) -> Long = { upper -> Random.nextLong(0, upper + 1) },
    networkAvailable: (() -> Boolean)? = null,
    private val networkStatus: StateFlow<NetworkStatus>? = null,
    private val eventRepository: EventRepository? = null,
    private val desiredRoots: suspend () -> Set<UUID> = { emptySet() },
    private val nowMillis: () -> Long = System::currentTimeMillis,
) {
    private val networkAvailableCheck = networkAvailable ?: {
        val manager = requireNotNull(context).getSystemService(ConnectivityManager::class.java)
        manager.activeNetwork?.let(manager::getNetworkCapabilities)
            ?.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET) == true
    }
    private val orchestrator = TunnelOrchestrator(
        scope = scope,
        configs = repository::snapshot,
        starter = OrchestratedNodeStarter(::startUntilReady),
    )
    private val watcherJobs = ConcurrentHashMap<UUID, Job>()
    private val recoveryJobs = ConcurrentHashMap<UUID, Job>()
    private val operations = ConcurrentHashMap<Pair<UUID, StartLease>, MutableSet<Job>>()
    val workCounters = stateStore.workCounters
    private val recoveryMutex = Mutex()
    private val stopVersions = ConcurrentHashMap<UUID, Long>()

    fun start(configId: UUID) {
        scope.launch { startAndAwait(configId) }
    }

    suspend fun startAndAwait(
        configId: UUID,
        lease: StartLease = StartLease.User,
        expectedStopVersion: Long? = null,
    ): Boolean {
        val job = currentCoroutineContext()[Job]!!
        val operationKey = configId to lease
        val jobs = operations.computeIfAbsent(operationKey) { ConcurrentHashMap.newKeySet() }
        jobs.add(job)
        try {
            var attempt = 0
            while (true) {
                currentCoroutineContext().ensureActive()
                if (expectedStopVersion != null &&
                    ((stopVersions[configId] ?: 0L) != expectedStopVersion || configId !in desiredRoots())) return false
                if (!networkAvailableCheck()) {
                    stateStore.update(configId, TunnelState.Blocked(BlockReason.NETWORK_UNAVAILABLE))
                    networkStatus?.first { it.available } ?: delay(2_000)
                    continue
                }
                val graphBeforeStart = DependencyGraph(repository.snapshot())
                runCatching { graphBeforeStart.resolve(configId) }.getOrDefault(emptyList()).forEach { dependency ->
                    val dependencyGeneration = orchestrator.generation(dependency.id)
                    val handle = dependencyGeneration?.let { orchestrator.handle(dependency.id, it) }
                    (handle as? RecoveringBackendHandle)?.let { recovering ->
                        if (!recovering.availability.value.ready) {
                            stateStore.update(configId, TunnelState.WaitingDependency(dependency.id))
                            recovering.availability.first { it.ready || it.terminal }
                        }
                    }
                }
                record(configId, 0, EventLevel.INFO, TunnelEventCode.START_REQUESTED, "start_requested")
                stateStore.update(configId, TunnelState.ResolvingDependencies)
                when (val result = orchestrator.start(configId, lease)) {
                    is OrchestrationResult.Started -> {
                        result.order.forEach { watch(it) }
                        return true
                    }
                    is OrchestrationResult.Rejected -> {
                        val backendFailure = result.cause as? RuntimeBackendException
                        val error = backendFailure?.tunnelError
                        val graph = DependencyGraph(repository.snapshot())
                        val retry = error?.retryable == true &&
                            runCatching { graph.resolve(configId).last().autoRecover }.getOrDefault(false) &&
                            (backendFailure?.failedConfigId?.let { repository.find(it) }?.autoRecover != false)
                        if (!retry) { publishRejection(configId, result.cause); return false }
                        attempt++
                        val cap = min(60_000L, 1_000L shl min(attempt - 1, 15))
                        val wait = randomDelayMillis(cap).coerceIn(0, cap)
                        stateStore.update(configId, TunnelState.Reconnecting(attempt, nowMillis() + wait))
                        record(configId, 0, EventLevel.WARNING, TunnelEventCode.RETRY_SCHEDULED, "retry_scheduled")
                        delay(wait)
                    }
                }
            }
        } finally {
            jobs.remove(job)
            if (jobs.isEmpty()) operations.remove(operationKey, jobs)
        }
    }

    private fun operationIds(): Set<UUID> = operations.keys.mapTo(mutableSetOf()) { it.first }

    private suspend fun cancelOperations(ids: Set<UUID>) {
        val self = currentCoroutineContext()[Job]
        ids.forEach { id ->
            recoveryJobs.remove(id)?.takeIf { it !== self }?.cancel()
            operations.filterKeys { it.first == id }.values.flatMap { it.toList() }
                .filter { it !== self }.forEach { it.cancel() }
        }
    }

    suspend fun release(configId: UUID, lease: StartLease) {
        val self = currentCoroutineContext()[Job]
        operations[configId to lease]?.toList()?.filter { it !== self }?.forEach { it.cancel() }
        val before = orchestrator.activeIds()
        orchestrator.release(configId, lease)
        val stopped = before - orchestrator.activeIds()
        stopped.forEach { id ->
            watcherJobs.remove(id)?.cancel()
            stateStore.update(id, TunnelState.Stopped)
        }
    }

    suspend fun stop(configId: UUID) {
        stopVersions.merge(configId, 1L, Long::plus)
        cancelOperations(setOf(configId))
        record(configId, 0, EventLevel.INFO, TunnelEventCode.STOP_REQUESTED, "stop_requested")
        stateStore.update(configId, TunnelState.Stopping)
        val before = orchestrator.activeIds()
        orchestrator.stop(configId)
        val after = orchestrator.activeIds()
        (before - after).forEach {
            watcherJobs.remove(it)?.cancel()
            stateStore.update(it, TunnelState.Stopped)
        }
        stateStore.update(configId, TunnelState.Stopped)
        record(configId, 0, EventLevel.INFO, TunnelEventCode.STOPPED, "stopped")
    }

    suspend fun stopTree(configId: UUID) {
        val snapshot = repository.snapshot()
        val graph = DependencyGraph(snapshot)
        val affected = graph.transitiveDependents(configId).map(TunnelConfig::id) + configId
        affected.forEach { stopVersions.merge(it, 1L, Long::plus) }
        cancelOperations(affected.toSet())
        affected.forEach { stateStore.update(it, TunnelState.Stopping) }
        affected.forEach { watcherJobs.remove(it)?.cancel() }
        val before = orchestrator.activeIds()
        orchestrator.stopTree(configId)
        (affected + (before - orchestrator.activeIds())).forEach {
            watcherJobs.remove(it)?.cancel()
            stateStore.update(it, TunnelState.Stopped)
        }
    }

    suspend fun stopAll() {
        (operationIds() + desiredRoots()).forEach { stopVersions.merge(it, 1L, Long::plus) }
        cancelOperations(operationIds() + recoveryJobs.keys.toSet())
        val active = orchestrator.activeIds() + operationIds() + stateStore.states.value.keys
        active.forEach { stateStore.update(it, TunnelState.Stopping) }
        orchestrator.stopAll()
        active.forEach { stateStore.update(it, TunnelState.Stopped) }
        watcherJobs.values.forEach(Job::cancel)
        watcherJobs.clear()
    }

    suspend fun onNetworkStatus(status: NetworkStatus) {
        cancelOperations(operationIds() + recoveryJobs.keys.toSet())
        val active = orchestrator.activeIds()
        val affected = active + desiredRoots()
        if (affected.isEmpty()) return
        stateStore.networkChanged(affected)
        affected.forEach { id ->
            record(id, status.generation, EventLevel.WARNING, TunnelEventCode.NETWORK_CHANGED, "network_changed")
        }
        recover(status.available, "network_changed")
    }

    suspend fun restoreDesired(): Set<UUID> {
        val roots = desiredRoots()
        return coroutineScope {
            roots.map { id ->
                async {
                    record(id, 0, EventLevel.INFO, TunnelEventCode.PROCESS_RESTORED, "process_restored")
                    id.takeUnless { startAndAwait(id) }
                }
            }.awaitAll().filterNotNull().toSet()
        }
    }

    suspend fun retryDesired(): Set<UUID> = recover(networkAvailableCheck(), "user_retry")

    private suspend fun startUntilReady(config: TunnelConfig, generation: Long): BackendHandle {
        stateStore.setGeneration(config.id, generation)
        stateStore.update(config.id, TunnelState.Starting(StartStage.VALIDATING))
        val ssh = config.spec as? TunnelSpec.Ssh
        if (ssh != null && ssh.credentials.hostFingerprint == null) {
            throw RuntimeBackendException(TunnelError(TunnelErrorCode.SECRET_MISSING, false, "host_key_confirmation_required"), config.id)
        }
        try {
            stateStore.update(config.id, TunnelState.Starting(StartStage.CONNECTING))
            workCounters.increment("backendStarts")
            val handle = startBackend(config)
            stateStore.updateMetrics(config.id, handle.capabilities, handle.metrics.value, generation)
            stateStore.update(config.id, TunnelState.Ready(nowMillis()))
            record(config.id, generation, EventLevel.INFO, TunnelEventCode.TUNNEL_READY, "tunnel_ready")
            val stopped = java.util.concurrent.atomic.AtomicBoolean()
            suspend fun stopHandle() {
                if (!stopped.compareAndSet(false, true)) return
                try { handle.stop() } finally {
                    workCounters.increment("backendStops")
                    if (stateStore.runtime.value[config.id]?.generation == generation) {
                        stateStore.updateMetrics(config.id, handle.capabilities, handle.metrics.value, generation)
                    }
                }
            }
            return if (handle is RecoveringBackendHandle) {
                object : BackendHandle by handle, RecoveringBackendHandle by handle {
                    override suspend fun stop() = stopHandle()
                }
            } else object : BackendHandle by handle {
                override suspend fun stop() = stopHandle()
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: CredentialUnavailableException) {
            throw RuntimeBackendException(TunnelError(TunnelErrorCode.SECRET_MISSING, false, "secret_missing"), config.id)
        } catch (failure: Exception) {
            val error = when (failure) {
                is SshBackendException -> failure.tunnelError
                is StcpBackendException -> failure.tunnelError
                else -> throw failure
            }
            throw RuntimeBackendException(error, config.id)
        }
    }

    private suspend fun startBackend(config: TunnelConfig): BackendHandle = when (val spec = config.spec) {
        is TunnelSpec.Ssh -> {
            val refs = spec.credentials
            val credentials = SshCredentials(
                password = if (spec.authMethod == SshAuthMethod.PASSWORD) {
                    credentialStore.read(refs.password ?: throw CredentialUnavailableException())
                } else null,
                privateKey = if (spec.authMethod == SshAuthMethod.PRIVATE_KEY) {
                    credentialStore.read(refs.privateKey ?: throw CredentialUnavailableException())
                } else null,
                passphrase = refs.passphrase?.let { credentialStore.read(it) },
                hostFingerprint = credentialStore.read(refs.hostFingerprint ?: throw CredentialUnavailableException()),
            )
            try { backend.start(config, credentials) } finally { credentials.close() }
        }
        is TunnelSpec.Stcp -> {
            val implementation = stcpBackend ?: throw StcpBackendException(
                TunnelError(TunnelErrorCode.CONFIG_INVALID, false, "stcp_backend_unavailable"),
            )
            val token = spec.tokenRef?.let { credentialStore.read(it) }
            val credentials = try {
                StcpCredentials(
                    token,
                    credentialStore.read(spec.secretKeyRef ?: throw CredentialUnavailableException()),
                )
            } catch (failure: Exception) {
                token?.fill(0)
                throw failure
            }
            try { implementation.start(config, credentials) } finally { credentials.close() }
        }
    }

    private fun publishRejection(configId: UUID, cause: Throwable) {
        val state = when (cause) {
            is DependencyGraphException.Missing -> TunnelState.Blocked(BlockReason.DEPENDENCY_MISSING)
            is DependencyGraphException.Cycle -> TunnelState.Blocked(BlockReason.DEPENDENCY_CYCLE)
            is RuntimeBackendException -> TunnelState.Failed(cause.tunnelError)
            else -> TunnelState.Failed(TunnelError(TunnelErrorCode.INTERNAL, false, "orchestration_failed"))
        }
        stateStore.update(configId, state)
        scope.launch {
            record(configId, 0, EventLevel.ERROR, TunnelEventCode.FAILED, (state as? TunnelState.Failed)?.error?.messageKey ?: "start_blocked")
        }
    }

    private suspend fun watch(configId: UUID) {
        val generation = orchestrator.generation(configId) ?: return
        val handle = orchestrator.handle(configId, generation) ?: return
        if (watcherJobs[configId]?.isActive == true) return
        watcherJobs.remove(configId)?.cancel()
        watcherJobs[configId] = scope.launch {
            val metricsJob = launch {
                handle.metrics.collectLatest { metrics ->
                    if (stateStore.runtime.value[configId]?.generation == generation) {
                        stateStore.updateMetrics(configId, handle.capabilities, metrics, generation)
                    }
                }
            }
            val availabilityJob = (handle as? RecoveringBackendHandle)?.let { recovering ->
                launch {
                    var lastEpoch: Long? = recovering.availability.value.epoch.takeIf { it > 0 }
                    recovering.availability.collect { status ->
                        if (orchestrator.generation(configId) != generation) return@collect
                        if (!status.ready) {
                            stateStore.updateIfGeneration(configId, generation, TunnelState.Reconnecting(1, null))
                        } else {
                            if (stateStore.states.value[configId] !is TunnelState.Ready) {
                                stateStore.updateIfGeneration(configId, generation, TunnelState.Ready(nowMillis()))
                            }
                            if (lastEpoch != null && lastEpoch != status.epoch) {
                                workCounters.increment("stcpTransportRecoveries")
                                scheduleRecovery(configId, generation, 0, descendantsOnly = true)
                            }
                            lastEpoch = status.epoch.takeIf { it > 0 }
                        }
                    }
                }
            }
            try {
                if (orchestrator.awaitConnectionLost(configId, generation)) {
                    val config = repository.find(configId)
                    if (config?.autoRecover == true && handle.failure?.retryable != false) {
                        val runtime = stateStore.runtime.value[configId]
                        val stable = runtime?.lastReadyAtEpochMillis?.let { nowMillis() - it >= StableConnectionMillis } == true
                        val attempt = if (stable) 1 else snapshotAttempt(configId) + 1
                        val cap = min(60_000L, 1_000L shl min(attempt - 1, 15))
                        val wait = if (networkAvailableCheck()) randomDelayMillis(cap).coerceIn(0, cap) else 0L
                        stateStore.update(
                            configId,
                            TunnelState.Reconnecting(attempt, wait.takeIf { it > 0 }?.let { nowMillis() + it }),
                        )
                        scheduleRecovery(configId, generation, wait)
                    } else {
                        val graph = DependencyGraph(repository.snapshot())
                        val affected = graph.transitiveDependents(configId).map { it.id }.toSet() + configId
                        cancelOperations(affected)
                        affected.filter { it != configId }.forEach {
                            watcherJobs.remove(it)?.cancel()
                            stateStore.update(it, TunnelState.Blocked(BlockReason.DEPENDENCY_UNAVAILABLE))
                        }
                        orchestrator.invalidate(affected)
                        stateStore.update(
                            configId,
                            TunnelState.Failed(handle.failure ?: TunnelError(TunnelErrorCode.NETWORK_UNAVAILABLE, false, "connection_lost")),
                        )
                    }
                }
            } finally {
                metricsJob.cancel()
                availabilityJob?.cancel()
            }
        }
    }

    private fun snapshotAttempt(configId: UUID): Int = stateStore.runtime.value[configId]?.reconnectCount ?: 0

    suspend fun shutdown() {
        cancelOperations(operationIds() + recoveryJobs.keys.toSet())
        watcherJobs.values.forEach(Job::cancel)
        watcherJobs.clear()
        val owned = orchestrator.activeIds().mapNotNull { id -> orchestrator.generation(id)?.let { id to it } }
        orchestrator.stopAll()
        owned.forEach { (id, generation) -> stateStore.updateIfGeneration(id, generation, TunnelState.Stopped) }
    }

    private fun scheduleRecovery(configId: UUID, generation: Long, wait: Long, descendantsOnly: Boolean = false) {
        workCounters.increment("recoveryRequests")
        val task = scope.launch(start = CoroutineStart.LAZY) {
            if (wait > 0) delay(wait)
            recover(networkAvailableCheck(), "connection_lost", configId, generation, descendantsOnly)
        }
        recoveryJobs.put(configId, task)?.cancel()
        task.invokeOnCompletion { recoveryJobs.remove(configId, task) }
        task.start()
    }

    private suspend fun recover(
        networkAvailable: Boolean,
        reason: String,
        failedId: UUID? = null,
        generation: Long? = null,
        descendantsOnly: Boolean = false,
    ): Set<UUID> {
        if (failedId == null) cancelOperations(operationIds() + recoveryJobs.keys.toSet())
        val rootVersions = mutableMapOf<UUID, Long>()
        val rootsToStart = recoveryMutex.withLock {
            if (failedId != null && orchestrator.generation(failedId) != generation) return@withLock emptySet<UUID>()
            val graph = DependencyGraph(repository.snapshot())
            if (failedId != null && runCatching {
                graph.resolve(failedId).dropLast(1).any { recoveryJobs[it.id]?.isActive == true }
            }.getOrDefault(false)) return@withLock emptySet<UUID>()
            val roots = desiredRoots()
            if (roots.isEmpty()) return@withLock emptySet<UUID>()
            val affected = if (failedId == null) orchestrator.activeIds() else
                graph.transitiveDependents(failedId).map { it.id }.toSet() +
                    if (descendantsOnly) emptySet() else setOf(failedId)
            val selected = roots.filterTo(mutableSetOf()) { root ->
                failedId == null || runCatching { graph.resolve(root).any { it.id in affected } }.getOrDefault(false)
            }
            if (failedId != null) cancelOperations(affected)
            affected.forEach { watcherJobs.remove(it)?.cancel() }
            workCounters.increment("recoveries")
            orchestrator.invalidate(affected)
            selected.filterTo(mutableSetOf()) { root ->
                val chain = runCatching { graph.resolve(root) }.getOrDefault(emptyList())
                when {
                    !networkAvailable -> {
                        stateStore.update(root, TunnelState.Blocked(BlockReason.NETWORK_UNAVAILABLE)); false
                    }
                    chain.isEmpty() || reason != "user_retry" &&
                        !chain.filter { it.id in affected }.all(TunnelConfig::autoRecover) -> {
                        stateStore.update(root, TunnelState.Failed(TunnelError(TunnelErrorCode.NETWORK_UNAVAILABLE, false, reason)))
                        false
                    }
                    else -> { rootVersions[root] = stopVersions[root] ?: 0L; true }
                }
            }
        }
        return coroutineScope {
            rootsToStart.map { root ->
                async { root.takeUnless { startAndAwait(root, expectedStopVersion = rootVersions.getValue(root)) } }
            }.awaitAll().filterNotNull().toSet()
        }
    }

    private suspend fun record(configId: UUID, generation: Long, level: EventLevel, code: String, key: String) {
        runCatching {
            eventRepository?.record(
                TunnelEvent(
                    configId = configId,
                    generation = generation,
                    level = level,
                    code = code,
                    messageKey = key,
                    elapsedRealtimeMillis = System.nanoTime() / 1_000_000L,
                    occurredAtEpochMillis = nowMillis(),
                ),
            )
        }
    }

    private companion object {
        /**
         * A connection must stay up this long before its reconnect backoff resets. A short window lets a
         * link that is reclaimed every minute or two (NAT timeout, weak signal) reconnect almost immediately
         * forever, which vendor power monitors flag as frequent background refresh.
         */
        const val StableConnectionMillis = 5 * 60_000L
    }
}

private class RuntimeBackendException(val tunnelError: TunnelError, val failedConfigId: UUID? = null) : Exception(tunnelError.messageKey)
