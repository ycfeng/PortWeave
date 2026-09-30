package dev.portweave.app.service

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.IBinder
import java.util.UUID
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout

class TunnelServiceClient(context: Context) : ServiceConnection {
    private val applicationContext = context.applicationContext
    private var bound = false
    private val service = MutableStateFlow<TunnelForegroundService.LocalBinder?>(null)

    fun bind() {
        if (bound) return
        bound = applicationContext.bindService(
            Intent(applicationContext, TunnelForegroundService::class.java),
            this,
            Context.BIND_AUTO_CREATE,
        )
    }

    fun unbind() {
        if (!bound) return
        applicationContext.unbindService(this)
        service.value = null
        bound = false
    }

    suspend fun acquireHostKeyDiscovery(configId: UUID, requestId: UUID): Boolean {
        bind()
        return withTimeout(ServiceConnectionTimeoutMillis) {
            service.filterNotNull().first().acquireHostKeyDiscovery(configId, requestId)
        }
    }

    suspend fun releaseHostKeyDiscovery(configId: UUID, requestId: UUID) {
        withTimeout(ServiceConnectionTimeoutMillis) {
            service.filterNotNull().first().releaseHostKeyDiscovery(configId, requestId)
        }
    }

    suspend fun stopAndAwait(configId: UUID) {
        bind()
        withTimeout(ServiceConnectionTimeoutMillis) {
            service.filterNotNull().first().stopAndAwait(configId)
        }
    }

    override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
        this.service.value = service as? TunnelForegroundService.LocalBinder
    }

    override fun onServiceDisconnected(name: ComponentName?) {
        service.value = null
        bound = false
    }

    private companion object {
        const val ServiceConnectionTimeoutMillis = 10_000L
    }
}
