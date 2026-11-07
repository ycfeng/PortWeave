package dev.portweave.tunnel.engine.ssh

import dev.portweave.tunnel.engine.BackendMetrics
import dev.portweave.tunnel.engine.RuntimeWorkPolicy
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

internal class SshMetricsTracker(
    private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default),
    var policy: RuntimeWorkPolicy = RuntimeWorkPolicy(),
) : AutoCloseable {
    private val uploaded = AtomicLong()
    private val downloaded = AtomicLong()
    private val active = AtomicInteger()
    private val peak = AtomicInteger()
    private val handshakeLatency = AtomicLong(-1)
    private val dirty = Channel<Unit>(Channel.CONFLATED)
    private val mutableMetrics = MutableStateFlow(BackendMetrics(0, 0, 0, 0))
    val metrics: StateFlow<BackendMetrics> = mutableMetrics
    private var closed = false
    private val publisher = scope.launch {
        for (signal in dirty) {
            if (policy.visible.value) {
                // Visible: coalesce traffic into one update per interval, flushing at once if the UI goes away.
                withTimeoutOrNull(VisiblePublishMillis) { policy.visible.first { !it } }
            } else {
                // Background: nobody reads byte counters, so no timer at all. They stay exact in the atomics and are
                // published when the UI comes back or the session closes; connection counts still publish at once.
                policy.visible.first { it }
            }
            synchronized(this@SshMetricsTracker) {
                dirty.tryReceive()
                if (!closed) publish()
            }
        }
    }

    @Synchronized fun uploaded(count: Long) {
        uploaded.addAndGet(count)
        if (closed) publish() else dirty.trySend(Unit)
    }

    @Synchronized fun downloaded(count: Long) {
        downloaded.addAndGet(count)
        if (closed) publish() else dirty.trySend(Unit)
    }

    @Synchronized fun connectionOpened() {
        val count = active.incrementAndGet()
        peak.updateAndGet { maxOf(it, count) }
        publish()
    }

    @Synchronized fun connectionClosed() {
        active.updateAndGet { (it - 1).coerceAtLeast(0) }
        publish()
    }

    @Synchronized fun handshakeLatency(millis: Long) {
        handshakeLatency.set(millis.coerceAtLeast(0))
        publish()
    }

    private fun publish() {
        val next = BackendMetrics(
            bytesUploaded = uploaded.get(),
            bytesDownloaded = downloaded.get(),
            activeConnections = active.get(),
            peakConnections = peak.get(),
            handshakeLatencyMillis = handshakeLatency.get().takeIf { it >= 0 },
        )
        if (next != mutableMetrics.value) {
            mutableMetrics.value = next
            policy.counters.increment("sshMetricsPublications")
        }
    }

    @Synchronized override fun close() {
        closed = true
        publish()
        dirty.close()
        publisher.cancel()
    }

    private companion object {
        const val VisiblePublishMillis = 1_000L
    }
}
