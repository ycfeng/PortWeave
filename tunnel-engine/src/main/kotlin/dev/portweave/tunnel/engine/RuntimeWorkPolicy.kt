package dev.portweave.tunnel.engine

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeoutOrNull

/** Process-local counters, exported on demand rather than logged per operation. */
class RuntimeWorkCounters {
    private val counts = ConcurrentHashMap<String, AtomicLong>()
    fun increment(name: String) { counts.computeIfAbsent(name) { AtomicLong() }.incrementAndGet() }
    fun snapshot(): Map<String, Long> = counts.mapValues { it.value.get() }
}

class RuntimeWorkPolicy(val counters: RuntimeWorkCounters = RuntimeWorkCounters()) {
    val interactive = MutableStateFlow(true)
    val deviceIdle = MutableStateFlow(false)
    val visible = MutableStateFlow(false)

    /**
     * Fallback connection-state polling interval. High frequency is reserved for a visible UI; while the
     * app is in the background, vendor power monitors count every timer wakeup as "frequent refresh".
     */
    fun checkInterval(activeConnections: Int?): Long = when {
        visible.value -> VisibleCheckMillis
        interactive.value && activeConnections != 0 -> BackgroundActiveCheckMillis
        else -> BackgroundIdleCheckMillis
    }

    /** Waits for the next check; ends early when visibility, screen state or connection count changes the interval. */
    suspend fun awaitNextCheck(metrics: StateFlow<BackendMetrics>) {
        val interval = checkInterval(metrics.value.activeConnections)
        withTimeoutOrNull(interval) {
            combine(visible, interactive, metrics) { _, _, current -> checkInterval(current.activeConnections) }
                .first { it != interval }
        }
    }

    fun metricsInterval(): Long = if (visible.value) 1_000L else 5_000L

    companion object {
        const val VisibleCheckMillis = 500L
        const val BackgroundActiveCheckMillis = 5_000L
        const val BackgroundIdleCheckMillis = 30_000L
    }
}

/** A recoverable backend keeps its handle while replacing the underlying transport. */
data class BackendAvailability(val ready: Boolean = true, val epoch: Long = 0, val terminal: Boolean = false)

interface RecoveringBackendHandle {
    val availability: StateFlow<BackendAvailability>
}
