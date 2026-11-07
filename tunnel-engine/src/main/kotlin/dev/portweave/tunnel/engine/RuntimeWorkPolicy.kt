package dev.portweave.tunnel.engine

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.withTimeoutOrNull

/** Process-local counters, exported on demand rather than logged per operation. */
class RuntimeWorkCounters {
    private val counts = ConcurrentHashMap<String, AtomicLong>()
    fun increment(name: String) { counts.computeIfAbsent(name) { AtomicLong() }.incrementAndGet() }
    fun snapshot(): Map<String, Long> = counts.mapValues { it.value.get() }
}

/**
 * How much background work the device currently tolerates. Vendor power monitors judge an app by how often
 * it wakes up while nobody looks at it, so every tier below [VISIBLE] retries less often, and the quieter
 * tiers stop scheduling timers altogether after a run of failures ("dormant") until a wake trigger arrives.
 */
enum class RuntimeMode(val maximumRetryDelayMillis: Long, val dormantAfterFailures: Int?) {
    VISIBLE(60_000L, null),
    BACKGROUND(5 * 60_000L, 20),
    SCREEN_OFF(15 * 60_000L, 10),
    DEVICE_IDLE(15 * 60_000L, 1),
}

class RuntimeWorkPolicy(val counters: RuntimeWorkCounters = RuntimeWorkCounters()) {
    val interactive = MutableStateFlow(true)
    val deviceIdle = MutableStateFlow(false)
    val visible = MutableStateFlow(false)
    val powerSave = MutableStateFlow(false)
    private val networkChanges = MutableStateFlow(0L)

    val mode: RuntimeMode get() = modeOf(visible.value, interactive.value, deviceIdle.value, powerSave.value)

    val modes: Flow<RuntimeMode> =
        combine(visible, interactive, deviceIdle, powerSave) { v, i, d, p -> modeOf(v, i, d, p) }.distinctUntilChanged()

    /** Emits when reconnecting becomes worthwhile again: UI visible, screen on, Doze exit or a real network change. */
    val wakeTriggers: Flow<Unit> = merge(
        visible.drop(1).filter { it },
        interactive.drop(1).filter { it },
        deviceIdle.drop(1).filter { !it },
        networkChanges.drop(1),
    ).map { }

    /** Called once per settled default-network change, so dormant and backed-off retries try the new network. */
    fun onNetworkChanged() = networkChanges.update { it + 1 }

    /**
     * Delay before reconnect [attempt] (1-based count of consecutive failures), or null when the caller should go
     * dormant and wait for [wakeTriggers]. While visible the delay keeps full [jitter]; in the background, once the
     * ceiling reaches [RetryAlignMillis], the delay is the ceiling itself rounded up to a shared grid so that several
     * tunnels retrying at once share a single radio wakeup.
     */
    fun retryDelay(attempt: Int, nowMillis: Long, jitter: (Long) -> Long): Long? {
        val mode = mode
        if (mode.dormantAfterFailures?.let { attempt >= it } == true) return null
        val ceiling = min(mode.maximumRetryDelayMillis, 1_000L shl (attempt - 1).coerceIn(0, 20))
        if (mode == RuntimeMode.VISIBLE || ceiling < RetryAlignMillis) return jitter(ceiling).coerceIn(0, ceiling)
        return ceiling + Math.floorMod(-(nowMillis + ceiling), RetryAlignMillis)
    }

    /**
     * Waits out a delay from [retryDelay]. A dormant wait (null) ends only on a wake trigger. A timed wait also ends
     * early on a network change, or when the mode changes to one whose ceiling is shorter than the wait, so a user
     * opening the app is not left looking at a 15-minute background backoff.
     */
    suspend fun awaitRetry(waitMillis: Long?) {
        if (waitMillis == null) {
            counters.increment("dormantRetryWaits")
            wakeTriggers.first()
            return
        }
        if (waitMillis <= 0) return
        withTimeoutOrNull(waitMillis) {
            merge(
                modes.drop(1).filter { it.maximumRetryDelayMillis < waitMillis },
                networkChanges.drop(1),
            ).first()
        }
    }

    /**
     * [awaitRetry] for a transport that reconnects on its own (the STCP control connection). While visible it keeps
     * the transport's own [defaultDelayMillis]; elsewhere it follows the same tiers and dormancy as tunnel restarts.
     */
    suspend fun awaitTransportReconnect(attempt: Int, defaultDelayMillis: Long) {
        if (mode == RuntimeMode.VISIBLE) {
            awaitRetry(defaultDelayMillis)
            return
        }
        counters.increment("transportReconnectWaits")
        awaitRetry(retryDelay(attempt, System.currentTimeMillis()) { it }?.coerceAtLeast(defaultDelayMillis))
    }

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

    companion object {
        const val VisibleCheckMillis = 500L
        const val BackgroundActiveCheckMillis = 5_000L
        const val BackgroundIdleCheckMillis = 30_000L
        const val RetryAlignMillis = 30_000L

        private fun modeOf(visible: Boolean, interactive: Boolean, deviceIdle: Boolean, powerSave: Boolean) = when {
            visible -> RuntimeMode.VISIBLE
            deviceIdle -> RuntimeMode.DEVICE_IDLE
            !interactive || powerSave -> RuntimeMode.SCREEN_OFF
            else -> RuntimeMode.BACKGROUND
        }
    }
}

/** A recoverable backend keeps its handle while replacing the underlying transport. */
data class BackendAvailability(val ready: Boolean = true, val epoch: Long = 0, val terminal: Boolean = false)

interface RecoveringBackendHandle {
    val availability: StateFlow<BackendAvailability>

    /**
     * Re-dials only the network transport after the default network changed, keeping the local listener and
     * anything built on top of it. Returns false when the handle cannot do that and must be rebuilt instead.
     */
    suspend fun reconnectTransport(): Boolean = false
}
