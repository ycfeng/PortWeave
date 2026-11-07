package dev.portweave.tunnel.engine

import dev.portweave.core.data.settings.PowerProfile
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Decides when every tunnel sends its keepalive.
 *
 * - While the UI is visible, or under [PowerProfile.REALTIME], a tunnel keeps its configured interval.
 * - Otherwise the interval climbs a ladder (60s up to 4min) per network: a level is promoted after a few answered
 *   idle keepalives, and an unanswered idle keepalive means a NAT or firewall forgot the mapping sooner, so that level
 *   becomes the network's ceiling minus one. Too long an interval costs a full reconnect, so it is learned, not guessed.
 * - Beats land on a shared wall-clock grid, so several tunnels (SSH and STCP alike) wake the radio once per interval.
 */
class HeartbeatCoordinator internal constructor(
    private val policy: RuntimeWorkPolicy,
    private val nowMillis: () -> Long = System::currentTimeMillis,
) {
    private class Memory(var level: Int, var ceiling: Int = Ladder.lastIndex, var successes: Int = 0)

    private val memories = object : LinkedHashMap<Long?, Memory>(8, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Long?, Memory>?) = size > MaximumNetworks
    }

    /** The interval a tunnel configured with [configuredMillis] should use right now. */
    fun intervalMillis(configuredMillis: Long): Long {
        if (!adaptive()) return configuredMillis
        return maxOf(configuredMillis, Ladder[synchronized(this) { memory().level }])
    }

    /**
     * Suspends until the next beat on the shared grid for [intervalMillis]. Returns early when the UI becomes visible,
     * so a returning user gets a prompt liveness check instead of waiting out a long background interval.
     */
    suspend fun awaitBeat(intervalMillis: Long) {
        val wait = intervalMillis - Math.floorMod(nowMillis(), intervalMillis)
        withTimeoutOrNull(wait) { policy.visible.drop(1).first { it } }
    }

    /**
     * Feeds back whether a keepalive sent after [intervalMillis] of idleness was answered. Only background idle beats
     * teach anything about how long the network keeps a quiet connection alive.
     */
    fun onKeepAliveResult(answered: Boolean, intervalMillis: Long) {
        if (!adaptive()) return
        synchronized(this) {
            val memory = memory()
            val level = Ladder.indexOfLast { it <= intervalMillis }
            if (answered) {
                if (level < memory.level) return
                memory.successes += 1
                if (memory.successes >= promoteAfter() && memory.level < memory.ceiling) {
                    memory.level += 1
                    memory.successes = 0
                    policy.counters.increment("heartbeatPromotions")
                }
            } else {
                memory.ceiling = (level - 1).coerceAtLeast(0)
                memory.level = minOf(memory.level, memory.ceiling)
                memory.successes = 0
                policy.counters.increment("heartbeatDemotions")
            }
        }
    }

    private fun adaptive() = policy.profile.value != PowerProfile.REALTIME && policy.mode != RuntimeMode.VISIBLE

    private fun memory(): Memory = memories.getOrPut(policy.networkKey.value) {
        Memory(level = if (policy.profile.value == PowerProfile.SAVER) 1 else 0)
    }

    private fun promoteAfter() = if (policy.profile.value == PowerProfile.SAVER) 2 else 4

    companion object {
        /** Background keepalive intervals; NATs commonly forget idle TCP mappings after 2-5 minutes. */
        val Ladder = longArrayOf(60_000L, 90_000L, 120_000L, 180_000L, 240_000L)
        private const val MaximumNetworks = 8
    }
}
