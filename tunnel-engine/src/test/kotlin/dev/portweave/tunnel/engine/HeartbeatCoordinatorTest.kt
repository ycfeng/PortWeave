package dev.portweave.tunnel.engine

import dev.portweave.core.data.settings.PowerProfile
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class HeartbeatCoordinatorTest {
    @Test fun visibleOrRealtimeKeepsTheConfiguredInterval() {
        val policy = RuntimeWorkPolicy().apply { visible.value = true }
        assertEquals(30_000L, policy.heartbeat.intervalMillis(30_000L))
        policy.visible.value = false
        policy.profile.value = PowerProfile.REALTIME
        assertEquals(30_000L, policy.heartbeat.intervalMillis(30_000L))
        policy.profile.value = PowerProfile.BALANCED
        assertEquals(60_000L, policy.heartbeat.intervalMillis(30_000L))
        // Never shorter than what the user configured.
        assertEquals(300_000L, policy.heartbeat.intervalMillis(300_000L))
    }

    @Test fun backgroundIntervalClimbsAfterAnsweredBeatsAndRemembersTheFailedLevelPerNetwork() {
        val policy = RuntimeWorkPolicy().apply { networkKey.value = 1L }
        val heartbeat = policy.heartbeat
        fun answer(times: Int) = repeat(times) { heartbeat.onKeepAliveResult(true, heartbeat.intervalMillis(30_000L)) }
        answer(4)
        assertEquals(90_000L, heartbeat.intervalMillis(30_000L))
        answer(4)
        assertEquals(120_000L, heartbeat.intervalMillis(30_000L))
        // 120s was too long for this NAT: fall back and never probe it again on this network.
        heartbeat.onKeepAliveResult(false, 120_000L)
        assertEquals(90_000L, heartbeat.intervalMillis(30_000L))
        answer(20)
        assertEquals(90_000L, heartbeat.intervalMillis(30_000L))
        assertEquals(1L, policy.counters.snapshot()["heartbeatDemotions"])
        // Another network starts from scratch; coming back restores what was learned.
        policy.networkKey.value = 2L
        assertEquals(60_000L, heartbeat.intervalMillis(30_000L))
        policy.networkKey.value = 1L
        assertEquals(90_000L, heartbeat.intervalMillis(30_000L))
    }

    @Test fun foregroundResultsTeachNothing() {
        val policy = RuntimeWorkPolicy().apply { visible.value = true }
        repeat(20) { policy.heartbeat.onKeepAliveResult(true, 30_000L) }
        policy.visible.value = false
        assertEquals(60_000L, policy.heartbeat.intervalMillis(30_000L))
    }

    @Test fun saverStartsHigherAndClimbsFaster() {
        val policy = RuntimeWorkPolicy().apply { profile.value = PowerProfile.SAVER }
        assertEquals(90_000L, policy.heartbeat.intervalMillis(30_000L))
        repeat(2) { policy.heartbeat.onKeepAliveResult(true, 90_000L) }
        assertEquals(120_000L, policy.heartbeat.intervalMillis(30_000L))
    }

    @Test fun beatsLandOnASharedGridAndAVisibleUiTriggersOneAtOnce() = runTest {
        val policy = RuntimeWorkPolicy()
        val heartbeat = HeartbeatCoordinator(policy) { currentTime }
        advanceTimeBy(10_000L)
        var beats = 0
        val beating = launch { while (true) { heartbeat.awaitBeat(60_000L); beats++ } }
        runCurrent()
        advanceTimeBy(49_999L)
        runCurrent()
        assertEquals(0, beats)
        advanceTimeBy(1L)
        runCurrent()
        assertEquals(1, beats)
        assertEquals(0L, currentTime % 60_000L)
        policy.visible.value = true
        runCurrent()
        assertEquals(2, beats)
        beating.cancel()
    }
}
