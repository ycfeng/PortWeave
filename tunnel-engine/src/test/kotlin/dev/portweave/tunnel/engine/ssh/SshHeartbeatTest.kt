package dev.portweave.tunnel.engine.ssh

import dev.portweave.tunnel.engine.HeartbeatCoordinator
import dev.portweave.tunnel.engine.RuntimeWorkPolicy
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class SshHeartbeatTest {
    @Test fun answeredBeatsKeepTheSessionAndTeachTheCoordinator() = runTest {
        val policy = RuntimeWorkPolicy().apply { networkKey.value = 9L }
        val transport = FakeTransport(answer = true)
        val heartbeat = SshHeartbeat(transport, HeartbeatCoordinator(policy) { currentTime }, 30_000L, policy.counters)
        val job = launch { heartbeat.run() }
        runCurrent()
        // Background: the 60s rung, plus reply timeout and slack, is JSch's backstop.
        assertEquals(listOf(60_000 + 30_000 + 5_000), transport.backstops)
        advanceTimeBy(4 * 60_000L)
        runCurrent()
        assertEquals(4, transport.sent)
        assertFalse(transport.dead)
        // Four answered idle beats promote the network to the next rung.
        advanceTimeBy(90_000L)
        runCurrent()
        assertEquals(90_000 + 30_000 + 5_000, transport.backstops.last())
        assertEquals(1L, policy.counters.snapshot()["heartbeatPromotions"])
        job.cancel()
    }

    @Test fun anUnansweredBeatEndsTheSessionAndLowersTheCeiling() = runTest {
        val policy = RuntimeWorkPolicy()
        val transport = FakeTransport(answer = false)
        val heartbeat = SshHeartbeat(transport, HeartbeatCoordinator(policy) { currentTime }, 30_000L, policy.counters)
        val job = launch { heartbeat.run() }
        advanceTimeBy(60_000L + 30_000L)
        runCurrent()
        assertTrue(transport.dead)
        assertEquals(1, transport.sent)
        assertEquals(1L, policy.counters.snapshot()["sshKeepAliveTimeouts"])
        assertEquals(1L, policy.counters.snapshot()["heartbeatDemotions"])
        assertTrue(job.isCompleted)
    }

    @Test fun busyTunnelsDoNotTeachTheCoordinator() = runTest {
        val policy = RuntimeWorkPolicy()
        val transport = FakeTransport(answer = true).apply { idle = false }
        val job = launch { SshHeartbeat(transport, HeartbeatCoordinator(policy) { currentTime }, 30_000L, policy.counters).run() }
        advanceTimeBy(10 * 60_000L)
        runCurrent()
        assertEquals(null, policy.counters.snapshot()["heartbeatPromotions"])
        job.cancel()
    }

    private class FakeTransport(private val answer: Boolean) : SshKeepAliveTransport {
        var sent = 0
        var dead = false
        val backstops = mutableListOf<Int>()
        private var reads = 0L
        override val isConnected get() = !dead
        override var idle = true
        override fun readMark() = reads
        override suspend fun awaitReadAfter(mark: Long, timeoutMillis: Long): Boolean {
            if (answer) {
                reads += 1
                return true
            }
            return withTimeoutOrNull(timeoutMillis) { CompletableDeferred<Unit>().await() } != null
        }
        override fun sendKeepAlive() { sent += 1 }
        override fun setBackstopIntervalMillis(intervalMillis: Int) { backstops += intervalMillis }
        override fun declareDead() { dead = true }
    }
}
