package dev.portweave.tunnel.engine.ssh

import dev.portweave.tunnel.engine.RuntimeWorkPolicy
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class SshMetricsTrackerTest {
    @Test fun backgroundTrafficWaitsForTheUiAndVisibleTrafficIsCoalesced() = runTest {
        val policy = RuntimeWorkPolicy()
        val tracker = SshMetricsTracker(backgroundScope, policy)
        repeat(1_000) { tracker.uploaded(16_384) }
        advanceTimeBy(60 * 60_000L)
        runCurrent()
        // Background: no timer-driven publication at all.
        assertEquals(0L, tracker.metrics.value.bytesUploaded)
        policy.visible.value = true
        runCurrent()
        assertEquals(16_384_000L, tracker.metrics.value.bytesUploaded)
        tracker.uploaded(1)
        runCurrent()
        assertEquals(16_384_000L, tracker.metrics.value.bytesUploaded)
        advanceTimeBy(1_000)
        runCurrent()
        assertEquals(16_384_001L, tracker.metrics.value.bytesUploaded)
        val publications = policy.counters.snapshot()
        advanceTimeBy(60_000)
        runCurrent()
        assertEquals(publications, policy.counters.snapshot())
        tracker.downloaded(123)
        tracker.close()
        assertEquals(123L, tracker.metrics.value.bytesDownloaded)
        advanceTimeBy(60_000)
        assertEquals(123L, tracker.metrics.value.bytesDownloaded)
    }

    @Test fun connectionChangesAreImmediateAndVisibilityFlushesPendingTraffic() = runTest {
        val policy = RuntimeWorkPolicy()
        val tracker = SshMetricsTracker(backgroundScope, policy)
        tracker.connectionOpened()
        tracker.connectionOpened()
        tracker.connectionClosed()
        assertEquals(1, tracker.metrics.value.activeConnections)
        assertEquals(2, tracker.metrics.value.peakConnections)
        tracker.downloaded(7)
        runCurrent()
        policy.visible.value = true
        runCurrent()
        assertEquals(7L, tracker.metrics.value.bytesDownloaded)
        tracker.close()
    }
}
