package dev.portweave.tunnel.engine

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class RuntimeWorkPolicyTest {
    @Test fun idleScreenOffChecksSlowDownAndScreenOnInterruptsWait() = runTest {
        val policy = RuntimeWorkPolicy().apply { interactive.value = false }
        val metrics = MutableStateFlow(BackendMetrics(activeConnections = 0))
        var checks = 0
        val job = launch { while (true) { policy.awaitNextCheck(metrics); checks++ } }
        runCurrent()
        advanceTimeBy(60_000)
        runCurrent()
        assertEquals(12, checks)
        policy.interactive.value = true
        runCurrent()
        assertEquals(13, checks)
        advanceTimeBy(500)
        runCurrent()
        assertEquals(14, checks)
        job.cancel()
    }

    @Test fun activeAndUnknownConnectionCountsKeepOriginalInterval() {
        val policy = RuntimeWorkPolicy().apply { interactive.value = false }
        assertEquals(500L, policy.checkInterval(null))
        assertEquals(500L, policy.checkInterval(1))
        assertEquals(5_000L, policy.checkInterval(0))
    }
}
