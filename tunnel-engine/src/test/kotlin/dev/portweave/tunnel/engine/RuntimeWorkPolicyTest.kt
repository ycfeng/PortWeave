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
    @Test fun backgroundIdleChecksSlowDownAndBecomingVisibleInterruptsWait() = runTest {
        val policy = RuntimeWorkPolicy().apply { interactive.value = false }
        val metrics = MutableStateFlow(BackendMetrics(activeConnections = 0))
        var checks = 0
        val job = launch { while (true) { policy.awaitNextCheck(metrics); checks++ } }
        runCurrent()
        advanceTimeBy(60_000)
        runCurrent()
        assertEquals(2, checks)
        policy.visible.value = true
        runCurrent()
        assertEquals(3, checks)
        advanceTimeBy(500)
        runCurrent()
        assertEquals(4, checks)
        job.cancel()
    }

    @Test fun screenOnInBackgroundDoesNotRestoreForegroundPolling() = runTest {
        val policy = RuntimeWorkPolicy().apply { interactive.value = false }
        val metrics = MutableStateFlow(BackendMetrics(activeConnections = 0))
        var checks = 0
        val job = launch { while (true) { policy.awaitNextCheck(metrics); checks++ } }
        runCurrent()
        policy.interactive.value = true
        runCurrent()
        advanceTimeBy(29_000)
        runCurrent()
        assertEquals(0, checks)
        job.cancel()
    }

    @Test fun closingLastBackgroundConnectionTriggersImmediateCheck() = runTest {
        val policy = RuntimeWorkPolicy()
        val metrics = MutableStateFlow(BackendMetrics(activeConnections = 1))
        var checks = 0
        val job = launch { while (true) { policy.awaitNextCheck(metrics); checks++ } }
        runCurrent()
        metrics.value = BackendMetrics(activeConnections = 0)
        runCurrent()
        assertEquals(1, checks)
        job.cancel()
    }

    @Test fun intervalsDependOnVisibilityScreenAndConnections() {
        val policy = RuntimeWorkPolicy()
        assertEquals(5_000L, policy.checkInterval(null))
        assertEquals(5_000L, policy.checkInterval(1))
        assertEquals(30_000L, policy.checkInterval(0))
        policy.interactive.value = false
        assertEquals(30_000L, policy.checkInterval(null))
        assertEquals(30_000L, policy.checkInterval(1))
        policy.visible.value = true
        assertEquals(500L, policy.checkInterval(0))
        assertEquals(500L, policy.checkInterval(1))
    }
}
