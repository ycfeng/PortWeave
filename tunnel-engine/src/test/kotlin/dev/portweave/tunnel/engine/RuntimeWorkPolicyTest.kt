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

    @Test fun modeFollowsVisibilityDozeScreenAndPowerSave() {
        val policy = RuntimeWorkPolicy()
        assertEquals(RuntimeMode.BACKGROUND, policy.mode)
        policy.powerSave.value = true
        assertEquals(RuntimeMode.SCREEN_OFF, policy.mode)
        policy.powerSave.value = false
        policy.interactive.value = false
        assertEquals(RuntimeMode.SCREEN_OFF, policy.mode)
        policy.deviceIdle.value = true
        assertEquals(RuntimeMode.DEVICE_IDLE, policy.mode)
        policy.visible.value = true
        assertEquals(RuntimeMode.VISIBLE, policy.mode)
    }

    @Test fun retryDelayUsesModeCeilingsAlignsLongBackgroundWaitsAndGoesDormant() {
        val policy = RuntimeWorkPolicy()
        val noJitter: (Long) -> Long = { 0 }
        // Short background waits keep jitter; long ones are the ceiling rounded up to the shared 30s grid.
        assertEquals(0L, policy.retryDelay(5, nowMillis = 0, jitter = noJitter))
        assertEquals(60_000L, policy.retryDelay(6, nowMillis = 0, jitter = noJitter))
        assertEquals(320_000L, policy.retryDelay(10, nowMillis = 10_000, jitter = noJitter))
        assertEquals(0L, (10_000L + 320_000L) % RuntimeWorkPolicy.RetryAlignMillis)
        assertEquals(null, policy.retryDelay(20, nowMillis = 0, jitter = noJitter))
        policy.interactive.value = false
        assertEquals(270_000L, policy.retryDelay(9, nowMillis = 0, jitter = noJitter))
        assertEquals(null, policy.retryDelay(10, nowMillis = 0, jitter = noJitter))
        policy.deviceIdle.value = true
        assertEquals(null, policy.retryDelay(1, nowMillis = 0, jitter = noJitter))
        policy.visible.value = true
        assertEquals(60_000L, policy.retryDelay(100, nowMillis = 7, jitter = { it }))
    }

    @Test fun dormantWaitEndsOnlyOnWakeTriggers() = runTest {
        val policy = RuntimeWorkPolicy().apply { interactive.value = false }
        var wakes = 0
        val job = launch { while (true) { policy.awaitRetry(null); wakes++ } }
        runCurrent()
        advanceTimeBy(24 * 60 * 60_000L)
        runCurrent()
        assertEquals(0, wakes)
        policy.interactive.value = true
        runCurrent()
        assertEquals(1, wakes)
        policy.interactive.value = false
        runCurrent()
        assertEquals(1, wakes)
        policy.onNetworkChanged()
        runCurrent()
        assertEquals(2, wakes)
        policy.visible.value = true
        runCurrent()
        assertEquals(3, wakes)
        job.cancel()
    }

    @Test fun timedWaitEndsEarlyOnlyWhenTheNewModeCeilingIsShorter() = runTest {
        val policy = RuntimeWorkPolicy()
        var done = false
        launch { policy.awaitRetry(200_000L); done = true }
        runCurrent()
        // Screen off allows longer waits, so it must not cut a background wait short.
        policy.interactive.value = false
        runCurrent()
        assertEquals(false, done)
        policy.visible.value = true
        runCurrent()
        assertEquals(true, done)
        done = false
        launch { policy.awaitRetry(30_000L); done = true }
        runCurrent()
        policy.visible.value = false
        runCurrent()
        assertEquals(false, done)
        advanceTimeBy(30_000L)
        runCurrent()
        assertEquals(true, done)
    }

    @Test fun transportReconnectKeepsFrpcTimingWhileVisibleAndPausesInDoze() = runTest {
        val policy = RuntimeWorkPolicy().apply { visible.value = true }
        var done = false
        launch { policy.awaitTransportReconnect(attempt = 3, defaultDelayMillis = 4_000L); done = true }
        advanceTimeBy(3_999L)
        runCurrent()
        assertEquals(false, done)
        advanceTimeBy(1L)
        runCurrent()
        assertEquals(true, done)
        policy.visible.value = false
        policy.deviceIdle.value = true
        done = false
        launch { policy.awaitTransportReconnect(attempt = 1, defaultDelayMillis = 1_000L); done = true }
        advanceTimeBy(60 * 60_000L)
        runCurrent()
        assertEquals(false, done)
        policy.deviceIdle.value = false
        runCurrent()
        assertEquals(true, done)
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
