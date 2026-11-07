package dev.portweave.app.service

import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.diagnostics.EventRepository
import dev.portweave.core.diagnostics.TunnelEvent
import dev.portweave.core.diagnostics.TunnelEventCode
import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.SshAuthMethod
import dev.portweave.core.model.SshCredentialRefs
import dev.portweave.core.model.SshServerIdentity
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.core.security.CredentialKind
import dev.portweave.core.security.CredentialRef
import dev.portweave.core.security.CredentialStore
import dev.portweave.tunnel.engine.BackendAvailability
import dev.portweave.tunnel.engine.RecoveringBackendHandle
import dev.portweave.tunnel.engine.RuntimeMode
import dev.portweave.tunnel.engine.RuntimeWorkPolicy
import dev.portweave.tunnel.engine.BackendHandle
import dev.portweave.tunnel.engine.LocalEndpoint
import dev.portweave.tunnel.engine.TunnelError
import dev.portweave.tunnel.engine.TunnelErrorCode
import dev.portweave.tunnel.engine.TunnelState
import dev.portweave.tunnel.engine.orchestration.StartLease
import dev.portweave.tunnel.engine.ssh.DiscoveredHostKey
import dev.portweave.tunnel.engine.ssh.SshBackend
import dev.portweave.tunnel.engine.ssh.SshBackendException
import dev.portweave.tunnel.engine.ssh.SshCredentials
import java.util.UUID
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class SshRuntimeRunnerTest {
    @Test fun releasingHostKeyDiscoveryDoesNotCancelConcurrentUserStart() = runTest {
        val config = runtimeConfig()
        val backend = ControlledBackend().apply { startGate = CompletableDeferred() }
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(config),
            FakeCredentialStore(), backend, RuntimeStateStore(), networkAvailable = { true })
        val discovery = StartLease.HostKeyDiscovery(UUID.randomUUID())
        val userStart = async { runner.startAndAwait(config.id) }
        val discoveryStart = async { runner.startAndAwait(config.id, discovery) }
        runCurrent()
        runner.release(config.id, discovery)
        backend.startGate!!.complete(Unit)
        assertTrue(userStart.await())
        assertTrue(discoveryStart.isCancelled)
        assertEquals(1, backend.starts.count { it == config.id })
        runner.stopAll()
    }

    @Test fun restoreStartsIndependentRootWhileAnotherIsStillConnecting() = runTest {
        val blocked = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15311)
        val healthy = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15312)
        val backend = ControlledBackend().apply { blockedId = blocked.id }
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(blocked, healthy),
            FakeCredentialStore(), backend, RuntimeStateStore(), networkAvailable = { true },
            desiredRoots = { setOf(blocked.id, healthy.id) })
        val restoring = launch { runner.restoreDesired() }
        runCurrent()
        assertTrue(healthy.id in backend.handles)
        assertTrue(!backend.handles.getValue(healthy.id).stopped)
        runner.stopAll()
        restoring.cancel()
    }

    @Test fun manualRetryReplacesActiveSessionEvenWhenAutoRecoverIsDisabled() = runTest {
        val config = runtimeConfig().copy(autoRecover = false)
        val backend = ControlledBackend()
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(config),
            FakeCredentialStore(), backend, RuntimeStateStore(), networkAvailable = { true },
            desiredRoots = { setOf(config.id) })
        runner.startAndAwait(config.id)
        runCurrent()
        val original = backend.handles.getValue(config.id)
        assertTrue(runner.retryDesired().isEmpty())
        runCurrent()
        assertEquals(2, backend.starts.count { it == config.id })
        assertTrue(original.stopped)
        runner.stopAll()
    }

    @Test fun childRecoversWithHealthyAncestorThatHasAutoRecoverDisabled() = runTest {
        val parent = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15301, autoRecover = false)
        val child = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15302,
            spec = (runtimeConfig().spec as TunnelSpec.Ssh).copy(endpoint = EndpointSource.Dependency(parent.id)))
        val backend = ControlledBackend()
        val states = RuntimeStateStore()
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(parent, child),
            FakeCredentialStore(), backend, states, networkAvailable = { true },
            desiredRoots = { setOf(child.id) }, randomDelayMillis = { 0 })
        assertTrue(runner.startAndAwait(child.id))
        runCurrent()
        val existingParent = backend.handles.getValue(parent.id)
        backend.handles.getValue(child.id).lost.complete(Unit)
        runCurrent()
        assertEquals(1, backend.starts.count { it == parent.id })
        assertEquals(2, backend.starts.count { it == child.id })
        assertTrue(!existingParent.stopped)
        assertTrue(states.states.value[child.id] is TunnelState.Ready)
        runner.stopAll()
    }

    @Test fun internalTransportRecoveryKeepsParentAndRebuildsOnlyDependents() = runTest {
        val a = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15201)
        val b = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15202,
            spec = (runtimeConfig().spec as TunnelSpec.Ssh).copy(endpoint = EndpointSource.Dependency(a.id)))
        val backend = ControlledBackend()
        val states = RuntimeStateStore()
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(a, b),
            FakeCredentialStore(), backend, states, networkAvailable = { true },
            desiredRoots = { setOf(b.id) }, randomDelayMillis = { 0 })
        runner.startAndAwait(b.id)
        runCurrent()
        val parent = backend.handles.getValue(a.id)
        parent.availability.value = BackendAvailability(false, 1)
        backend.handles.getValue(b.id).lost.complete(Unit)
        runCurrent()
        assertEquals(1, backend.starts.count { it == b.id })
        parent.availability.value = BackendAvailability(true, 2)
        runCurrent()
        assertEquals(1, backend.starts.count { it == a.id })
        assertEquals(2, backend.starts.count { it == b.id })
        assertTrue(!parent.stopped)
        assertTrue(states.states.value[b.id] is TunnelState.Ready)
        runner.stopAll()
    }

    @Test fun simultaneousParentAndChildLossCoalesceIntoOneTreeRecovery() = runTest {
        val a = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15211)
        val b = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15212,
            spec = (runtimeConfig().spec as TunnelSpec.Ssh).copy(endpoint = EndpointSource.Dependency(a.id)))
        val backend = ControlledBackend()
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(a, b),
            FakeCredentialStore(), backend, RuntimeStateStore(), networkAvailable = { true },
            desiredRoots = { setOf(b.id) }, randomDelayMillis = { 0 })
        runner.startAndAwait(b.id)
        runCurrent()
        backend.handles.getValue(b.id).lost.complete(Unit)
        backend.handles.getValue(a.id).lost.complete(Unit)
        runCurrent()
        assertEquals(2, backend.starts.count { it == a.id })
        assertEquals(2, backend.starts.count { it == b.id })
        assertEquals(1L, runner.workCounters.snapshot()["recoveries"])
        runner.stopAll()
    }

    @Test fun localFailureLeavesSharedAncestorSiblingAndIndependentTunnelRunning() = runTest {
        val a = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15101)
        val b = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15102,
            spec = (runtimeConfig().spec as TunnelSpec.Ssh).copy(endpoint = EndpointSource.Dependency(a.id)))
        val c = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15103,
            spec = (runtimeConfig().spec as TunnelSpec.Ssh).copy(endpoint = EndpointSource.Dependency(a.id)))
        val d = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15104)
        val backend = ControlledBackend()
        val roots = mutableSetOf(b.id, c.id, d.id)
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(a, b, c, d),
            FakeCredentialStore(), backend, RuntimeStateStore(), networkAvailable = { true },
            desiredRoots = { roots.toSet() }, randomDelayMillis = { 0 })
        roots.forEach { runner.startAndAwait(it) }
        runCurrent()
        val originals = backend.handles.toMap()
        originals.getValue(b.id).lost.complete(Unit)
        runCurrent()
        assertEquals(2, backend.starts.count { it == b.id })
        listOf(a.id, c.id, d.id).forEach {
            assertEquals(1, backend.starts.count { id -> id == it })
            assertTrue(!originals.getValue(it).stopped)
        }
        originals.getValue(a.id).lost.complete(Unit)
        runCurrent()
        assertEquals(2, backend.starts.count { it == a.id })
        assertEquals(3, backend.starts.count { it == b.id })
        assertEquals(2, backend.starts.count { it == c.id })
        assertEquals(1, backend.starts.count { it == d.id })
        runner.stopAll()
    }

    @Test fun backoffResetsOnlyAfterConnectionStaysUpForFiveMinutes() = runTest {
        val config = runtimeConfig()
        val backend = ControlledBackend()
        val states = RuntimeStateStore()
        var now = 1_000_000L
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(config),
            FakeCredentialStore(), backend, states, networkAvailable = { true },
            desiredRoots = { setOf(config.id) }, randomDelayMillis = { 0 }, nowMillis = { now })
        assertTrue(runner.startAndAwait(config.id))
        runCurrent()

        backend.handles.getValue(config.id).lost.complete(Unit)
        runCurrent()
        assertEquals(1, states.runtime.value.getValue(config.id).reconnectCount)

        // Up for four minutes: the old 60 s window treated this as stable and reset the backoff.
        now += 4 * 60_000L
        backend.handles.getValue(config.id).lost.complete(Unit)
        runCurrent()
        assertEquals(2, states.runtime.value.getValue(config.id).reconnectCount)

        now += 5 * 60_000L
        backend.handles.getValue(config.id).lost.complete(Unit)
        runCurrent()
        assertEquals(2, states.runtime.value.getValue(config.id).reconnectCount)
        assertEquals(4, backend.starts.count { it == config.id })
        assertTrue(states.states.value[config.id] is TunnelState.Ready)
        runner.stopAll()
    }

    @Test fun backgroundRetriesGoDormantUntilTheAppBecomesVisible() = runTest {
        val config = runtimeConfig()
        val backend = FlakyBackend(failures = Int.MAX_VALUE)
        val states = RuntimeStateStore()
        val policy = RuntimeWorkPolicy(states.workCounters).apply { interactive.value = false }
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(config),
            FakeCredentialStore(), backend, states, networkAvailable = { true },
            randomDelayMillis = { 0 }, nowMillis = { 0L }, workPolicy = policy)
        val start = async { runner.startAndAwait(config.id) }
        advanceTimeBy(60 * 60_000L)
        runCurrent()
        // Screen off: ten attempts with growing, grid-aligned waits, then no timer at all.
        assertEquals(RuntimeMode.SCREEN_OFF.dormantAfterFailures, backend.startCount)
        val dormant = states.states.value.getValue(config.id) as TunnelState.Reconnecting
        assertTrue(dormant.dormant)
        assertEquals(null, dormant.nextRetryAtEpochMillis)
        advanceTimeBy(24 * 60 * 60_000L)
        runCurrent()
        assertEquals(10, backend.startCount)
        backend.failures = 0
        policy.visible.value = true
        runCurrent()
        assertTrue(start.await())
        assertEquals(11, backend.startCount)
        assertTrue(states.states.value[config.id] is TunnelState.Ready)
        runner.stopAll()
    }

    @Test fun lossBackoffRestartsFromOneAfterStablePeriodAndKeepsCountingFromThere() = runTest {
        val config = runtimeConfig()
        val backend = ControlledBackend()
        val states = RuntimeStateStore()
        var now = 1_000_000L
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(config),
            FakeCredentialStore(), backend, states, networkAvailable = { true },
            desiredRoots = { setOf(config.id) }, randomDelayMillis = { it }, nowMillis = { now })
        assertTrue(runner.startAndAwait(config.id)); runCurrent()
        fun loseAndObserve(): TunnelState.Reconnecting {
            backend.handles.getValue(config.id).lost.complete(Unit)
            runCurrent()
            return states.states.value.getValue(config.id) as TunnelState.Reconnecting
        }
        assertEquals(1, loseAndObserve().attempt)
        advanceTimeBy(1_000); runCurrent()
        now += 60_000L
        assertEquals(2, loseAndObserve().attempt)
        advanceTimeBy(2_000); runCurrent()
        now += 5 * 60_000L
        assertEquals(1, loseAndObserve().attempt)
        advanceTimeBy(1_000); runCurrent()
        now += 60_000L
        // The UI's reconnectCount is a high-water mark; backoff must not inherit it after a reset.
        assertEquals(2, loseAndObserve().attempt)
        runner.stopAll()
    }

    @Test fun networkSwitchReDialsCapableTransportsInPlaceAndRebuildsTheRest() = runTest {
        val inPlace = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15501)
        val rebuilt = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15502)
        val backend = ControlledBackend().apply { reconnectableIds += inPlace.id }
        val network = MutableStateFlow(NetworkStatus(true, 0, networkHandle = 1))
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(inPlace, rebuilt),
            FakeCredentialStore(), backend, RuntimeStateStore(), networkAvailable = { true },
            networkStatus = network, desiredRoots = { setOf(inPlace.id, rebuilt.id) })
        assertTrue(runner.startAndAwait(inPlace.id))
        assertTrue(runner.startAndAwait(rebuilt.id))
        runCurrent()
        val kept = backend.handles.getValue(inPlace.id)
        val replaced = backend.handles.getValue(rebuilt.id)
        runner.onNetworkStatus(NetworkStatus(true, 1, networkHandle = 2))
        runCurrent()
        assertEquals(1, kept.transportReconnects)
        assertTrue(!kept.stopped)
        assertEquals(1, backend.starts.count { it == inPlace.id })
        assertTrue(replaced.stopped)
        assertEquals(2, backend.starts.count { it == rebuilt.id })
        runner.stopAll()
    }

    @Test fun validationFlapOnTheSameNetworkKeepsTunnelsRunning() = runTest {
        val config = runtimeConfig()
        val backend = ControlledBackend()
        val states = RuntimeStateStore()
        val network = MutableStateFlow(NetworkStatus(true, 0, networkHandle = 7))
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(config),
            FakeCredentialStore(), backend, states, networkAvailable = { true },
            networkStatus = network, desiredRoots = { setOf(config.id) })
        assertTrue(runner.startAndAwait(config.id)); runCurrent()
        runner.onNetworkStatus(NetworkStatus(false, 1, networkHandle = 7))
        runner.onNetworkStatus(NetworkStatus(true, 2, networkHandle = 7))
        runCurrent()
        assertEquals(1, backend.starts.size)
        assertTrue(!backend.handles.getValue(config.id).stopped)
        assertTrue(states.states.value[config.id] is TunnelState.Ready)
        assertEquals(1L, states.workCounters.snapshot()["networkValidationLost"])
        // Losing the network itself still blocks the tunnels.
        runner.onNetworkStatus(NetworkStatus(false, 3, networkHandle = null))
        assertTrue(states.states.value[config.id] is TunnelState.Blocked)
        runner.stopAll()
    }

    @Test fun backgroundEventsAreThrottledPerCodeButVisibleOnesAreNot() = runTest {
        val config = runtimeConfig()
        val events = RecordingEvents()
        val states = RuntimeStateStore()
        val policy = RuntimeWorkPolicy(states.workCounters)
        fun runner(backend: SshBackend) = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(config),
            FakeCredentialStore(), backend, states, networkAvailable = { true }, eventRepository = events,
            randomDelayMillis = { 0 }, nowMillis = { 5_000L }, workPolicy = policy)
        val background = runner(FlakyBackend(failures = 3))
        assertTrue(background.startAndAwait(config.id))
        assertEquals(
            listOf(TunnelEventCode.START_REQUESTED, TunnelEventCode.RETRY_SCHEDULED, TunnelEventCode.TUNNEL_READY),
            events.recorded.map(TunnelEvent::code),
        )
        assertEquals(5L, states.workCounters.snapshot()["suppressedEvents"])
        background.stopAll()
        events.recorded.clear()
        policy.visible.value = true
        val visible = runner(FlakyBackend(failures = 3))
        assertTrue(visible.startAndAwait(config.id))
        assertEquals(8, events.recorded.size)
        visible.stopAll()
    }

    @Test fun parkedStandbyShowsAsReadyStandbyWithoutTouchingReadiness() = runTest {
        val config = runtimeConfig()
        val backend = ControlledBackend()
        val states = RuntimeStateStore()
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(config),
            FakeCredentialStore(), backend, states, networkAvailable = { true })
        assertTrue(runner.startAndAwait(config.id)); runCurrent()
        val since = (states.states.value.getValue(config.id) as TunnelState.Ready).sinceEpochMillis
        backend.handles.getValue(config.id).standby.value = true
        runCurrent()
        assertEquals(TunnelState.Ready(since, standby = true), states.states.value[config.id])
        backend.handles.getValue(config.id).standby.value = false
        runCurrent()
        assertEquals(TunnelState.Ready(since), states.states.value[config.id])
        assertEquals(1, backend.starts.size)
        runner.stopAll()
    }

    @Test fun stopDuringRecoveryDelayNeverRestartsTunnel() = runTest {
        val config = runtimeConfig()
        val backend = ControlledBackend()
        val roots = mutableSetOf(config.id)
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(config),
            FakeCredentialStore(), backend, RuntimeStateStore(), networkAvailable = { true },
            desiredRoots = { roots.toSet() }, randomDelayMillis = { it })
        runner.startAndAwait(config.id)
        runCurrent()
        backend.handles.getValue(config.id).lost.complete(Unit)
        runCurrent()
        roots.clear()
        runner.stopAll()
        advanceTimeBy(120_000)
        runCurrent()
        assertEquals(1, backend.starts.size)
    }

    @Test fun pendingConnectionDoesNotBlockIndependentStartOrStop() = runTest {
        val stuck = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15111)
        val healthy = runtimeConfig().copy(id = UUID.randomUUID(), localPort = 15112)
        val backend = ControlledBackend().apply { blockedId = stuck.id }
        val runner = SshRuntimeRunner(null, backgroundScope, FakeConfigRepository(stuck, healthy),
            FakeCredentialStore(), backend, RuntimeStateStore(), networkAvailable = { true })
        val pending = launch { runner.startAndAwait(stuck.id) }
        runCurrent()
        assertTrue(runner.startAndAwait(healthy.id))
        runner.stop(stuck.id)
        runCurrent()
        assertTrue(pending.isCancelled)
        assertTrue(!backend.handles.getValue(healthy.id).stopped)
        runner.stopAll()
    }

    @Test
    fun multiLevelDependencyChainStartsParentFirstAndStopsLeafFirst() = runTest {
        val parent = runtimeConfig().copy(name = "parent", localPort = 15431)
        val middle = runtimeConfig().copy(
            id = UUID.randomUUID(),
            name = "middle",
            localPort = 15432,
            spec = (runtimeConfig().spec as TunnelSpec.Ssh).copy(
                endpoint = EndpointSource.Dependency(parent.id),
            ),
        )
        val leaf = runtimeConfig().copy(
            id = UUID.randomUUID(),
            name = "leaf",
            localPort = 15433,
            spec = (runtimeConfig().spec as TunnelSpec.Ssh).copy(
                endpoint = EndpointSource.Dependency(middle.id),
            ),
        )
        val backend = RecordingBackend()
        val runner = SshRuntimeRunner(
            context = null,
            scope = this,
            repository = FakeConfigRepository(parent, middle, leaf),
            credentialStore = FakeCredentialStore(),
            backend = backend,
            stateStore = RuntimeStateStore(),
            networkAvailable = { true },
        )

        assertTrue(runner.startAndAwait(leaf.id))
        assertEquals(listOf(parent.id, middle.id, leaf.id), backend.started.map(TunnelConfig::id))
        assertEquals(
            EndpointSource.Remote("127.0.0.1", parent.localPort),
            (backend.started[1].spec as TunnelSpec.Ssh).endpoint,
        )
        assertEquals(
            EndpointSource.Remote("127.0.0.1", middle.localPort),
            (backend.started[2].spec as TunnelSpec.Ssh).endpoint,
        )

        runner.stopTree(parent.id)
        assertEquals(listOf(leaf.id, middle.id, parent.id), backend.stopped)
    }

    @Test
    fun retryableFailureAutomaticallyReconnectsAndCanStopCleanly() = runTest {
        val config = runtimeConfig()
        val backend = RetryOnceBackend()
        val states = RuntimeStateStore()
        val runner = SshRuntimeRunner(
            context = null,
            scope = this,
            repository = FakeConfigRepository(config),
            credentialStore = FakeCredentialStore(),
            backend = backend,
            stateStore = states,
            randomDelayMillis = { 0L },
            networkAvailable = { true },
        )

        runner.start(config.id)
        advanceUntilIdle()

        assertEquals(2, backend.startCount)
        assertTrue(states.states.value[config.id] is TunnelState.Ready)

        runner.stop(config.id)

        assertTrue(backend.handle.stopped)
        assertEquals(TunnelState.Stopped, states.states.value[config.id])
    }

    @Test
    fun permanentHostKeyMismatchDoesNotRetry() = runTest {
        val config = runtimeConfig()
        val backend = PermanentFailureBackend()
        val states = RuntimeStateStore()
        val runner = SshRuntimeRunner(
            context = null,
            scope = this,
            repository = FakeConfigRepository(config),
            credentialStore = FakeCredentialStore(),
            backend = backend,
            stateStore = states,
            networkAvailable = { true },
        )

        runner.start(config.id)
        advanceUntilIdle()

        assertEquals(1, backend.startCount)
        val failure = states.states.value[config.id] as TunnelState.Failed
        assertEquals(TunnelErrorCode.SSH_HOST_KEY_MISMATCH, failure.error.code)
    }

    @Test
    fun networkGenerationStopsOldSessionsAndRestoresDesiredRoot() = runTest {
        val config = runtimeConfig()
        val backend = RecordingBackend()
        val states = RuntimeStateStore()
        var available = true
        val runner = SshRuntimeRunner(
            context = null,
            scope = this,
            repository = FakeConfigRepository(config),
            credentialStore = FakeCredentialStore(),
            backend = backend,
            stateStore = states,
            networkAvailable = { available },
            desiredRoots = { setOf(config.id) },
        )

        assertTrue(runner.startAndAwait(config.id))
        val firstGeneration = states.runtime.value.getValue(config.id).generation
        available = false
        runner.onNetworkStatus(NetworkStatus(false, 1))
        assertTrue(states.states.value[config.id] is TunnelState.Blocked)
        available = true
        runner.onNetworkStatus(NetworkStatus(true, 2))

        assertEquals(2, backend.started.size)
        assertTrue(states.runtime.value.getValue(config.id).generation > firstGeneration)
        assertTrue(states.states.value[config.id] is TunnelState.Ready)
        assertEquals(2, states.runtime.value.getValue(config.id).networkChanges)
        runner.stopAll()
    }

    @Test
    fun processRestoreRecomputesAndStartsPersistedDependencyChain() = runTest {
        val parent = runtimeConfig().copy(name = "parent", localPort = 15431)
        val child = runtimeConfig().copy(
            id = UUID.randomUUID(),
            name = "child",
            spec = (runtimeConfig().spec as TunnelSpec.Ssh).copy(endpoint = EndpointSource.Dependency(parent.id)),
        )
        val backend = RecordingBackend()
        val runner = SshRuntimeRunner(
            context = null,
            scope = this,
            repository = FakeConfigRepository(parent, child),
            credentialStore = FakeCredentialStore(),
            backend = backend,
            stateStore = RuntimeStateStore(),
            networkAvailable = { true },
            desiredRoots = { setOf(child.id) },
        )

        runner.restoreDesired()

        assertEquals(listOf(parent.id, child.id), backend.started.map(TunnelConfig::id))
        runner.stopAll()
    }
}

private class FlakyBackend(var failures: Int) : SshBackend {
    var startCount = 0
    override suspend fun discoverHostKey(config: TunnelConfig) = DiscoveredHostKey("ssh-rsa", Fingerprint)
    override suspend fun start(config: TunnelConfig, credentials: SshCredentials): BackendHandle {
        startCount += 1
        if (failures > 0) {
            failures -= 1
            throw SshBackendException(TunnelError(TunnelErrorCode.CONNECTION_REFUSED, true, "refused"))
        }
        return FakeHandle()
    }
}

private class RecordingEvents : EventRepository {
    val recorded = mutableListOf<TunnelEvent>()
    override fun observeRecent(limit: Int): Flow<List<TunnelEvent>> = MutableStateFlow(recorded.toList())
    override fun observeForConfig(configId: UUID, limit: Int): Flow<List<TunnelEvent>> = observeRecent(limit)
    override suspend fun recent(limit: Int): List<TunnelEvent> = recorded.toList()
    override suspend fun record(event: TunnelEvent) { recorded += event }
    override suspend fun clear(configId: UUID?) { recorded.clear() }
    override suspend fun prune(retentionDays: Int) = Unit
}

private class RetryOnceBackend : SshBackend {
    var startCount = 0
    val handle = FakeHandle()
    override suspend fun discoverHostKey(config: TunnelConfig) = DiscoveredHostKey("ssh-rsa", Fingerprint)
    override suspend fun start(config: TunnelConfig, credentials: SshCredentials): BackendHandle {
        startCount += 1
        if (startCount == 1) {
            throw SshBackendException(TunnelError(TunnelErrorCode.CONNECTION_REFUSED, true, "refused"))
        }
        return handle
    }
}

private class PermanentFailureBackend : SshBackend {
    var startCount = 0
    override suspend fun discoverHostKey(config: TunnelConfig) = DiscoveredHostKey("ssh-rsa", Fingerprint)
    override suspend fun start(config: TunnelConfig, credentials: SshCredentials): BackendHandle {
        startCount += 1
        throw SshBackendException(
            TunnelError(TunnelErrorCode.SSH_HOST_KEY_MISMATCH, false, "mismatch"),
        )
    }
}

private class FakeHandle : BackendHandle {
    override val kind = TunnelKind.SSH
    override val localEndpoint = LocalEndpoint("127.0.0.1", 15432)
    override val isConnected = true
    var stopped = false
    override suspend fun awaitConnectionLost(): Nothing = awaitCancellation()
    override suspend fun stop() { stopped = true }
}

private class FakeConfigRepository(vararg configs: TunnelConfig) : ConfigRepository {
    private val all = MutableStateFlow(configs.toList())
    override val configs: Flow<List<TunnelConfig>> = all
    override suspend fun find(id: UUID): TunnelConfig? = all.value.firstOrNull { it.id == id }
    override suspend fun save(config: TunnelConfig) {
        all.value = all.value.filterNot { it.id == config.id } + config
    }
    override suspend fun delete(id: UUID) { all.value = emptyList() }
}

private class RecordingBackend : SshBackend {
    val started = mutableListOf<TunnelConfig>()
    val stopped = mutableListOf<UUID>()
    override suspend fun discoverHostKey(config: TunnelConfig) = DiscoveredHostKey("ssh-rsa", Fingerprint)
    override suspend fun start(config: TunnelConfig, credentials: SshCredentials): BackendHandle {
        started += config
        return object : BackendHandle {
            override val kind = TunnelKind.SSH
            override val localEndpoint = LocalEndpoint("127.0.0.1", config.localPort)
            override val isConnected = true
            override suspend fun awaitConnectionLost(): Nothing = awaitCancellation()
            override suspend fun stop() { stopped += config.id }
        }
    }
}

private class FakeCredentialStore : CredentialStore {
    override suspend fun put(kind: CredentialKind, value: ByteArray) = CredentialRef(UUID.randomUUID())
    override suspend fun read(ref: CredentialRef): ByteArray = when (ref) {
        PasswordRef -> "password".toByteArray()
        FingerprintRef -> Fingerprint.toByteArray()
        else -> error("unexpected credential")
    }
    override suspend fun delete(ref: CredentialRef) = Unit
    override suspend fun rotate(ref: CredentialRef, kind: CredentialKind, value: ByteArray) =
        CredentialRef(UUID.randomUUID())
}

private val PasswordRef = CredentialRef(UUID.fromString("00000000-0000-0000-0000-000000000001"))
private val FingerprintRef = CredentialRef(UUID.fromString("00000000-0000-0000-0000-000000000002"))
private const val Fingerprint = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

private fun runtimeConfig(): TunnelConfig = TunnelConfig.empty(
    TunnelKind.SSH,
    "Runtime",
    15432,
    1L,
).copy(
    spec = TunnelSpec.Ssh(
        endpoint = EndpointSource.Remote("ssh.example.test", 22),
        identity = SshServerIdentity("ssh.example.test", 22),
        username = "user",
        authMethod = SshAuthMethod.PASSWORD,
        credentials = SshCredentialRefs(password = PasswordRef, hostFingerprint = FingerprintRef),
        remoteTargetHost = "db.internal",
        remoteTargetPort = 5432,
    ),
)

private class ControlledBackend : SshBackend {
    val starts = mutableListOf<UUID>()
    val handles = mutableMapOf<UUID, ControlledHandle>()
    val reconnectableIds = mutableSetOf<UUID>()
    var blockedId: UUID? = null
    var startGate: CompletableDeferred<Unit>? = null
    override suspend fun discoverHostKey(config: TunnelConfig) = DiscoveredHostKey("ssh-rsa", Fingerprint)
    override suspend fun start(config: TunnelConfig, credentials: SshCredentials): BackendHandle {
        starts += config.id
        if (config.id == blockedId) awaitCancellation()
        startGate?.await()
        return ControlledHandle(config.localPort, config.id in reconnectableIds).also { handles[config.id] = it }
    }
}

private class ControlledHandle(port: Int, private val reconnectable: Boolean = false) :
    BackendHandle, RecoveringBackendHandle {
    override val availability = MutableStateFlow(BackendAvailability(epoch = 1))
    val lost = CompletableDeferred<Unit>()
    var stopped = false
    var transportReconnects = 0
    override val standby = MutableStateFlow(false)
    override suspend fun reconnectTransport(): Boolean {
        if (!reconnectable) return false
        transportReconnects += 1
        return true
    }
    override val kind = TunnelKind.SSH
    override val localEndpoint = LocalEndpoint("127.0.0.1", port)
    override val isConnected get() = !stopped && !lost.isCompleted
    override suspend fun awaitConnectionLost() { lost.await() }
    override suspend fun stop() { stopped = true }
}
