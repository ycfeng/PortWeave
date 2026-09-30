package dev.portweave.tunnel.engine.orchestration

import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.tunnel.engine.BackendHandle
import dev.portweave.tunnel.engine.LocalEndpoint
import java.util.UUID
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.async
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.runCurrent
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class TunnelOrchestratorTest {
    @Test fun cancelledSharedStartKeepsTheOtherRootAndItsParentLease() = runTest {
        val parent = node(1)
        val left = node(2, parent.id)
        val right = node(3, parent.id)
        val gate = CompletableDeferred<Unit>()
        val starter = FakeStarter()
        val orchestrator = TunnelOrchestrator(backgroundScope, { listOf(parent, left, right) },
            OrchestratedNodeStarter { config, generation ->
                if (config.id == parent.id) gate.await()
                starter.start(config, generation)
            })
        val leftStart = async { orchestrator.start(left.id) }
        val rightStart = async { orchestrator.start(right.id) }
        runCurrent()
        orchestrator.stopTree(left.id)
        gate.complete(Unit)
        rightStart.await()
        runCurrent()
        assertTrue(leftStart.isCancelled)
        assertEquals(setOf(parent.id, right.id), orchestrator.activeIds())
        assertEquals(1, starter.started.count { it.id == parent.id })
        assertEquals(setOf(StartLease.Dependent(right.id)), orchestrator.leases(parent.id))
        orchestrator.stopAll()
    }

    @Test fun startsTopologicallyAndResolvesDependencyDialEndpoints() = runTest {
        val a = node(1)
        val b = node(2, a.id)
        val c = node(3, b.id)
        val starter = FakeStarter()
        val orchestrator = TunnelOrchestrator(this, { listOf(c, b, a) }, starter)

        val result = orchestrator.start(c.id) as OrchestrationResult.Started

        assertEquals(listOf(a.id, b.id, c.id), result.order)
        assertEquals(listOf(a.id, b.id, c.id), starter.started.map { it.id })
        assertEquals(EndpointSource.Remote("127.0.0.1", a.localPort), (starter.started[1].spec as TunnelSpec.Ssh).endpoint)
        assertEquals(EndpointSource.Remote("127.0.0.1", b.localPort), (starter.started[2].spec as TunnelSpec.Ssh).endpoint)
    }

    @Test fun concurrentStartIsSingleFlightAndSharedParentUsesDistinctLeases() = runTest {
        val parent = node(1)
        val left = node(2, parent.id)
        val right = node(3, parent.id)
        val starter = FakeStarter()
        val orchestrator = TunnelOrchestrator(this, { listOf(parent, left, right) }, starter)

        val first = async { orchestrator.start(left.id) }
        val second = async { orchestrator.start(left.id) }
        first.await(); second.await()
        orchestrator.start(right.id)

        assertEquals(1, starter.started.count { it.id == parent.id })
        assertEquals(
            setOf(StartLease.Dependent(left.id), StartLease.Dependent(right.id)),
            orchestrator.leases(parent.id),
        )
        orchestrator.stop(left.id)
        assertTrue(parent.id in orchestrator.activeIds())
        orchestrator.stop(right.id)
        assertFalse(parent.id in orchestrator.activeIds())
    }

    @Test fun failureRollsBackOnlyNodesStartedByTransaction() = runTest {
        val parent = node(1)
        val existing = node(2, parent.id)
        val failing = node(3, parent.id)
        val starter = FakeStarter()
        val orchestrator = TunnelOrchestrator(this, { listOf(parent, existing, failing) }, starter)
        orchestrator.start(existing.id)
        starter.failId = failing.id

        assertTrue(orchestrator.start(failing.id) is OrchestrationResult.Rejected)
        assertEquals(setOf(parent.id, existing.id), orchestrator.activeIds())
        assertFalse(starter.handles.getValue(parent.id).stopped)
    }

    @Test fun stoppingParentStopsDependentSubtreeLeafFirst() = runTest {
        val a = node(1)
        val b = node(2, a.id)
        val c = node(3, b.id)
        val starter = FakeStarter()
        val orchestrator = TunnelOrchestrator(this, { listOf(a, b, c) }, starter)
        orchestrator.start(c.id)

        orchestrator.stopTree(a.id)

        assertEquals(listOf(c.id, b.id, a.id), starter.stopOrder)
        assertTrue(orchestrator.activeIds().isEmpty())
    }

    @Test fun dependencyChainSurvivesOneThousandStartStopCyclesWithoutRetainedSessions() = runTest {
        val parent = node(1)
        val child = node(2, parent.id)
        val starter = FakeStarter()
        val orchestrator = TunnelOrchestrator(this, { listOf(parent, child) }, starter)

        repeat(1_000) {
            assertTrue(orchestrator.start(child.id) is OrchestrationResult.Started)
            orchestrator.stop(child.id)
            assertTrue(orchestrator.activeIds().isEmpty())
        }

        assertEquals(2_000, starter.started.size)
        assertEquals(2_000, starter.stopOrder.size)
    }

    @Test fun oldGenerationConnectionLossCannotAffectReplacementSession() = runTest {
        val config = node(1)
        val starter = FakeStarter()
        val orchestrator = TunnelOrchestrator(this, { listOf(config) }, starter)
        orchestrator.start(config.id)
        val oldGeneration = requireNotNull(orchestrator.generation(config.id))
        val waiter = async { orchestrator.awaitConnectionLost(config.id, oldGeneration) }
        runCurrent()

        orchestrator.stop(config.id)
        orchestrator.start(config.id)
        starter.handleHistory.first().loss.complete(Unit)

        assertFalse(waiter.await())
        assertTrue(requireNotNull(orchestrator.generation(config.id)) > oldGeneration)
        orchestrator.stopAll()
    }

    @Test fun temporaryDiscoveryLeaseStartsAndReleasesDependencyChain() = runTest {
        val parent = node(1)
        val dependency = node(2, parent.id)
        val starter = FakeStarter()
        val orchestrator = TunnelOrchestrator(this, { listOf(parent, dependency) }, starter)
        val lease = StartLease.HostKeyDiscovery(UUID.randomUUID())

        assertTrue(orchestrator.start(dependency.id, lease) is OrchestrationResult.Started)
        assertEquals(setOf(parent.id, dependency.id), orchestrator.activeIds())

        orchestrator.release(dependency.id, lease)

        assertTrue(orchestrator.activeIds().isEmpty())
        assertEquals(listOf(dependency.id, parent.id), starter.stopOrder)
    }

    @Test fun releasingTemporaryLeaseDoesNotStopManuallyRunningDependency() = runTest {
        val dependency = node(1)
        val starter = FakeStarter()
        val orchestrator = TunnelOrchestrator(this, { listOf(dependency) }, starter)
        val lease = StartLease.HostKeyDiscovery(UUID.randomUUID())
        orchestrator.start(dependency.id)
        orchestrator.start(dependency.id, lease)

        orchestrator.release(dependency.id, lease)

        assertEquals(setOf(dependency.id), orchestrator.activeIds())
        assertFalse(starter.handles.getValue(dependency.id).stopped)
    }

    @Test fun concurrentDifferentLeaseTypesAreBothAcquired() = runTest {
        val dependency = node(1)
        val starter = FakeStarter()
        val orchestrator = TunnelOrchestrator(this, { listOf(dependency) }, starter)
        val discovery = StartLease.HostKeyDiscovery(UUID.randomUUID())

        val manualStart = async { orchestrator.start(dependency.id) }
        val discoveryStart = async { orchestrator.start(dependency.id, discovery) }
        manualStart.await()
        discoveryStart.await()

        assertEquals(setOf(StartLease.User, discovery), orchestrator.leases(dependency.id))
        assertEquals(1, starter.started.size)
    }
}

private class FakeStarter : OrchestratedNodeStarter {
    val started = mutableListOf<TunnelConfig>()
    val handles = mutableMapOf<UUID, FakeHandle>()
    val handleHistory = mutableListOf<FakeHandle>()
    val stopOrder = mutableListOf<UUID>()
    var failId: UUID? = null
    override suspend fun start(config: TunnelConfig, generation: Long): BackendHandle {
        if (config.id == failId) error("expected failure")
        started += config
        return FakeHandle(config.id, config.localPort, stopOrder).also {
            handles[config.id] = it
            handleHistory += it
        }
    }
}

private class FakeHandle(
    private val id: UUID,
    port: Int,
    private val stopOrder: MutableList<UUID>,
) : BackendHandle {
    val loss = CompletableDeferred<Unit>()
    override val kind = TunnelKind.SSH
    override val localEndpoint = LocalEndpoint("127.0.0.1", port)
    override val isConnected = true
    var stopped = false
    override suspend fun awaitConnectionLost() { loss.await() }
    override suspend fun stop() { stopped = true; stopOrder += id }
}

private fun node(number: Int, dependency: UUID? = null): TunnelConfig {
    val id = UUID.fromString("00000000-0000-0000-0000-${number.toString().padStart(12, '0')}")
    val config = TunnelConfig.empty(TunnelKind.SSH, "node-$number", 20_000 + number, 1L).copy(id = id)
    return if (dependency == null) config else config.copy(
        spec = (config.spec as TunnelSpec.Ssh).copy(endpoint = EndpointSource.Dependency(dependency)),
    )
}
