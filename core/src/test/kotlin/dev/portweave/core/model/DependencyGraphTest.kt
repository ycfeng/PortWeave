package dev.portweave.core.model

import java.util.UUID
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class DependencyGraphTest {
    @Test fun noDependencyAndLongChainResolveParentFirst() {
        val a = config("1")
        val b = config("2", a.id)
        val c = config("3", b.id)
        val graph = DependencyGraph(listOf(c, a, b))
        graph.validate()
        assertEquals(listOf(a.id, b.id, c.id), graph.resolve(c.id).map(TunnelConfig::id))
    }

    @Test fun selfDependencyReturnsClosedPath() {
        val a = config("1")
        val cyclic = a.withDependency(a.id)
        assertEquals(listOf(a.id, a.id), DependencyGraph(listOf(cyclic)).findCycle())
    }

    @Test fun twoAndThreeNodeCyclesReturnStableCompletePaths() {
        val a = config("1")
        val b = config("2", a.id)
        val two = DependencyGraph(listOf(a.withDependency(b.id), b)).findCycle()
        assertEquals(listOf(a.id, b.id, a.id), two)

        val c = config("3", b.id)
        val three = DependencyGraph(listOf(c, b, a.withDependency(c.id))).findCycle()
        assertEquals(listOf(a.id, c.id, b.id, a.id), three)
    }

    @Test(expected = DependencyGraphException.Missing::class)
    fun danglingDependencyIsRejected() {
        DependencyGraph(listOf(config("1", id("9")))).validate()
    }

    @Test fun sharedParentIndependentComponentsAndCandidateChecksWork() {
        val a = config("1")
        val b = config("2", a.id)
        val c = config("3", a.id)
        val d = config("4")
        val graph = DependencyGraph(listOf(d, c, b, a))
        assertEquals(setOf(b.id, c.id), graph.transitiveDependents(a.id).map { it.id }.toSet())
        assertTrue(graph.wouldCreateCycle(a.id, b.id))
        assertFalse(graph.wouldCreateCycle(d.id, b.id))
    }
}

private fun config(suffix: String, dependency: UUID? = null): TunnelConfig = TunnelConfig.empty(
    TunnelKind.SSH,
    "config-$suffix",
    10_000 + suffix.toInt(),
    1L,
).copy(id = id(suffix)).let { if (dependency == null) it else it.withDependency(dependency) }

private fun TunnelConfig.withDependency(id: UUID) = copy(
    spec = (spec as TunnelSpec.Ssh).copy(endpoint = EndpointSource.Dependency(id)),
)

private fun id(suffix: String): UUID = UUID.fromString("00000000-0000-0000-0000-${suffix.padStart(12, '0')}")
