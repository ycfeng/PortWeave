package dev.portweave.core.model

import java.util.UUID

val TunnelConfig.dependencyConfigId: UUID?
    get() = when (val value = spec) {
        is TunnelSpec.Ssh -> (value.endpoint as? EndpointSource.Dependency)?.configId
        is TunnelSpec.Stcp -> (value.endpoint as? EndpointSource.Dependency)?.configId
    }

sealed class DependencyGraphException(message: String) : IllegalArgumentException(message) {
    data class Missing(val configId: UUID, val dependencyId: UUID) :
        DependencyGraphException("Dangling dependency: $configId -> $dependencyId")

    data class Cycle(val path: List<UUID>) :
        DependencyGraphException("Dependency cycle: ${path.joinToString(" -> ")}")
}

/** Immutable, deterministic view of the configuration dependency graph. */
class DependencyGraph(configs: Collection<TunnelConfig>) {
    private val ordered = configs.sortedWith(compareBy<TunnelConfig>({ it.sortOrder }, { it.id.toString() }))
    private val byId = ordered.associateBy(TunnelConfig::id)

    init {
        require(byId.size == configs.size) { "Duplicate configuration id" }
    }

    fun validate() {
        firstMissing()?.let { throw it }
        findCycle()?.let { throw DependencyGraphException.Cycle(it) }
    }

    fun firstMissing(): DependencyGraphException.Missing? = ordered.firstNotNullOfOrNull { config ->
        config.dependencyConfigId?.takeIf { it !in byId }?.let {
            DependencyGraphException.Missing(config.id, it)
        }
    }

    /** Returns a closed cycle path, e.g. A, B, C, A. */
    fun findCycle(): List<UUID>? {
        val visited = mutableSetOf<UUID>()
        for (start in ordered.map(TunnelConfig::id)) {
            if (start in visited) continue
            val path = mutableListOf<UUID>()
            val index = mutableMapOf<UUID, Int>()
            var current: UUID? = start
            while (current != null && current in byId && current !in visited) {
                index[current]?.let { cycleStart ->
                    return path.subList(cycleStart, path.size).toList() + current
                }
                index[current] = path.size
                path += current
                current = byId.getValue(current).dependencyConfigId
            }
            visited += path
        }
        return null
    }

    /** Parent-first dependency chain ending in [rootId]. */
    fun resolve(rootId: UUID): List<TunnelConfig> {
        firstMissing()?.let { throw it }
        findCycle()?.let { throw DependencyGraphException.Cycle(it) }
        val root = byId[rootId] ?: throw DependencyGraphException.Missing(rootId, rootId)
        val reversed = mutableListOf<TunnelConfig>()
        var current: TunnelConfig? = root
        while (current != null) {
            reversed += current
            current = current.dependencyConfigId?.let(byId::getValue)
        }
        return reversed.asReversed()
    }

    fun transitiveDependents(parentId: UUID): List<TunnelConfig> = ordered.filter { candidate ->
        var current = candidate.dependencyConfigId
        val seen = mutableSetOf<UUID>()
        while (current != null && seen.add(current)) {
            if (current == parentId) return@filter true
            current = byId[current]?.dependencyConfigId
        }
        false
    }

    fun wouldCreateCycle(configId: UUID, dependencyId: UUID): Boolean {
        if (configId == dependencyId) return true
        var current: UUID? = dependencyId
        val seen = mutableSetOf<UUID>()
        while (current != null && seen.add(current)) {
            if (current == configId) return true
            current = byId[current]?.dependencyConfigId
        }
        return false
    }
}
