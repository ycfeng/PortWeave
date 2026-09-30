package dev.portweave.tunnel.engine.orchestration

import dev.portweave.core.model.DependencyGraph
import dev.portweave.core.model.DependencyGraphException
import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelSpec
import dev.portweave.tunnel.engine.BackendHandle
import java.util.UUID
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.withContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

sealed interface StartLease {
    data object User : StartLease
    data class Dependent(val childConfigId: UUID) : StartLease
    data class Restore(val rootConfigId: UUID) : StartLease
    data class HostKeyDiscovery(val requestId: UUID) : StartLease
}

sealed interface OrchestrationResult {
    data class Started(val rootId: UUID, val order: List<UUID>) : OrchestrationResult
    data class Rejected(val rootId: UUID, val cause: Throwable) : OrchestrationResult
}

fun interface OrchestratedNodeStarter {
    suspend fun start(config: TunnelConfig, generation: Long): BackendHandle
}

/**
 * Serializes graph mutations while allowing callers to share one in-flight start.
 * Backend handles are considered ready when [OrchestratedNodeStarter.start] returns.
 */
class TunnelOrchestrator(
    private val scope: CoroutineScope,
    private val configs: suspend () -> List<TunnelConfig>,
    private val starter: OrchestratedNodeStarter,
) {
    private data class Session(val generation: Long, val handle: BackendHandle)
    private data class LeaseChange(val configId: UUID, val lease: StartLease)

    private val commandMutex = Mutex()
    private val flightMutex = Mutex()
    private data class NodeStart(val generation: Long, val result: Deferred<Result<BackendHandle>>)
    private val nodeStarts = mutableMapOf<UUID, NodeStart>()
    private val sessions = mutableMapOf<UUID, Session>()
    private val leases = mutableMapOf<UUID, MutableSet<StartLease>>()
    private val flights = mutableMapOf<Pair<UUID, StartLease>, Deferred<OrchestrationResult>>()
    private val flightWaiters = mutableMapOf<Deferred<OrchestrationResult>, Int>()

    suspend fun start(rootId: UUID, rootLease: StartLease = StartLease.User): OrchestrationResult {
        val flightKey = rootId to rootLease
        val flight = flightMutex.withLock {
            (flights[flightKey]?.takeIf { it.isActive } ?: scope.async {
                commandMutex.withLock { startTransaction(rootId, rootLease) }
            }.also { flights[flightKey] = it })
            .also { selected -> flightWaiters[selected] = (flightWaiters[selected] ?: 0) + 1 }
        }
        return try {
            flight.await()
        } finally {
            withContext(NonCancellable) {
                flightMutex.withLock {
                    val remaining = (flightWaiters[flight] ?: 1) - 1
                    if (remaining == 0) {
                        flightWaiters.remove(flight)
                        if (flights[flightKey] === flight) flights.remove(flightKey)
                        if (flight.isActive) flight.cancel()
                    } else flightWaiters[flight] = remaining
                }
            }
        }
    }

    suspend fun stop(rootId: UUID) {
        flightMutex.withLock {
            flights.filterKeys { (id, _) -> id == rootId }.values.forEach { it.cancel("Stopped by user") }
        }
        release(rootId, StartLease.User)
    }

    /** Releases one root lease and keeps every node still required by another lease running. */
    suspend fun release(rootId: UUID, rootLease: StartLease) {
        commandMutex.withLock {
            val snapshot = configs()
            val graph = DependencyGraph(snapshot)
            val chain = runCatching { graph.resolve(rootId) }.getOrElse { emptyList() }
            leases[rootId]?.remove(rootLease)
            chain.asReversed().forEachIndexed { reversedIndex, config ->
                val required = !leases[config.id].isNullOrEmpty()
                if (!required) {
                    leases.remove(config.id)
                    stopSession(config.id)
                    val chainIndex = chain.lastIndex - reversedIndex
                    if (chainIndex > 0) {
                        val parent = chain[chainIndex - 1]
                        leases[parent.id]?.remove(StartLease.Dependent(config.id))
                    }
                }
            }
            leases.entries.removeAll { it.value.isEmpty() }
        }
    }

    /** Explicit parent stop: dependents are stopped leaf-first to avoid orphaned children. */
    suspend fun stopTree(parentId: UUID) {
        val graphBeforeStop = DependencyGraph(configs())
        val affectedIds = graphBeforeStop.transitiveDependents(parentId).map { it.id }.toSet() + parentId
        cancelStarts(affectedIds)
        commandMutex.withLock {
            val snapshot = configs()
            val graph = DependencyGraph(snapshot)
            val affected = graph.transitiveDependents(parentId) + snapshot.filter { it.id == parentId }
            val depth = affected.associateWith { config ->
                runCatching { graph.resolve(config.id).size }.getOrDefault(0)
            }
            affected.sortedByDescending { depth.getValue(it) }.forEach { config ->
                leases.remove(config.id)
                stopSession(config.id)
            }
            leases.values.forEach { required ->
                required.removeAll { it is StartLease.Dependent && it.childConfigId in affectedIds }
            }
            runCatching { graph.resolve(parentId) }.getOrDefault(emptyList()).asReversed()
                .filter { it.id !in affectedIds }.forEach { stopIfUnleased(it.id) }
        }
    }

    suspend fun stopAll() {
        flightMutex.withLock { flights.values.forEach { it.cancel("All tunnels stopped") } }
        commandMutex.withLock {
            leases.clear()
            (sessions.keys + nodeStarts.keys).toList().asReversed().forEach { stopSession(it) }
        }
    }

    /** Invalidates transports without discarding the leases which still require them. */
    suspend fun invalidate(affectedIds: Set<UUID>) {
        cancelStarts(affectedIds)
        commandMutex.withLock {
            val graph = DependencyGraph(configs())
            (sessions.keys + nodeStarts.keys).filter { it in affectedIds }
                .sortedByDescending { runCatching { graph.resolve(it).size }.getOrDefault(0) }
                .forEach { stopSession(it) }
        }
    }

    private suspend fun cancelStarts(affectedIds: Set<UUID>) {
        val graph = DependencyGraph(configs())
        flightMutex.withLock {
            flights.filterKeys { (id, _) ->
                id in affectedIds || runCatching { graph.resolve(id).any { it.id in affectedIds } }.getOrDefault(false)
            }.values.forEach { it.cancel("Affected transport stopped") }
        }
    }

    suspend fun activeIds(): Set<UUID> = commandMutex.withLock { sessions.keys.toSet() }
    suspend fun generation(configId: UUID): Long? = commandMutex.withLock { sessions[configId]?.generation }
    suspend fun handle(configId: UUID, generation: Long): BackendHandle? =
        commandMutex.withLock { sessions[configId]?.takeIf { it.generation == generation }?.handle }
    suspend fun awaitConnectionLost(configId: UUID, generation: Long): Boolean {
        val session = commandMutex.withLock { sessions[configId]?.takeIf { it.generation == generation } } ?: return false
        session.handle.awaitConnectionLost()
        return commandMutex.withLock { sessions[configId]?.generation == generation }
    }
    suspend fun leases(configId: UUID): Set<StartLease> = commandMutex.withLock {
        leases[configId]?.toSet().orEmpty()
    }

    private suspend fun startTransaction(rootId: UUID, rootLease: StartLease): OrchestrationResult {
        val snapshot = configs()
        val graph = DependencyGraph(snapshot)
        val chain = try {
            graph.resolve(rootId)
        } catch (failure: DependencyGraphException) {
            return OrchestrationResult.Rejected(rootId, failure)
        }
        val duplicatePort = chain.groupBy(TunnelConfig::localPort).values.firstOrNull { it.size > 1 }
        if (duplicatePort != null) {
            return OrchestrationResult.Rejected(
                rootId,
                IllegalArgumentException("Local port conflict: ${duplicatePort.first().localPort}"),
            )
        }
        val newIds = chain.mapTo(mutableSetOf(), TunnelConfig::id)
        val activePorts = sessions.filterKeys { it !in newIds }.values.map { it.handle.localEndpoint.port }.toSet() +
            snapshot.filter { it.id in nodeStarts && it.id !in newIds }.map { it.localPort }
        chain.firstOrNull { it.id !in sessions && it.localPort in activePorts }?.let { conflict ->
            return OrchestrationResult.Rejected(
                rootId,
                IllegalArgumentException("Local port conflict: ${conflict.localPort}"),
            )
        }

        val acquired = mutableListOf<LeaseChange>()
        fun acquire(id: UUID, lease: StartLease) {
            if (leases.getOrPut(id, ::mutableSetOf).add(lease)) acquired += LeaseChange(id, lease)
        }
        suspend fun rollback() {
            acquired.filter { it.lease !is StartLease.Dependent }.forEach { leases[it.configId]?.remove(it.lease) }
            chain.asReversed().forEach { config ->
                if (leases[config.id].isNullOrEmpty()) {
                    runCatching { stopSession(config.id) }
                    leases.values.forEach { it.remove(StartLease.Dependent(config.id)) }
                }
            }
            leases.entries.removeAll { it.value.isEmpty() }
        }

        acquire(rootId, rootLease)
        chain.zipWithNext().forEach { (parent, child) -> acquire(parent.id, StartLease.Dependent(child.id)) }
        return try {
            chain.forEach { config ->
                if (config.id !in sessions) {
                    val pending = nodeStarts[config.id] ?: run {
                        val generation = generationSequence.incrementAndGet()
                        val resolved = resolveEndpoint(config, snapshot)
                        val task = scope.async {
                            var handle: BackendHandle? = null
                            try {
                                handle = starter.start(resolved, generation)
                                currentCoroutineContext().ensureActive()
                                commandMutex.withLock {
                                    check(nodeStarts[config.id]?.generation == generation && !leases[config.id].isNullOrEmpty())
                                    sessions[config.id] = Session(generation, handle)
                                    nodeStarts.remove(config.id)
                                }
                                Result.success(handle)
                            } catch (failure: Throwable) {
                                withContext(NonCancellable) {
                                    handle?.stop()
                                    commandMutex.withLock {
                                        if (nodeStarts[config.id]?.generation == generation) nodeStarts.remove(config.id)
                                    }
                                }
                                Result.failure(failure)
                            }
                        }
                        NodeStart(generation, task).also { nodeStarts[config.id] = it }
                    }
                    // Network I/O and retry waits must not monopolize graph mutation access.
                    commandMutex.unlock()
                    try {
                        pending.result.await().getOrThrow()
                    } finally {
                        withContext(NonCancellable) { commandMutex.lock() }
                    }
                    currentCoroutineContext().ensureActive()
                }
            }
            OrchestrationResult.Started(rootId, chain.map(TunnelConfig::id))
        } catch (cancelled: CancellationException) {
            withContext(NonCancellable) { rollback() }
            throw cancelled
        } catch (failure: Throwable) {
            withContext(NonCancellable) { rollback() }
            OrchestrationResult.Rejected(rootId, failure)
        }
    }

    private fun resolveEndpoint(config: TunnelConfig, all: List<TunnelConfig>): TunnelConfig {
        val dependency = when (val spec = config.spec) {
            is TunnelSpec.Ssh -> spec.endpoint as? EndpointSource.Dependency
            is TunnelSpec.Stcp -> spec.endpoint as? EndpointSource.Dependency
        } ?: return config
        val parent = all.first { it.id == dependency.configId }
        val endpoint = EndpointSource.Remote("127.0.0.1", parent.localPort)
        return config.copy(
            spec = when (val spec = config.spec) {
                is TunnelSpec.Ssh -> spec.copy(endpoint = endpoint)
                is TunnelSpec.Stcp -> spec.copy(endpoint = endpoint)
            },
        )
    }

    private suspend fun stopIfUnleased(configId: UUID) {
        if (leases[configId].isNullOrEmpty()) {
            leases.remove(configId)
            stopSession(configId)
        }
    }

    private suspend fun stopSession(configId: UUID) {
        nodeStarts.remove(configId)?.result?.cancel()
        sessions.remove(configId)?.handle?.stop()
    }

    private companion object {
        // Unique across foreground-service instances sharing the process runtime store.
        val generationSequence = java.util.concurrent.atomic.AtomicLong()
    }
}
