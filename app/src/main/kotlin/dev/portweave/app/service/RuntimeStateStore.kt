package dev.portweave.app.service

import dev.portweave.tunnel.engine.TunnelState
import dev.portweave.tunnel.engine.BackendCapabilities
import dev.portweave.tunnel.engine.BackendMetrics
import java.util.UUID
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

class RuntimeStateStore {
    val workCounters = dev.portweave.tunnel.engine.RuntimeWorkCounters()
    private val mutableStates = MutableStateFlow<Map<UUID, TunnelState>>(emptyMap())
    val states: StateFlow<Map<UUID, TunnelState>> = mutableStates.asStateFlow()
    private val mutableRuntime = MutableStateFlow<Map<UUID, RuntimeSnapshot>>(emptyMap())
    val runtime: StateFlow<Map<UUID, RuntimeSnapshot>> = mutableRuntime.asStateFlow()

    @Synchronized fun update(configId: UUID, state: TunnelState) {
        mutableStates.value = mutableStates.value + (configId to state)
        val current = mutableRuntime.value[configId] ?: RuntimeSnapshot()
        mutableRuntime.value = mutableRuntime.value + (configId to current.withState(state))
    }

    @Synchronized fun setGeneration(configId: UUID, generation: Long) {
        val current = mutableRuntime.value[configId] ?: RuntimeSnapshot()
        mutableRuntime.value = mutableRuntime.value + (configId to current.copy(generation = generation))
    }

    @Synchronized fun updateMetrics(configId: UUID, capabilities: BackendCapabilities, metrics: BackendMetrics, generation: Long? = null) {
        val current = mutableRuntime.value[configId] ?: RuntimeSnapshot()
        if (generation != null && current.generation != generation) return
        mutableRuntime.value = mutableRuntime.value + (configId to current.copy(capabilities = capabilities, metrics = metrics))
    }

    @Synchronized fun updateIfGeneration(configId: UUID, generation: Long, state: TunnelState) {
        if (mutableRuntime.value[configId]?.generation == generation) update(configId, state)
    }

    @Synchronized fun networkChanged(configIds: Collection<UUID>) {
        mutableRuntime.value = mutableRuntime.value.toMutableMap().apply {
            configIds.forEach { id -> this[id] = (this[id] ?: RuntimeSnapshot()).let { it.copy(networkChanges = it.networkChanges + 1) } }
        }
    }

    @Synchronized fun remove(configId: UUID) {
        mutableStates.value = mutableStates.value - configId
        mutableRuntime.value = mutableRuntime.value - configId
    }
}

data class RuntimeSnapshot(
    val state: TunnelState = TunnelState.Stopped,
    val generation: Long = 0,
    val startedAtEpochMillis: Long? = null,
    val lastReadyAtEpochMillis: Long? = null,
    val reconnectCount: Int = 0,
    val networkChanges: Int = 0,
    val lastFailureCode: String? = null,
    val capabilities: BackendCapabilities = BackendCapabilities(),
    val metrics: BackendMetrics = BackendMetrics(),
) {
    private fun now() = System.currentTimeMillis()

    fun withState(next: TunnelState): RuntimeSnapshot = copy(
        state = next,
        startedAtEpochMillis = when {
            next is TunnelState.Starting && startedAtEpochMillis == null -> now()
            next is TunnelState.Stopped -> null
            else -> startedAtEpochMillis
        },
        lastReadyAtEpochMillis = (next as? TunnelState.Ready)?.sinceEpochMillis ?: lastReadyAtEpochMillis,
        reconnectCount = maxOf(reconnectCount, (next as? TunnelState.Reconnecting)?.attempt ?: 0),
        lastFailureCode = (next as? TunnelState.Failed)?.error?.code?.name ?: lastFailureCode,
    )
}
