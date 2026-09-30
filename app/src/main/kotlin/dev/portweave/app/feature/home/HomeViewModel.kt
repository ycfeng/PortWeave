package dev.portweave.app.feature.home

import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import dev.portweave.app.service.RuntimeStateStore
import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.model.TunnelConfig
import dev.portweave.tunnel.engine.TunnelState
import java.util.UUID
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

class HomeViewModel(
    repository: ConfigRepository,
    private val runtimeStateStore: RuntimeStateStore,
    private val protocolProbe: PortProtocolProbe = SocketPortProtocolProbe(),
) : ViewModel() {
    val configs: StateFlow<List<TunnelConfig>> = repository.configs.stateIn(
        scope = viewModelScope,
        started = SharingStarted.WhileSubscribed(5_000),
        initialValue = emptyList(),
    )
    private val mutableProtocols = MutableStateFlow<Map<UUID, PortProtocol>>(emptyMap())
    val protocols: StateFlow<Map<UUID, PortProtocol>> = mutableProtocols.asStateFlow()

    private val visible = MutableStateFlow(false)
    fun setVisible(value: Boolean) { visible.value = value }

    private val probeJobs = mutableMapOf<UUID, Job>()
    private val probedReadyInstances = mutableMapOf<UUID, Pair<TunnelConfig, Long>>()
    private val probingInstances = mutableMapOf<UUID, Pair<TunnelConfig, Long>>()

    init {
        viewModelScope.launch {
            combine(repository.configs, runtimeStateStore.states, visible) { currentConfigs, states, isVisible ->
                currentConfigs.mapNotNull { config ->
                    val ready = states[config.id] as? TunnelState.Ready
                    ready?.let { config to it.sinceEpochMillis }
                }.toMap() to isVisible
            }.collect { (readyConfigs, isVisible) ->
                val activeIds = readyConfigs.keys.mapTo(mutableSetOf()) { it.id }
                (probeJobs.keys - activeIds).forEach { id -> probeJobs.remove(id)?.cancel() }
                probedReadyInstances.entries.removeAll { (id, instance) ->
                    readyConfigs[instance.first] != instance.second || id !in activeIds
                }
                probingInstances.entries.toList().forEach { (id, instance) ->
                    if (readyConfigs[instance.first] != instance.second) {
                        probeJobs.remove(id)?.cancel()
                        probingInstances.remove(id)
                    }
                }
                mutableProtocols.update { known -> known.filterKeys { it in probedReadyInstances } }

                if (!isVisible) {
                    probeJobs.values.forEach(Job::cancel)
                    probeJobs.clear()
                    probingInstances.clear()
                    return@collect
                }
                readyConfigs.forEach { (config, readySince) ->
                    if (probedReadyInstances[config.id] == (config to readySince)) return@forEach
                    if (probeJobs[config.id]?.isActive == true) return@forEach
                    probeJobs.remove(config.id)?.cancel()
                    mutableProtocols.update { it - config.id }
                    probingInstances[config.id] = config to readySince
                    probeJobs[config.id] = launch {
                        val detected = detectWithRetry(config, readySince)
                        kotlinx.coroutines.currentCoroutineContext().ensureActive()
                        val currentReady = runtimeStateStore.states.value[config.id] as? TunnelState.Ready
                        if (currentReady?.sinceEpochMillis != readySince) return@launch
                        probedReadyInstances[config.id] = config to readySince
                        mutableProtocols.update { known ->
                            if (detected == null) known - config.id else known + (config.id to detected)
                        }
                    }
                }
            }
        }
    }

    private suspend fun detectWithRetry(config: TunnelConfig, readySince: Long): PortProtocol? {
        PROBE_RETRY_DELAYS_MILLIS.forEach { waitMillis ->
            if (waitMillis > 0) delay(waitMillis)
            val currentReady = runtimeStateStore.states.value[config.id] as? TunnelState.Ready
            if (currentReady?.sinceEpochMillis != readySince) return null
            runtimeStateStore.workCounters.increment("protocolProbes")
            when (val result = protocolProbe.detect(config.localPort)) {
                is PortProtocolProbeResult.Detected -> return result.protocol
                PortProtocolProbeResult.NotHttp -> return null
                PortProtocolProbeResult.Unavailable -> Unit
            }
        }
        return null
    }

    companion object {
        private val PROBE_RETRY_DELAYS_MILLIS = longArrayOf(0, 1_000, 2_000, 5_000, 10_000)

        fun factory(
            repository: ConfigRepository,
            runtimeStateStore: RuntimeStateStore,
        ): ViewModelProvider.Factory =
            object : ViewModelProvider.Factory {
                @Suppress("UNCHECKED_CAST")
                override fun <T : ViewModel> create(modelClass: Class<T>): T =
                    HomeViewModel(repository, runtimeStateStore) as T
            }
    }
}
