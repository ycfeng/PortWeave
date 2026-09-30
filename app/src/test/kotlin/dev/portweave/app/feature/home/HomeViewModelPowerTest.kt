package dev.portweave.app.feature.home

import androidx.lifecycle.ViewModelStore
import dev.portweave.app.service.RuntimeStateStore
import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.tunnel.engine.TunnelState
import java.util.UUID
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.setMain
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class HomeViewModelPowerTest {
    @Test fun backgroundCancelsProbeAndReturnRetriesWithoutRepeatingCompletedInstance() = runTest {
        Dispatchers.setMain(StandardTestDispatcher(testScheduler))
        val store = ViewModelStore()
        try {
            val config = TunnelConfig.empty(TunnelKind.SSH, "test", 12345)
            val repo = object : ConfigRepository {
                override val configs = MutableStateFlow(listOf(config))
                override suspend fun find(id: UUID) = configs.value.firstOrNull { it.id == id }
                override suspend fun save(config: TunnelConfig) { configs.value = listOf(config) }
                override suspend fun delete(id: UUID) { configs.value = emptyList() }
            }
            val runtime = RuntimeStateStore()
            runtime.update(config.id, TunnelState.Ready(1))
            var probes = 0
            val cancelled = CompletableDeferred<Unit>()
            val vm = HomeViewModel(repo, runtime, PortProtocolProbe {
                probes++
                if (probes == 1) try { awaitCancellation() } finally { cancelled.complete(Unit) }
                PortProtocolProbeResult.Detected(PortProtocol.HTTP)
            })
            store.put("home", vm)
            runCurrent()
            assertEquals(0, probes)
            vm.setVisible(true)
            runCurrent()
            assertEquals(1, probes)
            vm.setVisible(false)
            runCurrent()
            assertTrue(cancelled.isCompleted)
            vm.setVisible(true)
            runCurrent()
            assertEquals(2, probes)
            assertEquals(PortProtocol.HTTP, vm.protocols.value[config.id])
            vm.setVisible(false)
            runCurrent()
            vm.setVisible(true)
            runCurrent()
            assertEquals(2, probes)
            vm.setVisible(false)
            runCurrent()
            runtime.update(config.id, TunnelState.Ready(2))
            runCurrent()
            assertEquals(2, probes)
            assertTrue(vm.protocols.value.isEmpty())
            vm.setVisible(true)
            runCurrent()
            assertEquals(3, probes)
        } finally {
            store.clear()
            Dispatchers.resetMain()
        }
    }
}
