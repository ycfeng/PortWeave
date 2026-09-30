package dev.portweave.app.service

import dev.portweave.tunnel.engine.BackendCapabilities
import dev.portweave.tunnel.engine.BackendMetrics
import dev.portweave.tunnel.engine.TunnelState
import java.util.UUID
import org.junit.Assert.assertEquals
import org.junit.Test

class RuntimeStateStoreGenerationTest {
    @Test fun lateMetricsAndCleanupCannotOverwriteReplacementSession() {
        val store = RuntimeStateStore()
        val id = UUID.randomUUID()
        store.setGeneration(id, 2)
        store.update(id, TunnelState.Ready(100))
        store.updateMetrics(id, BackendCapabilities(), BackendMetrics(bytesUploaded = 42), 2)
        store.updateMetrics(id, BackendCapabilities(), BackendMetrics(bytesUploaded = 999), 1)
        store.updateIfGeneration(id, 1, TunnelState.Stopped)
        assertEquals(42L, store.runtime.value.getValue(id).metrics.bytesUploaded)
        assertEquals(TunnelState.Ready(100), store.states.value[id])
    }
}
