package dev.portweave.core.model

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class TunnelConfigTest {
    @Test
    fun emptyCreatesRequestedBackendKind() {
        val ssh = TunnelConfig.empty(TunnelKind.SSH, " SSH 入口 ", 10022, 10L)
        val stcp = TunnelConfig.empty(TunnelKind.STCP, "STCP 入口", 17000, 20L)

        assertEquals(TunnelKind.SSH, ssh.kind)
        assertEquals("SSH 入口", ssh.name)
        assertEquals(TunnelKind.STCP, stcp.kind)
        assertNull(stcp.validationError())
    }

    @Test
    fun invalidPortIsRejected() {
        val config = TunnelConfig.empty(TunnelKind.SSH, "test", 0)
        assertEquals("本地端口必须在 1024 到 65535 之间", config.validationError())
    }
}
