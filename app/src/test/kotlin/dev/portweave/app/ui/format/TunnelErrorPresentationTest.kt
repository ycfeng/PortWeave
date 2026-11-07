package dev.portweave.app.ui.format

import dev.portweave.core.diagnostics.TunnelEventCode
import dev.portweave.tunnel.engine.TunnelError
import dev.portweave.tunnel.engine.TunnelErrorCode
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class TunnelErrorPresentationTest {
    @Test
    fun keyFailuresHaveDistinctChineseReasonsAndActionableSuggestions() {
        val cases = mapOf(
            TunnelErrorCode.SSH_PRIVATE_KEY_INVALID to "私钥格式无法读取",
            TunnelErrorCode.SSH_PASSPHRASE_REQUIRED to "缺少私钥密码短语",
            TunnelErrorCode.SSH_PASSPHRASE_INVALID to "私钥解密失败",
            TunnelErrorCode.SSH_ALGORITHM_UNSUPPORTED to "SSH 算法不兼容",
        )
        cases.forEach { (code, title) ->
            val presentation = tunnelErrorPresentation(TunnelError(code, false, "unknown"))
            assertEquals(title, presentation.title)
            assertTrue(presentation.suggestion.isNotBlank())
        }
    }

    @Test
    fun authenticationMethodAndLegacyAuthenticationErrorsRemainDistinct() {
        val key = tunnelErrorPresentation(TunnelError(TunnelErrorCode.SSH_AUTH_FAILED, false, "ssh_public_key_rejected"))
        val password = tunnelErrorPresentation(TunnelError(TunnelErrorCode.SSH_AUTH_FAILED, false, "ssh_password_rejected"))
        val legacy = tunnelErrorPresentation(TunnelError(TunnelErrorCode.SSH_AUTH_FAILED, false, "ssh_auth_failed"))
        assertEquals("服务器未接受密钥认证", key.title)
        assertTrue(key.suggestion.contains("服务器 SSH 日志"))
        assertEquals("服务器未接受密码认证", password.title)
        assertFalse(password.suggestion.contains("私钥"))
        assertEquals("SSH 认证失败", legacy.title)
    }

    @Test
    fun failureEventsShowSpecificCauseWithoutExposingUnknownRawText() {
        val description = eventDescription(TunnelEventCode.FAILED, "ssh_passphrase_required").orEmpty()
        assertTrue(description.contains("缺少私钥密码短语"))
        assertTrue(description.contains("编辑配置"))
        val raw = "secret raw exception"
        assertFalse(eventDescription(TunnelEventCode.FAILED, raw).orEmpty().contains(raw))
        assertEquals("正在准备连接", eventDescription(TunnelEventCode.START_REQUESTED, "ssh_passphrase_required"))
    }
}
