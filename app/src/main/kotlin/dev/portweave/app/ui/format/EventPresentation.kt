package dev.portweave.app.ui.format

import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.graphics.Color
import dev.portweave.app.ui.theme.PortWeaveThemeTokens
import dev.portweave.core.diagnostics.EventLevel
import dev.portweave.core.diagnostics.TunnelEventCode

fun eventTitle(code: String): String = when (code) {
    TunnelEventCode.START_REQUESTED -> "请求启动"
    TunnelEventCode.DEPENDENCY_RESOLVED -> "依赖已就绪"
    TunnelEventCode.LISTENER_BOUND -> "本地端口已监听"
    TunnelEventCode.TUNNEL_READY -> "映射已就绪"
    TunnelEventCode.NETWORK_CHANGED -> "网络已变化"
    TunnelEventCode.RETRY_SCHEDULED -> "等待重连"
    TunnelEventCode.STOP_REQUESTED -> "请求停止"
    TunnelEventCode.STOPPED -> "映射已停止"
    TunnelEventCode.FAILED -> "运行失败"
    TunnelEventCode.PROCESS_RESTORED -> "已恢复运行状态"
    else -> "运行事件"
}

fun eventDescription(code: String, messageKey: String = ""): String? = when (code) {
    TunnelEventCode.START_REQUESTED -> "正在准备连接"
    TunnelEventCode.DEPENDENCY_RESOLVED -> "上游映射可以使用"
    TunnelEventCode.LISTENER_BOUND -> "本地端口已成功绑定"
    TunnelEventCode.TUNNEL_READY -> "本地映射现在可以使用"
    TunnelEventCode.NETWORK_CHANGED -> "将根据连接状态自动恢复"
    TunnelEventCode.RETRY_SCHEDULED -> "系统将在稍后再次连接"
    TunnelEventCode.STOP_REQUESTED -> "正在安全关闭连接"
    TunnelEventCode.STOPPED -> "连接和本地端口已释放"
    TunnelEventCode.FAILED -> sshFailureDescription(messageKey) ?: "请查看技术信息或导出日志排查"
    TunnelEventCode.PROCESS_RESTORED -> "应用已重新载入运行信息"
    else -> null
}

fun eventLevelLabel(level: EventLevel): String = when (level) {
    EventLevel.DEBUG -> "调试"
    EventLevel.INFO -> "信息"
    EventLevel.WARNING -> "警告"
    EventLevel.ERROR -> "错误"
}

@Composable
fun eventLevelColor(level: EventLevel): Color = when (level) {
    EventLevel.DEBUG -> PortWeaveThemeTokens.status.mutedText
    EventLevel.INFO -> PortWeaveThemeTokens.status.success
    EventLevel.WARNING -> PortWeaveThemeTokens.status.warning
    EventLevel.ERROR -> MaterialTheme.colorScheme.error
}
