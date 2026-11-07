package dev.portweave.app.ui.format

import dev.portweave.tunnel.engine.TunnelError
import dev.portweave.tunnel.engine.TunnelErrorCode

data class TunnelErrorPresentation(val title: String, val suggestion: String)

private val sshErrors = mapOf(
    "ssh_private_key_invalid" to TunnelErrorPresentation(
        "私钥格式无法读取",
        "请重新导入完整、受支持的私钥文件，确认没有选择 .pub 公钥文件。",
    ),
    "ssh_passphrase_required" to TunnelErrorPresentation(
        "缺少私钥密码短语",
        "这把私钥已加密，请在编辑配置中填写私钥密码短语。",
    ),
    "ssh_passphrase_invalid" to TunnelErrorPresentation(
        "私钥解密失败",
        "请检查私钥密码短语是否正确；若仍失败，请确认私钥文件完整且加密格式受支持。",
    ),
    "ssh_algorithm_unsupported" to TunnelErrorPresentation(
        "SSH 算法不兼容",
        "客户端与服务器无法协商共同的 SSH 算法，请检查服务器的密钥交换、主机密钥和加密算法配置。",
    ),
    "ssh_public_key_rejected" to TunnelErrorPresentation(
        "服务器未接受密钥认证",
        "私钥已读取并完成所需解密，但密钥认证未通过。请检查用户名、authorized_keys、登录策略及签名算法；具体原因需查看服务器 SSH 日志。",
    ),
    "ssh_password_rejected" to TunnelErrorPresentation(
        "服务器未接受密码认证",
        "请检查用户名、密码，以及服务器是否允许该用户使用密码登录；具体原因需查看服务器 SSH 日志。",
    ),
    "ssh_auth_failed" to TunnelErrorPresentation(
        "SSH 认证失败",
        "请检查登录凭据和服务器登录策略；具体原因需查看服务器 SSH 日志。",
    ),
)

fun tunnelErrorPresentation(error: TunnelError): TunnelErrorPresentation = sshErrors[error.messageKey] ?: when (error.code) {
    TunnelErrorCode.SSH_PRIVATE_KEY_INVALID -> sshErrors.getValue("ssh_private_key_invalid")
    TunnelErrorCode.SSH_PASSPHRASE_REQUIRED -> sshErrors.getValue("ssh_passphrase_required")
    TunnelErrorCode.SSH_PASSPHRASE_INVALID -> sshErrors.getValue("ssh_passphrase_invalid")
    TunnelErrorCode.SSH_ALGORITHM_UNSUPPORTED -> sshErrors.getValue("ssh_algorithm_unsupported")
    TunnelErrorCode.SSH_AUTH_FAILED -> sshErrors.getValue("ssh_auth_failed")
    TunnelErrorCode.SSH_HOST_KEY_MISMATCH -> TunnelErrorPresentation(
        "SSH 主机指纹不匹配", "服务器主机指纹与已保存的记录不同，请核实服务器身份后重新确认指纹。",
    )
    TunnelErrorCode.CONNECT_TIMEOUT -> TunnelErrorPresentation("连接超时", "请检查服务器地址、SSH 端口、网络及防火墙设置。")
    TunnelErrorCode.CONNECTION_REFUSED -> TunnelErrorPresentation("连接被拒绝", "请确认服务器的 SSH 服务正在指定端口监听。")
    TunnelErrorCode.DNS_FAILED -> TunnelErrorPresentation("域名解析失败", "请检查服务器域名和网络的 DNS 设置。")
    TunnelErrorCode.PORT_IN_USE -> TunnelErrorPresentation("本地端口被占用", "请停止占用端口的映射或更换本地端口。")
    TunnelErrorCode.SSH_FORWARD_FAILED -> TunnelErrorPresentation("SSH 连接或转发失败", "请检查服务器状态，并查看日志中的错误标识进一步排查。")
    else -> TunnelErrorPresentation(error.code.name, "请检查配置，并查看日志进一步排查。")
}

fun sshFailureDescription(messageKey: String): String? = sshErrors[messageKey]?.let {
    "${it.title}。${it.suggestion}"
}
