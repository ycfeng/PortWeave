package dev.portweave.core.model

import java.util.UUID

enum class TunnelKind {
    SSH,
    STCP,
}

sealed interface TunnelSpec {
    data class Ssh(
        val endpoint: EndpointSource = EndpointSource.Remote("", 22),
        val identity: SshServerIdentity = SshServerIdentity("", 22),
        val username: String = "",
        val authMethod: SshAuthMethod = SshAuthMethod.PASSWORD,
        val credentials: SshCredentialRefs = SshCredentialRefs(),
        val remoteTargetHost: String = "",
        val remoteTargetPort: Int = 0,
        val hostKeyPolicy: SshHostKeyPolicy = SshHostKeyPolicy.TOFU,
        val connectTimeoutSeconds: Int = 10,
        val keepAliveSeconds: Int = 30,
    ) : TunnelSpec {
        override fun toString(): String =
            "Ssh(endpoint=<redacted>, identity=<redacted>, username=<redacted>, " +
                "authMethod=$authMethod, credentials=<redacted>, remoteTarget=<redacted>, " +
                "hostKeyPolicy=$hostKeyPolicy, connectTimeoutSeconds=$connectTimeoutSeconds, " +
                "keepAliveSeconds=$keepAliveSeconds)"
    }

    data class Stcp(
        val endpoint: EndpointSource = EndpointSource.Remote("", 7000),
        val identity: FrpsServerIdentity = FrpsServerIdentity("", 7000),
        val tokenRef: dev.portweave.core.security.CredentialRef? = null,
        val user: String? = null,
        val serverUser: String? = null,
        val serverName: String = "",
        val secretKeyRef: dev.portweave.core.security.CredentialRef? = null,
        val wireProtocol: StcpWireProtocol = StcpWireProtocol.V1,
        val useEncryption: Boolean = false,
        val useCompression: Boolean = false,
        val tlsLeafPinSha256: String? = null,
        val connectTimeoutSeconds: Int = 10,
    ) : TunnelSpec {
        override fun toString(): String =
            "Stcp(endpoint=<redacted>, identity=<redacted>, tokenRef=<redacted>, user=<redacted>, " +
                "serverUser=<redacted>, serverName=<redacted>, secretKeyRef=<redacted>, " +
                "wireProtocol=$wireProtocol, useEncryption=$useEncryption, " +
                "useCompression=$useCompression, tlsLeafPinSha256=<redacted>, " +
                "connectTimeoutSeconds=$connectTimeoutSeconds)"
    }
}

data class FrpsServerIdentity(val logicalHost: String, val logicalPort: Int) {
    override fun toString(): String = "FrpsServerIdentity(logicalHost=<redacted>, logicalPort=$logicalPort)"
}

enum class StcpWireProtocol(val wireValue: String) {
    V1("v1"),
    V2("v2"),
}

sealed interface EndpointSource {
    data class Remote(val host: String, val port: Int) : EndpointSource {
        override fun toString(): String = "Remote(host=<redacted>, port=$port)"
    }

    data class Dependency(val configId: UUID) : EndpointSource
}

data class SshServerIdentity(
    val logicalHost: String,
    val logicalPort: Int,
) {
    override fun toString(): String = "SshServerIdentity(logicalHost=<redacted>, logicalPort=$logicalPort)"
}

enum class SshAuthMethod {
    PASSWORD,
    PRIVATE_KEY,
}

enum class SshHostKeyPolicy {
    TOFU,
    FIXED,
}

data class SshCredentialRefs(
    val password: dev.portweave.core.security.CredentialRef? = null,
    val privateKey: dev.portweave.core.security.CredentialRef? = null,
    val passphrase: dev.portweave.core.security.CredentialRef? = null,
    val hostFingerprint: dev.portweave.core.security.CredentialRef? = null,
) {
    override fun toString(): String = "SshCredentialRefs(<redacted>)"
}

data class TunnelConfig(
    val id: UUID,
    val name: String,
    val description: String = "",
    val localPort: Int,
    val spec: TunnelSpec,
    val autoRecover: Boolean = true,
    val favorite: Boolean = false,
    val sortOrder: Long = 0,
    val revision: Long = 1,
    val createdAtEpochMillis: Long,
    val updatedAtEpochMillis: Long,
) {
    val kind: TunnelKind
        get() = when (spec) {
            is TunnelSpec.Ssh -> TunnelKind.SSH
            is TunnelSpec.Stcp -> TunnelKind.STCP
        }

    val requiresCredentials: Boolean
        get() = when (val value = spec) {
            is TunnelSpec.Ssh -> value.credentials.hostFingerprint == null || when (value.authMethod) {
                SshAuthMethod.PASSWORD -> value.credentials.password == null
                SshAuthMethod.PRIVATE_KEY -> value.credentials.privateKey == null
            }
            is TunnelSpec.Stcp -> value.secretKeyRef == null
        }

    companion object {
        fun empty(
            kind: TunnelKind,
            name: String,
            localPort: Int,
            nowEpochMillis: Long = System.currentTimeMillis(),
        ): TunnelConfig = TunnelConfig(
            id = UUID.randomUUID(),
            name = name.trim(),
            localPort = localPort,
            spec = when (kind) {
                TunnelKind.SSH -> TunnelSpec.Ssh()
                TunnelKind.STCP -> TunnelSpec.Stcp()
            },
            createdAtEpochMillis = nowEpochMillis,
            updatedAtEpochMillis = nowEpochMillis,
        )
    }
}

fun TunnelConfig.validationError(): String? = when {
    name.isBlank() -> "配置名称不能为空"
    localPort !in 1024..65535 -> "本地端口必须在 1024 到 65535 之间"
    else -> null
}
