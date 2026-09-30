package dev.portweave.core.data.db

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.Embedded
import androidx.room.ForeignKey
import androidx.room.Index
import androidx.room.PrimaryKey
import androidx.room.Relation
import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.FrpsServerIdentity
import dev.portweave.core.model.SshAuthMethod
import dev.portweave.core.model.SshCredentialRefs
import dev.portweave.core.model.SshHostKeyPolicy
import dev.portweave.core.model.SshServerIdentity
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.core.model.StcpWireProtocol
import java.util.UUID
import dev.portweave.core.security.CredentialRef

@Entity(tableName = "tunnel_config")
data class TunnelConfigEntity(
    @PrimaryKey val id: String,
    val name: String,
    val description: String,
    @ColumnInfo(name = "local_port") val localPort: Int,
    val type: String,
    @ColumnInfo(name = "auto_recover") val autoRecover: Boolean,
    val favorite: Boolean,
    @ColumnInfo(name = "sort_order") val sortOrder: Long,
    val revision: Long,
    @ColumnInfo(name = "created_at") val createdAt: Long,
    @ColumnInfo(name = "updated_at") val updatedAt: Long,
)

@Entity(
    tableName = "ssh_detail",
    foreignKeys = [
        ForeignKey(
            entity = TunnelConfigEntity::class,
            parentColumns = ["id"],
            childColumns = ["config_id"],
            onDelete = ForeignKey.CASCADE,
        ),
    ],
    indices = [Index("config_id")],
)
data class SshDetailEntity(
    @PrimaryKey @ColumnInfo(name = "config_id") val configId: String,
    @ColumnInfo(name = "dial_host") val dialHost: String,
    @ColumnInfo(name = "dial_port") val dialPort: Int,
    @ColumnInfo(name = "dependency_config_id") val dependencyConfigId: String?,
    @ColumnInfo(name = "logical_host") val logicalHost: String,
    @ColumnInfo(name = "logical_port") val logicalPort: Int,
    val username: String,
    @ColumnInfo(name = "auth_method") val authMethod: String,
    @ColumnInfo(name = "password_ref") val passwordRef: String?,
    @ColumnInfo(name = "private_key_ref") val privateKeyRef: String?,
    @ColumnInfo(name = "passphrase_ref") val passphraseRef: String?,
    @ColumnInfo(name = "host_fingerprint_ref") val hostFingerprintRef: String?,
    @ColumnInfo(name = "remote_target_host") val remoteTargetHost: String,
    @ColumnInfo(name = "remote_target_port") val remoteTargetPort: Int,
    @ColumnInfo(name = "host_key_policy") val hostKeyPolicy: String,
    @ColumnInfo(name = "connect_timeout_seconds") val connectTimeoutSeconds: Int,
    @ColumnInfo(name = "keep_alive_seconds") val keepAliveSeconds: Int,
) {
    override fun toString(): String =
        "SshDetailEntity(configId=<redacted>, endpoint=<redacted>, identity=<redacted>, " +
            "username=<redacted>, authMethod=$authMethod, credentials=<redacted>, " +
            "remoteTarget=<redacted>, hostKeyPolicy=$hostKeyPolicy, " +
            "connectTimeoutSeconds=$connectTimeoutSeconds, keepAliveSeconds=$keepAliveSeconds)"
}

@Entity(
    tableName = "stcp_detail",
    foreignKeys = [
        ForeignKey(
            entity = TunnelConfigEntity::class,
            parentColumns = ["id"],
            childColumns = ["config_id"],
            onDelete = ForeignKey.CASCADE,
        ),
    ],
    indices = [Index("config_id")],
)
data class StcpDetailEntity(
    @PrimaryKey @ColumnInfo(name = "config_id") val configId: String,
    @ColumnInfo(name = "dial_host") val dialHost: String,
    @ColumnInfo(name = "dial_port") val dialPort: Int,
    @ColumnInfo(name = "dependency_config_id") val dependencyConfigId: String?,
    @ColumnInfo(name = "logical_host") val logicalHost: String,
    @ColumnInfo(name = "logical_port") val logicalPort: Int,
    @ColumnInfo(name = "token_ref") val tokenRef: String?,
    val user: String?,
    @ColumnInfo(name = "server_user") val serverUser: String?,
    @ColumnInfo(name = "server_name") val serverName: String,
    @ColumnInfo(name = "secret_key_ref") val secretKeyRef: String?,
    @ColumnInfo(name = "wire_protocol") val wireProtocol: String,
    @ColumnInfo(name = "use_encryption") val useEncryption: Boolean,
    @ColumnInfo(name = "use_compression") val useCompression: Boolean,
    @ColumnInfo(name = "tls_leaf_pin_sha256") val tlsLeafPinSha256: String?,
    @ColumnInfo(name = "connect_timeout_seconds") val connectTimeoutSeconds: Int,
) {
    override fun toString(): String =
        "StcpDetailEntity(configId=<redacted>, endpoint=<redacted>, identity=<redacted>, " +
            "credentials=<redacted>, user=<redacted>, serverUser=<redacted>, serverName=<redacted>, " +
            "wireProtocol=$wireProtocol, useEncryption=$useEncryption, useCompression=$useCompression, " +
            "tlsLeafPinSha256=<redacted>, connectTimeoutSeconds=$connectTimeoutSeconds)"
}

data class TunnelConfigRecord(
    @Embedded val config: TunnelConfigEntity,
    @Relation(parentColumn = "id", entityColumn = "config_id")
    val ssh: SshDetailEntity?,
    @Relation(parentColumn = "id", entityColumn = "config_id")
    val stcp: StcpDetailEntity?,
)

internal fun TunnelConfigRecord.toDomain(): TunnelConfig = TunnelConfig(
    id = UUID.fromString(config.id),
    name = config.name,
    description = config.description,
    localPort = config.localPort,
    spec = when (TunnelKind.valueOf(config.type)) {
        TunnelKind.SSH -> ssh?.toDomain() ?: TunnelSpec.Ssh()
        TunnelKind.STCP -> stcp?.toDomain() ?: TunnelSpec.Stcp()
    },
    autoRecover = config.autoRecover,
    favorite = config.favorite,
    sortOrder = config.sortOrder,
    revision = config.revision,
    createdAtEpochMillis = config.createdAt,
    updatedAtEpochMillis = config.updatedAt,
)

private fun SshDetailEntity.toDomain(): TunnelSpec.Ssh = TunnelSpec.Ssh(
    endpoint = dependencyConfigId?.let { EndpointSource.Dependency(UUID.fromString(it)) }
        ?: EndpointSource.Remote(dialHost, dialPort),
    identity = SshServerIdentity(logicalHost, logicalPort),
    username = username,
    authMethod = SshAuthMethod.valueOf(authMethod),
    credentials = SshCredentialRefs(
        password = passwordRef?.let(::credentialRef),
        privateKey = privateKeyRef?.let(::credentialRef),
        passphrase = passphraseRef?.let(::credentialRef),
        hostFingerprint = hostFingerprintRef?.let(::credentialRef),
    ),
    remoteTargetHost = remoteTargetHost,
    remoteTargetPort = remoteTargetPort,
    hostKeyPolicy = SshHostKeyPolicy.valueOf(hostKeyPolicy),
    connectTimeoutSeconds = connectTimeoutSeconds,
    keepAliveSeconds = keepAliveSeconds,
)

private fun credentialRef(value: String) = CredentialRef(UUID.fromString(value))

private fun StcpDetailEntity.toDomain(): TunnelSpec.Stcp = TunnelSpec.Stcp(
    endpoint = dependencyConfigId?.let { EndpointSource.Dependency(UUID.fromString(it)) }
        ?: EndpointSource.Remote(dialHost, dialPort),
    identity = FrpsServerIdentity(logicalHost, logicalPort),
    tokenRef = tokenRef?.let(::credentialRef),
    user = user,
    serverUser = serverUser,
    serverName = serverName,
    secretKeyRef = secretKeyRef?.let(::credentialRef),
    wireProtocol = StcpWireProtocol.valueOf(wireProtocol),
    useEncryption = useEncryption,
    useCompression = useCompression,
    tlsLeafPinSha256 = tlsLeafPinSha256,
    connectTimeoutSeconds = connectTimeoutSeconds,
)

internal fun TunnelConfig.toEntity(): TunnelConfigEntity = TunnelConfigEntity(
    id = id.toString(),
    name = name,
    description = description,
    localPort = localPort,
    type = kind.name,
    autoRecover = autoRecover,
    favorite = favorite,
    sortOrder = sortOrder,
    revision = revision,
    createdAt = createdAtEpochMillis,
    updatedAt = updatedAtEpochMillis,
)

internal fun TunnelConfig.toSshDetailEntity(): SshDetailEntity? {
    val ssh = spec as? TunnelSpec.Ssh ?: return null
    val remote = ssh.endpoint as? EndpointSource.Remote
    val dependency = ssh.endpoint as? EndpointSource.Dependency
    return SshDetailEntity(
        configId = id.toString(),
        dialHost = remote?.host.orEmpty(),
        dialPort = remote?.port ?: 0,
        dependencyConfigId = dependency?.configId?.toString(),
        logicalHost = ssh.identity.logicalHost,
        logicalPort = ssh.identity.logicalPort,
        username = ssh.username,
        authMethod = ssh.authMethod.name,
        passwordRef = ssh.credentials.password?.value?.toString(),
        privateKeyRef = ssh.credentials.privateKey?.value?.toString(),
        passphraseRef = ssh.credentials.passphrase?.value?.toString(),
        hostFingerprintRef = ssh.credentials.hostFingerprint?.value?.toString(),
        remoteTargetHost = ssh.remoteTargetHost,
        remoteTargetPort = ssh.remoteTargetPort,
        hostKeyPolicy = ssh.hostKeyPolicy.name,
        connectTimeoutSeconds = ssh.connectTimeoutSeconds,
        keepAliveSeconds = ssh.keepAliveSeconds,
    )
}

internal fun TunnelConfig.toStcpDetailEntity(): StcpDetailEntity? {
    val stcp = spec as? TunnelSpec.Stcp ?: return null
    val remote = stcp.endpoint as? EndpointSource.Remote
    val dependency = stcp.endpoint as? EndpointSource.Dependency
    return StcpDetailEntity(
        configId = id.toString(),
        dialHost = remote?.host.orEmpty(),
        dialPort = remote?.port ?: 0,
        dependencyConfigId = dependency?.configId?.toString(),
        logicalHost = stcp.identity.logicalHost,
        logicalPort = stcp.identity.logicalPort,
        tokenRef = stcp.tokenRef?.value?.toString(),
        user = stcp.user,
        serverUser = stcp.serverUser,
        serverName = stcp.serverName,
        secretKeyRef = stcp.secretKeyRef?.value?.toString(),
        wireProtocol = stcp.wireProtocol.name,
        useEncryption = stcp.useEncryption,
        useCompression = stcp.useCompression,
        tlsLeafPinSha256 = stcp.tlsLeafPinSha256,
        connectTimeoutSeconds = stcp.connectTimeoutSeconds,
    )
}
