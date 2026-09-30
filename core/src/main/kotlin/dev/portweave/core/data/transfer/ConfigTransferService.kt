package dev.portweave.core.data.transfer

import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.model.DependencyGraph
import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.FrpsServerIdentity
import dev.portweave.core.model.SshAuthMethod
import dev.portweave.core.model.SshCredentialRefs
import dev.portweave.core.model.SshHostKeyPolicy
import dev.portweave.core.model.SshServerIdentity
import dev.portweave.core.model.StcpWireProtocol
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.core.security.CredentialKind
import dev.portweave.core.security.CredentialRef
import dev.portweave.core.security.CredentialStore
import java.util.Base64
import java.util.UUID
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put

enum class ImportConflictStrategy { CREATE_COPY, REPLACE_LOCAL, SKIP }

data class ImportPreview(
    val configs: List<TunnelConfig>,
    val replaceIds: Set<UUID>,
    val skipped: Int,
    val credentialsRequired: Set<UUID>,
    internal val sourceIdsByImportedId: Map<UUID, UUID> = emptyMap(),
)

data class ImportResult(val imported: Int, val skipped: Int, val credentialsRequired: Set<UUID>)

class ConfigTransferService(
    private val repository: ConfigRepository,
    private val credentialStore: CredentialStore? = null,
    private val nowEpochMillis: () -> Long = System::currentTimeMillis,
) {
    fun export(
        configs: Collection<TunnelConfig>,
        privacyMode: Boolean = false,
        prettyPrint: Boolean = true,
    ): String {
        return exportJson(configs, privacyMode, prettyPrint, emptyMap(), containsSecrets = false)
    }

    suspend fun exportIncludingSecrets(
        configs: Collection<TunnelConfig>,
        prettyPrint: Boolean = true,
    ): String {
        val store = requireNotNull(credentialStore) { "凭据存储不可用" }
        val secrets = configs.associate { it.id to it.credentialsToJson(store) }
        return exportJson(
            configs = configs,
            privacyMode = false,
            prettyPrint = prettyPrint,
            secrets = secrets,
            containsSecrets = true,
        )
    }

    private fun exportJson(
        configs: Collection<TunnelConfig>,
        privacyMode: Boolean,
        prettyPrint: Boolean,
        secrets: Map<UUID, JsonObject>,
        containsSecrets: Boolean,
    ): String {
        val ordered = configs.sortedWith(compareBy(TunnelConfig::sortOrder, TunnelConfig::name))
        return Json { this.prettyPrint = prettyPrint }.encodeToString(
            JsonObject.serializer(),
            buildJsonObject {
                put("schemaVersion", 1)
                put("exportedAtEpochMillis", nowEpochMillis())
                put("containsSecrets", containsSecrets)
                put("configs", buildJsonArray { ordered.forEach { add(it.toJson(privacyMode, secrets[it.id])) } })
                put("dependencies", buildJsonArray {
                    ordered.forEach { config ->
                        config.dependencyId()?.let { parentId ->
                            add(buildJsonObject {
                                put("configId", config.id.toString())
                                put("dependsOn", parentId.toString())
                            })
                        }
                    }
                })
            },
        )
    }

    suspend fun preview(json: String, strategy: ImportConflictStrategy): ImportPreview {
        val decodedJson = QrConfigPayloadCodec.decodeIfEncoded(json)
        require(decodedJson.length <= MaxImportChars) { "导入文件过大" }
        val root = Json.parseToJsonElement(decodedJson).jsonObject
        require(root.requiredInt("schemaVersion") == 1) { "不支持的导入格式版本" }
        val containsSecrets = root["containsSecrets"]?.jsonPrimitive?.boolean == true
        val sourceJson = root["configs"]?.jsonArray.orEmpty().map(JsonElement::jsonObject)
        val parsed = sourceJson.map(::parseConfig)
        require(parsed.isNotEmpty()) { "导入包不包含配置" }
        require(parsed.size <= MaxConfigs) { "导入配置数量过多" }
        require(parsed.map(TunnelConfig::id).distinct().size == parsed.size) { "导入包包含重复 ID" }
        val sourceJsonById = sourceJson.associateBy { UUID.fromString(it.requiredString("id")) }
        val current = repository.snapshot()
        val currentNames = current.associateBy { it.name.trim().lowercase() }
        val currentIds = current.associateBy(TunnelConfig::id)
        val idMap = parsed.associate { incoming ->
            val byId = currentIds[incoming.id]
            val byName = currentNames[incoming.name.trim().lowercase()]
            require(strategy != ImportConflictStrategy.REPLACE_LOCAL || byId == null || byName == null || byId.id == byName.id) {
                "导入项的 ID 与名称分别冲突到不同本地配置"
            }
            val targetId = when (strategy) {
                ImportConflictStrategy.CREATE_COPY -> if (byId != null || byName != null) UUID.randomUUID() else incoming.id
                ImportConflictStrategy.REPLACE_LOCAL -> byId?.id ?: byName?.id ?: incoming.id
                ImportConflictStrategy.SKIP -> incoming.id
            }
            incoming.id to targetId
        }
        val skippedIds = if (strategy == ImportConflictStrategy.SKIP) parsed.filter { incoming ->
            incoming.id in currentIds || incoming.name.trim().lowercase() in currentNames
        }.mapTo(mutableSetOf(), TunnelConfig::id) else mutableSetOf()
        var changed: Boolean
        do {
            changed = false
            parsed.forEach { config ->
                val dependency = config.dependencyId()
                if (config.id !in skippedIds && dependency in skippedIds && skippedIds.add(config.id)) changed = true
            }
        } while (changed)
        val selected = parsed.filterNot { it.id in skippedIds }.map { incoming ->
            val newId = idMap.getValue(incoming.id)
            incoming.copy(
                id = newId,
                spec = incoming.spec.remapDependency(idMap),
                revision = 1,
                createdAtEpochMillis = nowEpochMillis(),
                updatedAtEpochMillis = nowEpochMillis(),
            )
        }
        val replaceIds = if (strategy == ImportConflictStrategy.REPLACE_LOCAL) selected
            .map(TunnelConfig::id)
            .filterTo(mutableSetOf()) { it in currentIds }
        else emptySet()
        val normalized = selected
        DependencyGraph(current.filterNot { it.id in replaceIds } + normalized).validate()
        val sourceIdsByImportedId = parsed.filterNot { it.id in skippedIds }
            .associate { incoming -> idMap.getValue(incoming.id) to incoming.id }
        return ImportPreview(
            configs = normalized,
            replaceIds = replaceIds,
            skipped = skippedIds.size,
            credentialsRequired = normalized.filter { imported ->
                val sourceId = sourceIdsByImportedId.getValue(imported.id)
                !containsSecrets || !sourceJsonById.getValue(sourceId).hasRequiredCredentials(imported)
            }.mapTo(mutableSetOf(), TunnelConfig::id),
            sourceIdsByImportedId = sourceIdsByImportedId,
        )
    }

    suspend fun import(json: String, strategy: ImportConflictStrategy): ImportResult {
        val preview = preview(json, strategy)
        val decodedJson = QrConfigPayloadCodec.decodeIfEncoded(json)
        val root = Json.parseToJsonElement(decodedJson).jsonObject
        val containsSecrets = root["containsSecrets"]?.jsonPrimitive?.boolean == true
        val sourceJsonById = root["configs"]?.jsonArray.orEmpty()
            .map(JsonElement::jsonObject)
            .associateBy { UUID.fromString(it.requiredString("id")) }
        val createdRefs = mutableListOf<CredentialRef>()
        val imported = try {
            preview.configs.map { config ->
                if (!containsSecrets) config else config.withImportedCredentials(
                    sourceJsonById.getValue(preview.sourceIdsByImportedId.getValue(config.id)),
                    requireNotNull(credentialStore) { "此设备无法保存二维码中的敏感参数" },
                    createdRefs,
                )
            }.also { repository.replaceBatch(it, preview.replaceIds) }
        } catch (failure: Throwable) {
            credentialStore?.let { store -> createdRefs.forEach { runCatching { store.delete(it) } } }
            throw failure
        }
        val required = imported.filter(TunnelConfig::requiresCredentials).mapTo(mutableSetOf(), TunnelConfig::id)
        return ImportResult(imported.size, preview.skipped, required)
    }

    private companion object {
        const val MaxImportChars = 4 * 1024 * 1024
        const val MaxConfigs = 1_000
    }
}

private fun TunnelConfig.toJson(privacyMode: Boolean, credentials: JsonObject?): JsonObject = buildJsonObject {
    put("id", id.toString())
    put("name", name)
    put("description", description)
    put("localPort", localPort)
    put("kind", kind.name)
    put("autoRecover", autoRecover)
    put("favorite", favorite)
    put("sortOrder", sortOrder)
    put("spec", when (val value = spec) {
        is TunnelSpec.Ssh -> value.toJson(privacyMode, credentials)
        is TunnelSpec.Stcp -> value.toJson(privacyMode, credentials)
    })
}

private fun TunnelSpec.Ssh.toJson(privacyMode: Boolean, credentials: JsonObject?) = buildJsonObject {
    put("endpoint", endpoint.toJson(privacyMode))
    put("logicalHost", identity.logicalHost.privateValue(privacyMode))
    put("logicalPort", identity.logicalPort)
    put("username", username.privateValue(privacyMode))
    put("authMethod", authMethod.name)
    put("remoteTargetHost", remoteTargetHost.privateValue(privacyMode))
    put("remoteTargetPort", remoteTargetPort)
    put("hostKeyPolicy", hostKeyPolicy.name)
    put("connectTimeoutSeconds", connectTimeoutSeconds)
    put("keepAliveSeconds", keepAliveSeconds)
    credentials?.let { put("credentials", it) }
}

private fun TunnelSpec.Stcp.toJson(privacyMode: Boolean, credentials: JsonObject?) = buildJsonObject {
    put("endpoint", endpoint.toJson(privacyMode))
    put("logicalHost", identity.logicalHost.privateValue(privacyMode))
    put("logicalPort", identity.logicalPort)
    putNullable("user", user?.privateValue(privacyMode))
    putNullable("serverUser", serverUser?.privateValue(privacyMode))
    put("serverName", serverName.privateValue(privacyMode))
    put("wireProtocol", wireProtocol.name)
    put("useEncryption", useEncryption)
    put("useCompression", useCompression)
    putNullable("tlsLeafPinSha256", tlsLeafPinSha256)
    put("connectTimeoutSeconds", connectTimeoutSeconds)
    credentials?.let { put("credentials", it) }
}

private fun EndpointSource.toJson(privacyMode: Boolean) = buildJsonObject {
    when (this@toJson) {
        is EndpointSource.Remote -> {
            put("type", "REMOTE")
            put("host", host.privateValue(privacyMode))
            put("port", port)
        }
        is EndpointSource.Dependency -> {
            put("type", "DEPENDENCY")
            put("configId", configId.toString())
        }
    }
}

private fun String.privateValue(enabled: Boolean): String = if (enabled && isNotEmpty()) "<redacted>" else this

private fun parseConfig(value: JsonObject): TunnelConfig {
    val now = System.currentTimeMillis()
    val kind = enumValueOf<TunnelKind>(value.requiredString("kind"))
    val specJson = value.requiredObject("spec")
    return TunnelConfig(
        id = UUID.fromString(value.requiredString("id")),
        name = value.requiredString("name"),
        description = value.optionalString("description").orEmpty(),
        localPort = value.requiredInt("localPort"),
        spec = when (kind) {
            TunnelKind.SSH -> TunnelSpec.Ssh(
                endpoint = specJson.requiredObject("endpoint").parseEndpoint(),
                identity = SshServerIdentity(specJson.requiredString("logicalHost"), specJson.requiredInt("logicalPort")),
                username = specJson.requiredString("username"),
                authMethod = enumValueOf<SshAuthMethod>(specJson.requiredString("authMethod")),
                credentials = SshCredentialRefs(),
                remoteTargetHost = specJson.requiredString("remoteTargetHost"),
                remoteTargetPort = specJson.requiredInt("remoteTargetPort"),
                hostKeyPolicy = enumValueOf<SshHostKeyPolicy>(specJson.requiredString("hostKeyPolicy")),
                connectTimeoutSeconds = specJson.requiredInt("connectTimeoutSeconds"),
                keepAliveSeconds = specJson.requiredInt("keepAliveSeconds"),
            )
            TunnelKind.STCP -> TunnelSpec.Stcp(
                endpoint = specJson.requiredObject("endpoint").parseEndpoint(),
                identity = FrpsServerIdentity(specJson.requiredString("logicalHost"), specJson.requiredInt("logicalPort")),
                user = specJson.optionalString("user"),
                serverUser = specJson.optionalString("serverUser"),
                serverName = specJson.requiredString("serverName"),
                wireProtocol = enumValueOf<StcpWireProtocol>(specJson.requiredString("wireProtocol")),
                useEncryption = specJson.requiredBoolean("useEncryption"),
                useCompression = specJson.requiredBoolean("useCompression"),
                tlsLeafPinSha256 = specJson.optionalString("tlsLeafPinSha256"),
                connectTimeoutSeconds = specJson.requiredInt("connectTimeoutSeconds"),
            )
        },
        autoRecover = value.requiredBoolean("autoRecover"),
        favorite = value.requiredBoolean("favorite"),
        sortOrder = value["sortOrder"]?.jsonPrimitive?.long ?: 0L,
        createdAtEpochMillis = now,
        updatedAtEpochMillis = now,
    )
}

private fun JsonObject.parseEndpoint(): EndpointSource = when (requiredString("type")) {
    "REMOTE" -> EndpointSource.Remote(requiredString("host"), requiredInt("port"))
    "DEPENDENCY" -> EndpointSource.Dependency(UUID.fromString(requiredString("configId")))
    else -> error("未知连接入口类型")
}

private fun TunnelConfig.dependencyId(): UUID? = when (val value = spec) {
    is TunnelSpec.Ssh -> (value.endpoint as? EndpointSource.Dependency)?.configId
    is TunnelSpec.Stcp -> (value.endpoint as? EndpointSource.Dependency)?.configId
}

private fun TunnelSpec.remapDependency(ids: Map<UUID, UUID>): TunnelSpec = when (this) {
    is TunnelSpec.Ssh -> copy(endpoint = endpoint.remap(ids))
    is TunnelSpec.Stcp -> copy(endpoint = endpoint.remap(ids))
}

private fun EndpointSource.remap(ids: Map<UUID, UUID>): EndpointSource = when (this) {
    is EndpointSource.Remote -> this
    is EndpointSource.Dependency -> copy(configId = ids[configId] ?: configId)
}

private fun JsonObject.requiredString(key: String) = getValue(key).jsonPrimitive.content
private fun JsonObject.requiredInt(key: String) = getValue(key).jsonPrimitive.int
private fun JsonObject.requiredBoolean(key: String) = getValue(key).jsonPrimitive.boolean
private fun JsonObject.requiredObject(key: String) = getValue(key).jsonObject
private fun JsonObject.optionalString(key: String) = get(key)?.takeUnless { it is JsonNull }?.jsonPrimitive?.contentOrNull
private fun kotlinx.serialization.json.JsonObjectBuilder.putNullable(key: String, value: String?) {
    put(key, value?.let(::JsonPrimitive) ?: JsonNull)
}

private suspend fun TunnelConfig.credentialsToJson(store: CredentialStore): JsonObject = when (val value = spec) {
    is TunnelSpec.Ssh -> {
        val password = value.credentials.password?.let { store.read(it) }
        val privateKey = value.credentials.privateKey?.let { store.read(it) }
        val passphrase = value.credentials.passphrase?.let { store.read(it) }
        val hostFingerprint = value.credentials.hostFingerprint?.let { store.read(it) }
        buildJsonObject {
            putCredential("password", password)
            putCredential("privateKey", privateKey)
            putCredential("passphrase", passphrase)
            putCredential("hostFingerprint", hostFingerprint)
        }
    }
    is TunnelSpec.Stcp -> {
        val token = value.tokenRef?.let { store.read(it) }
        val secretKey = value.secretKeyRef?.let { store.read(it) }
        buildJsonObject {
            putCredential("token", token)
            putCredential("secretKey", secretKey)
        }
    }
}

private fun kotlinx.serialization.json.JsonObjectBuilder.putCredential(key: String, value: ByteArray?) {
    value?.let { put(key, Base64.getEncoder().encodeToString(it)) }
}

private fun JsonObject.hasRequiredCredentials(config: TunnelConfig): Boolean {
    val credentials = requiredObject("spec").optionalObject("credentials") ?: return false
    return when (val value = config.spec) {
        is TunnelSpec.Ssh -> "hostFingerprint" in credentials && when (value.authMethod) {
            SshAuthMethod.PASSWORD -> "password" in credentials
            SshAuthMethod.PRIVATE_KEY -> "privateKey" in credentials
        }
        is TunnelSpec.Stcp -> "secretKey" in credentials
    }
}

private suspend fun TunnelConfig.withImportedCredentials(
    source: JsonObject,
    store: CredentialStore,
    createdRefs: MutableList<CredentialRef>,
): TunnelConfig {
    val credentials = source.requiredObject("spec").optionalObject("credentials") ?: return this
    suspend fun create(key: String, kind: CredentialKind): CredentialRef? {
        val encoded = credentials.optionalString(key) ?: return null
        require(encoded.length <= MaxCredentialBase64Chars) { "敏感参数过大：$key" }
        val bytes = runCatching { Base64.getDecoder().decode(encoded) }
            .getOrElse { throw IllegalArgumentException("敏感参数编码无效：$key", it) }
        return store.put(kind, bytes).also(createdRefs::add)
    }
    return copy(spec = when (val value = spec) {
        is TunnelSpec.Ssh -> value.copy(
            credentials = SshCredentialRefs(
                password = create("password", CredentialKind.PASSWORD),
                privateKey = create("privateKey", CredentialKind.PRIVATE_KEY),
                passphrase = create("passphrase", CredentialKind.PRIVATE_KEY_PASSPHRASE),
                hostFingerprint = create("hostFingerprint", CredentialKind.SSH_HOST_FINGERPRINT),
            ),
        )
        is TunnelSpec.Stcp -> value.copy(
            tokenRef = create("token", CredentialKind.TOKEN),
            secretKeyRef = create("secretKey", CredentialKind.STCP_SECRET),
        )
    })
}

private fun JsonObject.optionalObject(key: String): JsonObject? = get(key)?.takeUnless { it is JsonNull }?.jsonObject
private const val MaxCredentialBase64Chars = 2 * 1024 * 1024
