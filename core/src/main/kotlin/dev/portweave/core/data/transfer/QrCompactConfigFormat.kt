package dev.portweave.core.data.transfer

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put

/** Positional V2 representation used only inside QR payloads. */
internal object QrCompactConfigFormat {
    fun compactIfTransferJson(json: String): String {
        val root = runCatching { Json.parseToJsonElement(json).jsonObject }.getOrNull() ?: return json
        if (root["schemaVersion"]?.jsonPrimitive?.int != 1 || root["configs"] !is JsonArray) return json
        return buildJsonArray {
            add(root["containsSecrets"]?.jsonPrimitive?.boolean == true)
            add(buildJsonArray { root.getValue("configs").jsonArray.forEach { add(compactConfig(it.jsonObject)) } })
        }.toString()
    }

    fun expandIfCompact(json: String): String {
        val root = runCatching { Json.parseToJsonElement(json) }.getOrElse {
            throw IllegalArgumentException("二维码紧凑数据无效", it)
        }
        if (root !is JsonArray) return json
        require(root.size == 2 && root[1] is JsonArray) { "二维码紧凑数据无效" }
        return buildJsonObject {
            put("schemaVersion", 1)
            put("containsSecrets", root[0].jsonPrimitive.boolean)
            put("configs", buildJsonArray { root[1].jsonArray.forEach { add(expandConfig(it.jsonArray)) } })
        }.toString()
    }

    private fun compactConfig(config: JsonObject): JsonArray {
        val kind = config.string("kind")
        val flags = (if (config.boolean("autoRecover")) 1 else 0) or
            (if (config.boolean("favorite")) 2 else 0)
        return buildJsonArray {
            add(config.string("id"))
            add(config.string("name"))
            add(config.string("description"))
            add(config.int("localPort"))
            add(if (kind == "SSH") 0 else 1)
            add(flags)
            add(config.getValue("sortOrder").jsonPrimitive.long)
            add(if (kind == "SSH") compactSsh(config.objectValue("spec")) else compactStcp(config.objectValue("spec")))
        }
    }

    private fun compactSsh(spec: JsonObject) = buildJsonArray {
        add(compactEndpoint(spec.objectValue("endpoint")))
        add(spec.string("logicalHost"))
        add(spec.int("logicalPort"))
        add(spec.string("username"))
        add(if (spec.string("authMethod") == "PASSWORD") 0 else 1)
        add(spec.string("remoteTargetHost"))
        add(spec.int("remoteTargetPort"))
        add(if (spec.string("hostKeyPolicy") == "TOFU") 0 else 1)
        add(spec.int("connectTimeoutSeconds"))
        add(spec.int("keepAliveSeconds"))
        spec["credentials"]?.let { add(compactCredentials(it.jsonObject, listOf("password", "privateKey", "passphrase", "hostFingerprint"))) }
    }

    private fun compactStcp(spec: JsonObject) = buildJsonArray {
        add(compactEndpoint(spec.objectValue("endpoint")))
        add(spec.string("logicalHost"))
        add(spec.int("logicalPort"))
        add(spec["user"] ?: JsonNull)
        add(spec["serverUser"] ?: JsonNull)
        add(spec.string("serverName"))
        add(if (spec.string("wireProtocol") == "V1") 0 else 1)
        add((if (spec.boolean("useEncryption")) 1 else 0) or (if (spec.boolean("useCompression")) 2 else 0))
        add(spec["tlsLeafPinSha256"] ?: JsonNull)
        add(spec.int("connectTimeoutSeconds"))
        spec["credentials"]?.let { add(compactCredentials(it.jsonObject, listOf("token", "secretKey"))) }
    }

    private fun compactEndpoint(endpoint: JsonObject) = buildJsonArray {
        if (endpoint.string("type") == "REMOTE") {
            add(0)
            add(endpoint.string("host"))
            add(endpoint.int("port"))
        } else {
            add(1)
            add(endpoint.string("configId"))
        }
    }

    private fun compactCredentials(credentials: JsonObject, keys: List<String>) = buildJsonArray {
        keys.forEach { add(credentials[it] ?: JsonNull) }
    }

    private fun expandConfig(config: JsonArray): JsonObject {
        require(config.size == 8) { "二维码配置字段数量无效" }
        val kind = config[4].jsonPrimitive.int
        val flags = config[5].jsonPrimitive.int
        return buildJsonObject {
            put("id", config[0])
            put("name", config[1])
            put("description", config[2])
            put("localPort", config[3])
            put("kind", if (kind == 0) "SSH" else "STCP")
            put("autoRecover", flags and 1 != 0)
            put("favorite", flags and 2 != 0)
            put("sortOrder", config[6])
            put("spec", if (kind == 0) expandSsh(config[7].jsonArray) else expandStcp(config[7].jsonArray))
        }
    }

    private fun expandSsh(spec: JsonArray): JsonObject {
        require(spec.size in 10..11) { "二维码 SSH 字段数量无效" }
        return buildJsonObject {
            put("endpoint", expandEndpoint(spec[0].jsonArray))
            put("logicalHost", spec[1])
            put("logicalPort", spec[2])
            put("username", spec[3])
            put("authMethod", if (spec[4].jsonPrimitive.int == 0) "PASSWORD" else "PRIVATE_KEY")
            put("remoteTargetHost", spec[5])
            put("remoteTargetPort", spec[6])
            put("hostKeyPolicy", if (spec[7].jsonPrimitive.int == 0) "TOFU" else "FIXED")
            put("connectTimeoutSeconds", spec[8])
            put("keepAliveSeconds", spec[9])
            if (spec.size == 11) put("credentials", expandCredentials(spec[10].jsonArray, listOf("password", "privateKey", "passphrase", "hostFingerprint")))
        }
    }

    private fun expandStcp(spec: JsonArray): JsonObject {
        require(spec.size in 10..11) { "二维码 STCP 字段数量无效" }
        val flags = spec[7].jsonPrimitive.int
        return buildJsonObject {
            put("endpoint", expandEndpoint(spec[0].jsonArray))
            put("logicalHost", spec[1])
            put("logicalPort", spec[2])
            put("user", spec[3])
            put("serverUser", spec[4])
            put("serverName", spec[5])
            put("wireProtocol", if (spec[6].jsonPrimitive.int == 0) "V1" else "V2")
            put("useEncryption", flags and 1 != 0)
            put("useCompression", flags and 2 != 0)
            put("tlsLeafPinSha256", spec[8])
            put("connectTimeoutSeconds", spec[9])
            if (spec.size == 11) put("credentials", expandCredentials(spec[10].jsonArray, listOf("token", "secretKey")))
        }
    }

    private fun expandEndpoint(endpoint: JsonArray): JsonObject = buildJsonObject {
        if (endpoint[0].jsonPrimitive.int == 0) {
            require(endpoint.size == 3) { "二维码远端入口字段数量无效" }
            put("type", "REMOTE")
            put("host", endpoint[1])
            put("port", endpoint[2])
        } else {
            require(endpoint.size == 2) { "二维码依赖入口字段数量无效" }
            put("type", "DEPENDENCY")
            put("configId", endpoint[1])
        }
    }

    private fun expandCredentials(credentials: JsonArray, keys: List<String>): JsonObject {
        require(credentials.size == keys.size) { "二维码凭据字段数量无效" }
        return buildJsonObject {
            keys.forEachIndexed { index, key ->
                credentials[index].takeUnless { it is JsonNull }?.let { put(key, it) }
            }
        }
    }

    private fun JsonObject.string(key: String) = getValue(key).jsonPrimitive.content
    private fun JsonObject.int(key: String) = getValue(key).jsonPrimitive.int
    private fun JsonObject.boolean(key: String) = getValue(key).jsonPrimitive.boolean
    private fun JsonObject.objectValue(key: String) = getValue(key).jsonObject
}
