package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull

internal object FrpJsonValidation {
    fun requireObject(element: JsonElement): JsonObject =
        element as? JsonObject ?: throw FrpProtocolException(FrpProtocolFailure.INVALID_FIELD)

    fun validateMessage(type: Int, objectValue: JsonObject) {
        when (type) {
            FrpMessageTypes.V1_LOGIN, FrpMessageTypes.V2_LOGIN -> {
                objectValue.strings(
                    "version",
                    "hostname",
                    "os",
                    "arch",
                    "user",
                    "privilege_key",
                    "run_id",
                    "client_id",
                )
                objectValue.long("timestamp")
                objectValue.int("pool_count")
                objectValue.objectField("metas") { metas ->
                    metas.values.forEach(::requireStringOrNull)
                }
                objectValue.objectField("client_spec") { clientSpec ->
                    clientSpec.string("type")
                    clientSpec.boolean("always_auth_pass")
                }
            }
            FrpMessageTypes.V1_LOGIN_RESP, FrpMessageTypes.V2_LOGIN_RESP ->
                objectValue.strings("version", "run_id", "error")
            FrpMessageTypes.V1_NEW_WORK_CONN, FrpMessageTypes.V2_NEW_WORK_CONN -> {
                objectValue.strings("run_id", "privilege_key")
                objectValue.long("timestamp")
            }
            FrpMessageTypes.V1_REQ_WORK_CONN, FrpMessageTypes.V2_REQ_WORK_CONN -> Unit
            FrpMessageTypes.V1_START_WORK_CONN, FrpMessageTypes.V2_START_WORK_CONN -> {
                objectValue.strings("proxy_name", "src_addr", "dst_addr", "error")
                objectValue.int("src_port")
                objectValue.int("dst_port")
            }
            FrpMessageTypes.V1_NEW_VISITOR_CONN, FrpMessageTypes.V2_NEW_VISITOR_CONN -> {
                objectValue.strings("run_id", "proxy_name", "sign_key")
                objectValue.long("timestamp")
                objectValue.boolean("use_encryption")
                objectValue.boolean("use_compression")
            }
            FrpMessageTypes.V1_NEW_VISITOR_CONN_RESP, FrpMessageTypes.V2_NEW_VISITOR_CONN_RESP ->
                objectValue.strings("proxy_name", "error")
            FrpMessageTypes.V1_PING, FrpMessageTypes.V2_PING -> {
                objectValue.string("privilege_key")
                objectValue.long("timestamp")
            }
            FrpMessageTypes.V1_PONG, FrpMessageTypes.V2_PONG -> objectValue.string("error")
        }
    }

    fun validateClientHello(objectValue: JsonObject) {
        objectValue.objectField("bootstrap") { bootstrap ->
            bootstrap.string("transport")
            bootstrap.boolean("tls")
            bootstrap.boolean("tcpMux")
        }
        objectValue.objectField("capabilities") { capabilities ->
            capabilities.objectField("message") { message -> message.stringArray("codecs") }
            capabilities.objectField("crypto") { crypto ->
                crypto.stringArray("algorithms")
                crypto.string("clientRandom")
            }
        }
    }

    fun validateServerHello(objectValue: JsonObject) {
        objectValue.objectField("selected") { selected ->
            selected.objectField("message") { message -> message.string("codec") }
            selected.objectField("crypto") { crypto ->
                crypto.string("algorithm")
                crypto.string("serverRandom")
            }
        }
        objectValue.string("error")
    }

    private fun JsonObject.strings(vararg names: String) {
        names.forEach { name -> string(name) }
    }

    private fun JsonObject.string(name: String) {
        field(name, ::requireStringOrNull)
    }

    private fun JsonObject.boolean(name: String) {
        field(name) { value ->
            if (value !== JsonNull && (value !is JsonPrimitive || value.isString || value.booleanOrNull == null)) {
                invalidField()
            }
        }
    }

    private fun JsonObject.int(name: String) {
        field(name) { value ->
            if (value !== JsonNull && (value !is JsonPrimitive || value.isString || value.intOrNull == null)) {
                invalidField()
            }
        }
    }

    private fun JsonObject.long(name: String) {
        field(name) { value ->
            if (value !== JsonNull && (value !is JsonPrimitive || value.isString || value.longOrNull == null)) {
                invalidField()
            }
        }
    }

    private fun JsonObject.objectField(name: String, validate: (JsonObject) -> Unit) {
        field(name) { value ->
            if (value === JsonNull) {
                return@field
            }
            validate(value as? JsonObject ?: invalidField())
        }
    }

    private fun JsonObject.stringArray(name: String) {
        field(name) { value ->
            if (value === JsonNull) {
                return@field
            }
            val array = value as? JsonArray ?: invalidField()
            array.forEach(::requireStringOrNull)
        }
    }

    private fun JsonObject.field(name: String, validate: (JsonElement) -> Unit) {
        get(name)?.let(validate)
    }

    private fun requireStringOrNull(value: JsonElement) {
        if (value !== JsonNull && (value !is JsonPrimitive || !value.isString)) {
            invalidField()
        }
    }

    private fun invalidField(): Nothing =
        throw FrpProtocolException(FrpProtocolFailure.INVALID_FIELD)
}
