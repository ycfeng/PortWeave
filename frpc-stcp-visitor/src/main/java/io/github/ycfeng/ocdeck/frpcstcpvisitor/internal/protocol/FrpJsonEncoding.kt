package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol

import java.util.Base64

internal object FrpJsonEncoding {
    fun encodeMessage(message: FrpMessage, maximumBytes: Int): ByteArray {
        val writer = BoundedJsonWriter(maximumBytes)
        when (message) {
            is Login -> writeLogin(writer, message)
            is LoginResp -> writeLoginResp(writer, message)
            is NewWorkConn -> writeNewWorkConn(writer, message)
            is ReqWorkConn -> writer.emptyObject()
            is StartWorkConn -> writeStartWorkConn(writer, message)
            is NewVisitorConn -> writeNewVisitorConn(writer, message)
            is NewVisitorConnResp -> writeNewVisitorConnResp(writer, message)
            is Ping -> writePing(writer, message)
            is Pong -> writePong(writer, message)
        }
        return writer.toByteArray()
    }

    fun encodeClientHello(hello: ClientHello, maximumBytes: Int): ByteArray {
        val writer = BoundedJsonWriter(maximumBytes)
        val fields = FieldState()
        writer.beginObject()
        writer.field(fields, "bootstrap") { writeBootstrap(writer, hello.bootstrap) }
        writer.field(fields, "capabilities") { writeClientCapabilities(writer, hello.capabilities) }
        writer.endObject()
        return writer.toByteArray()
    }

    fun encodeServerHello(hello: ServerHello, maximumBytes: Int): ByteArray {
        val writer = BoundedJsonWriter(maximumBytes)
        val fields = FieldState()
        writer.beginObject()
        writer.field(fields, "selected") { writeServerSelection(writer, hello.selected) }
        writer.stringField(fields, "error", hello.error)
        writer.endObject()
        return writer.toByteArray()
    }

    private fun writeLogin(writer: BoundedJsonWriter, message: Login) {
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "version", message.version)
        writer.stringField(fields, "hostname", message.hostname)
        writer.stringField(fields, "os", message.os)
        writer.stringField(fields, "arch", message.arch)
        writer.stringField(fields, "user", message.user)
        writer.stringField(fields, "privilege_key", message.privilegeKey)
        writer.longField(fields, "timestamp", message.timestamp)
        writer.stringField(fields, "run_id", message.runId)
        writer.stringField(fields, "client_id", message.clientId)
        if (message.metas.isNotEmpty()) {
            writer.field(fields, "metas") {
                val mapFields = FieldState()
                writer.beginObject()
                message.metas.entries.sortedWith { left, right ->
                    compareGoJsonKeys(left.key, right.key)
                }.forEach { (key, value) ->
                    writer.field(mapFields, key) { writer.string(value) }
                }
                writer.endObject()
            }
        }
        writer.field(fields, "client_spec") { writeClientSpec(writer, message.clientSpec) }
        writer.intField(fields, "pool_count", message.poolCount)
        writer.endObject()
    }

    private fun writeClientSpec(writer: BoundedJsonWriter, clientSpec: ClientSpec) {
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "type", clientSpec.type)
        writer.booleanField(fields, "always_auth_pass", clientSpec.alwaysAuthPass)
        writer.endObject()
    }

    private fun writeLoginResp(writer: BoundedJsonWriter, message: LoginResp) {
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "version", message.version)
        writer.stringField(fields, "run_id", message.runId)
        writer.stringField(fields, "error", message.error)
        writer.endObject()
    }

    private fun writeNewWorkConn(writer: BoundedJsonWriter, message: NewWorkConn) {
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "run_id", message.runId)
        writer.stringField(fields, "privilege_key", message.privilegeKey)
        writer.longField(fields, "timestamp", message.timestamp)
        writer.endObject()
    }

    private fun writeStartWorkConn(writer: BoundedJsonWriter, message: StartWorkConn) {
        validatePort(message.srcPort)
        validatePort(message.dstPort)
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "proxy_name", message.proxyName)
        writer.stringField(fields, "src_addr", message.srcAddr)
        writer.stringField(fields, "dst_addr", message.dstAddr)
        writer.intField(fields, "src_port", message.srcPort)
        writer.intField(fields, "dst_port", message.dstPort)
        writer.stringField(fields, "error", message.error)
        writer.endObject()
    }

    private fun writeNewVisitorConn(writer: BoundedJsonWriter, message: NewVisitorConn) {
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "run_id", message.runId)
        writer.stringField(fields, "proxy_name", message.proxyName)
        writer.stringField(fields, "sign_key", message.signKey)
        writer.longField(fields, "timestamp", message.timestamp)
        writer.booleanField(fields, "use_encryption", message.useEncryption)
        writer.booleanField(fields, "use_compression", message.useCompression)
        writer.endObject()
    }

    private fun writeNewVisitorConnResp(writer: BoundedJsonWriter, message: NewVisitorConnResp) {
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "proxy_name", message.proxyName)
        writer.stringField(fields, "error", message.error)
        writer.endObject()
    }

    private fun writePing(writer: BoundedJsonWriter, message: Ping) {
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "privilege_key", message.privilegeKey)
        writer.longField(fields, "timestamp", message.timestamp)
        writer.endObject()
    }

    private fun writePong(writer: BoundedJsonWriter, message: Pong) {
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "error", message.error)
        writer.endObject()
    }

    private fun writeBootstrap(writer: BoundedJsonWriter, bootstrap: BootstrapInfo) {
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "transport", bootstrap.transport)
        writer.booleanField(fields, "tls", bootstrap.tls)
        writer.booleanField(fields, "tcpMux", bootstrap.tcpMux)
        writer.endObject()
    }

    private fun writeClientCapabilities(writer: BoundedJsonWriter, capabilities: ClientCapabilities) {
        val fields = FieldState()
        writer.beginObject()
        writer.field(fields, "message") { writeMessageCapabilities(writer, capabilities.message) }
        writer.field(fields, "crypto") { writeCryptoCapabilities(writer, capabilities.crypto) }
        writer.endObject()
    }

    private fun writeMessageCapabilities(writer: BoundedJsonWriter, capabilities: MessageCapabilities) {
        val fields = FieldState()
        writer.beginObject()
        if (capabilities.codecs.isNotEmpty()) {
            writer.field(fields, "codecs") { writer.stringArray(capabilities.codecs) }
        }
        writer.endObject()
    }

    private fun writeCryptoCapabilities(writer: BoundedJsonWriter, capabilities: CryptoCapabilities) {
        val fields = FieldState()
        writer.beginObject()
        if (capabilities.algorithms.isNotEmpty()) {
            writer.field(fields, "algorithms") { writer.stringArray(capabilities.algorithms) }
        }
        capabilities.clientRandom?.takeIf { it.isNotEmpty() }?.let { random ->
            if (random.size != FrpWireV2.CRYPTO_RANDOM_SIZE) {
                throw FrpProtocolException(FrpProtocolFailure.INVALID_HELLO)
            }
            writer.field(fields, "clientRandom") { writer.string(Base64.getEncoder().encodeToString(random)) }
        }
        writer.endObject()
    }

    private fun writeServerSelection(writer: BoundedJsonWriter, selection: ServerSelection) {
        val fields = FieldState()
        writer.beginObject()
        writer.field(fields, "message") { writeMessageSelection(writer, selection.message) }
        writer.field(fields, "crypto") { writeCryptoSelection(writer, selection.crypto) }
        writer.endObject()
    }

    private fun writeMessageSelection(writer: BoundedJsonWriter, selection: MessageSelection) {
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "codec", selection.codec)
        writer.endObject()
    }

    private fun writeCryptoSelection(writer: BoundedJsonWriter, selection: CryptoSelection) {
        val fields = FieldState()
        writer.beginObject()
        writer.stringField(fields, "algorithm", selection.algorithm)
        selection.serverRandom?.takeIf { it.isNotEmpty() }?.let { random ->
            if (random.size != FrpWireV2.CRYPTO_RANDOM_SIZE) {
                throw FrpProtocolException(FrpProtocolFailure.INVALID_HELLO)
            }
            writer.field(fields, "serverRandom") { writer.string(Base64.getEncoder().encodeToString(random)) }
        }
        writer.endObject()
    }

    private fun validatePort(port: Int) {
        if (port !in 0..65_535) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_FIELD)
        }
    }

    private fun compareGoJsonKeys(left: String, right: String): Int {
        var leftIndex = 0
        var rightIndex = 0
        while (leftIndex < left.length && rightIndex < right.length) {
            val leftCodePoint = jsonCodePointAt(left, leftIndex)
            val rightCodePoint = jsonCodePointAt(right, rightIndex)
            if (leftCodePoint != rightCodePoint) {
                return leftCodePoint.compareTo(rightCodePoint)
            }
            leftIndex += if (leftCodePoint > Char.MAX_VALUE.code) 2 else 1
            rightIndex += if (rightCodePoint > Char.MAX_VALUE.code) 2 else 1
        }
        return (left.length - leftIndex).compareTo(right.length - rightIndex)
    }

    private fun jsonCodePointAt(value: String, index: Int): Int {
        val character = value[index]
        return if (Character.isHighSurrogate(character) &&
            index + 1 < value.length &&
            Character.isLowSurrogate(value[index + 1])
        ) {
            Character.toCodePoint(character, value[index + 1])
        } else if (Character.isSurrogate(character)) {
            REPLACEMENT_CHARACTER
        } else {
            character.code
        }
    }

    private const val REPLACEMENT_CHARACTER = 0xfffd
}

private class FieldState(var first: Boolean = true)

private class BoundedJsonWriter(maximumBytes: Int) {
    private val output = BoundedByteArrayOutput(maximumBytes)

    fun beginObject() = byte('{')

    fun endObject() = byte('}')

    fun emptyObject() {
        beginObject()
        endObject()
    }

    fun field(fields: FieldState, name: String, value: () -> Unit) {
        if (!fields.first) {
            byte(',')
        }
        fields.first = false
        string(name)
        byte(':')
        value()
    }

    fun stringField(fields: FieldState, name: String, value: String) {
        if (value.isNotEmpty()) {
            field(fields, name) { string(value) }
        }
    }

    fun booleanField(fields: FieldState, name: String, value: Boolean) {
        if (value) {
            field(fields, name) { ascii("true") }
        }
    }

    fun intField(fields: FieldState, name: String, value: Int) {
        if (value != 0) {
            field(fields, name) { ascii(value.toString()) }
        }
    }

    fun longField(fields: FieldState, name: String, value: Long) {
        if (value != 0L) {
            field(fields, name) { ascii(value.toString()) }
        }
    }

    fun stringArray(values: List<String>) {
        byte('[')
        values.forEachIndexed { index, value ->
            if (index > 0) {
                byte(',')
            }
            string(value)
        }
        byte(']')
    }

    fun string(value: String) {
        byte('"')
        var index = 0
        while (index < value.length) {
            val character = value[index]
            when (character) {
                '"' -> ascii("\\\"")
                '\\' -> ascii("\\\\")
                '\b' -> ascii("\\b")
                '\u000c' -> ascii("\\f")
                '\n' -> ascii("\\n")
                '\r' -> ascii("\\r")
                '\t' -> ascii("\\t")
                '<' -> ascii("\\u003c")
                '>' -> ascii("\\u003e")
                '&' -> ascii("\\u0026")
                '\u2028' -> ascii("\\u2028")
                '\u2029' -> ascii("\\u2029")
                else -> {
                    when {
                        character.code < 0x20 -> unicodeEscape(character.code)
                        Character.isHighSurrogate(character) &&
                            index + 1 < value.length &&
                            Character.isLowSurrogate(value[index + 1]) -> {
                            utf8(Character.toCodePoint(character, value[index + 1]))
                            index++
                        }
                        Character.isSurrogate(character) -> utf8(REPLACEMENT_CHARACTER)
                        else -> utf8(character.code)
                    }
                }
            }
            index++
        }
        byte('"')
    }

    fun toByteArray(): ByteArray = output.toByteArray()

    private fun unicodeEscape(value: Int) {
        ascii("\\u00")
        byte(HEX_DIGITS[(value ushr 4) and 0x0f])
        byte(HEX_DIGITS[value and 0x0f])
    }

    private fun utf8(codePoint: Int) {
        when {
            codePoint <= 0x7f -> output.write(codePoint)
            codePoint <= 0x7ff -> {
                output.write(0xc0 or (codePoint ushr 6))
                output.write(0x80 or (codePoint and 0x3f))
            }
            codePoint <= 0xffff -> {
                output.write(0xe0 or (codePoint ushr 12))
                output.write(0x80 or ((codePoint ushr 6) and 0x3f))
                output.write(0x80 or (codePoint and 0x3f))
            }
            else -> {
                output.write(0xf0 or (codePoint ushr 18))
                output.write(0x80 or ((codePoint ushr 12) and 0x3f))
                output.write(0x80 or ((codePoint ushr 6) and 0x3f))
                output.write(0x80 or (codePoint and 0x3f))
            }
        }
    }

    private fun ascii(value: String) {
        value.forEach { character -> output.write(character.code) }
    }

    private fun byte(value: Char) = output.write(value.code)

    private companion object {
        const val HEX_DIGITS = "0123456789abcdef"
        const val REPLACEMENT_CHARACTER = 0xfffd
    }
}
