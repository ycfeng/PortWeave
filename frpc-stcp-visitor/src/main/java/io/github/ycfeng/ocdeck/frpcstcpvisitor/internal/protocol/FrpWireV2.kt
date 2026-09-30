@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpMuxStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.CharacterCodingException
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.CancellationException
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.decodeFromJsonElement

internal const val FRP_AEAD_AES_256_GCM = "aes-256-gcm"
internal const val FRP_AEAD_XCHACHA20_POLY1305 = "xchacha20-poly1305"

@Serializable
internal data class BootstrapInfo(
    val transport: String = "",
    val tls: Boolean = false,
    val tcpMux: Boolean = false,
)

@Serializable
internal data class MessageCapabilities(
    val codecs: List<String> = emptyList(),
)

@Serializable
internal data class CryptoCapabilities(
    val algorithms: List<String> = emptyList(),
    @Serializable(with = CryptoRandomSerializer::class)
    val clientRandom: ByteArray? = null,
) {
    override fun toString(): String =
        "CryptoCapabilities(algorithmCount=${algorithms.size}, clientRandomLength=${clientRandom?.size ?: 0})"
}

@Serializable
internal data class ClientCapabilities(
    @EncodeDefault(EncodeDefault.Mode.ALWAYS)
    val message: MessageCapabilities = MessageCapabilities(),
    @EncodeDefault(EncodeDefault.Mode.ALWAYS)
    val crypto: CryptoCapabilities = CryptoCapabilities(),
)

@Serializable
internal data class ClientHello(
    @EncodeDefault(EncodeDefault.Mode.ALWAYS)
    val bootstrap: BootstrapInfo = BootstrapInfo(),
    @EncodeDefault(EncodeDefault.Mode.ALWAYS)
    val capabilities: ClientCapabilities = ClientCapabilities(),
) {
    override fun toString(): String =
        "ClientHello(transportPresent=${bootstrap.transport.isNotEmpty()}, tls=${bootstrap.tls}, " +
            "tcpMux=${bootstrap.tcpMux}, codecCount=${capabilities.message.codecs.size}, " +
            "algorithmCount=${capabilities.crypto.algorithms.size}, " +
            "clientRandomLength=${capabilities.crypto.clientRandom?.size ?: 0})"
}

@Serializable
internal data class MessageSelection(
    val codec: String = "",
)

@Serializable
internal data class CryptoSelection(
    val algorithm: String = "",
    @Serializable(with = CryptoRandomSerializer::class)
    val serverRandom: ByteArray? = null,
) {
    override fun toString(): String =
        "CryptoSelection(algorithmPresent=${algorithm.isNotEmpty()}, serverRandomLength=${serverRandom?.size ?: 0})"
}

@Serializable
internal data class ServerSelection(
    @EncodeDefault(EncodeDefault.Mode.ALWAYS)
    val message: MessageSelection = MessageSelection(),
    @EncodeDefault(EncodeDefault.Mode.ALWAYS)
    val crypto: CryptoSelection = CryptoSelection(),
)

@Serializable
internal data class ServerHello(
    @EncodeDefault(EncodeDefault.Mode.ALWAYS)
    val selected: ServerSelection = ServerSelection(),
    val error: String = "",
) {
    override fun toString(): String =
        "ServerHello(codecPresent=${selected.message.codec.isNotEmpty()}, " +
            "algorithmPresent=${selected.crypto.algorithm.isNotEmpty()}, " +
            "serverRandomLength=${selected.crypto.serverRandom?.size ?: 0}, errorPresent=${error.isNotEmpty()})"
}

internal object CryptoRandomSerializer : KSerializer<ByteArray> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("FrpCryptoRandom", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: ByteArray) {
        if (value.size != FrpWireV2.CRYPTO_RANDOM_SIZE) {
            throw SerializationException("invalid frp crypto random")
        }
        encoder.encodeString(Base64.getEncoder().encodeToString(value))
    }

    override fun deserialize(decoder: Decoder): ByteArray {
        val encoded = decoder.decodeString()
        if (encoded.length > MAX_ENCODED_LENGTH ||
            encoded.length != MAX_ENCODED_LENGTH ||
            !ENCODED_RANDOM.matches(encoded)
        ) {
            throw SerializationException("invalid frp crypto random")
        }
        val decoded = try {
            Base64.getDecoder().decode(encoded)
        } catch (exception: IllegalArgumentException) {
            throw SerializationException("invalid frp crypto random")
        }
        if (decoded.size != FrpWireV2.CRYPTO_RANDOM_SIZE) {
            throw SerializationException("invalid frp crypto random")
        }
        return decoded
    }

    private const val MAX_ENCODED_LENGTH = 44
    private val ENCODED_RANDOM = Regex("[A-Za-z0-9+/]{43}=")
}

internal class FrpV2Frame(
    val type: Int,
    val flags: Int = 0,
    payload: ByteArray,
) {
    val payload: ByteArray = payload.copyOf()

    override fun toString(): String =
        "FrpV2Frame(type=$type, flags=$flags, payloadLength=${payload.size})"
}

internal class RawClientHello(
    val value: ClientHello,
    rawPayload: ByteArray,
) {
    val rawPayload: ByteArray = rawPayload.copyOf()

    override fun toString(): String =
        "RawClientHello(payloadLength=${rawPayload.size}, value=$value)"
}

internal class RawServerHello(
    val value: ServerHello,
    rawPayload: ByteArray,
) {
    val rawPayload: ByteArray = rawPayload.copyOf()

    override fun toString(): String =
        "RawServerHello(payloadLength=${rawPayload.size}, value=$value)"
}

internal object FrpWireV2 {
    const val FRAME_TYPE_CLIENT_HELLO = 1
    const val FRAME_TYPE_SERVER_HELLO = 2
    const val FRAME_TYPE_MESSAGE = 16
    const val MAX_FRAME_PAYLOAD = 65_536
    const val CRYPTO_RANDOM_SIZE = 32
    const val MESSAGE_CODEC_JSON = "json"

    val magic: ByteArray
        get() = MAGIC_BYTES.copyOf()

    fun writeMagic(output: OutputStream) {
        writeSafely(output, MAGIC_BYTES)
    }

    fun readMagic(input: InputStream) {
        val actual = ByteArray(MAGIC_BYTES.size)
        val first = readByteSafely(input)
        if (first < 0) {
            throw FrpProtocolException(FrpProtocolFailure.TRUNCATED_MAGIC)
        }
        actual[0] = first.toByte()
        readFullySafely(
            input = input,
            destination = actual,
            offset = 1,
            length = actual.size - 1,
            truncatedFailure = FrpProtocolFailure.TRUNCATED_MAGIC,
        )
        if (!actual.contentEquals(MAGIC_BYTES)) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_MAGIC)
        }
    }

    fun readFrame(input: InputStream): FrpV2Frame? {
        val first = readByteSafely(input)
        if (first < 0) {
            return null
        }
        val header = ByteArray(FRAME_HEADER_SIZE)
        header[0] = first.toByte()
        readFullySafely(
            input = input,
            destination = header,
            offset = 1,
            length = FRAME_HEADER_SIZE - 1,
            truncatedFailure = FrpProtocolFailure.TRUNCATED_HEADER,
        )

        val type = decodeUInt16(header, 0)
        val flags = decodeUInt16(header, 2)
        val payloadLength = decodeUInt32(header, 4)
        if (!isKnownFrameType(type)) {
            throw FrpProtocolException(FrpProtocolFailure.UNKNOWN_TYPE)
        }
        if (flags != 0) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_FLAGS)
        }
        if (payloadLength > MAX_FRAME_PAYLOAD.toLong()) {
            throw FrpProtocolException(FrpProtocolFailure.LENGTH_LIMIT)
        }

        val payload = ByteArray(payloadLength.toInt())
        readFullySafely(
            input = input,
            destination = payload,
            truncatedFailure = FrpProtocolFailure.TRUNCATED_BODY,
        )
        return FrpV2Frame(type, flags, payload)
    }

    suspend fun readFrame(input: FrpMuxStream): FrpV2Frame? {
        val first = readByteSafely(input)
        if (first < 0) return null
        val header = ByteArray(FRAME_HEADER_SIZE)
        var payload: ByteArray? = null
        try {
            header[0] = first.toByte()
            readFullySafely(
                input = input,
                destination = header,
                offset = 1,
                length = FRAME_HEADER_SIZE - 1,
                truncatedFailure = FrpProtocolFailure.TRUNCATED_HEADER,
            )

            val type = decodeUInt16(header, 0)
            val flags = decodeUInt16(header, 2)
            val payloadLength = decodeUInt32(header, 4)
            if (!isKnownFrameType(type)) throw FrpProtocolException(FrpProtocolFailure.UNKNOWN_TYPE)
            if (flags != 0) throw FrpProtocolException(FrpProtocolFailure.INVALID_FLAGS)
            if (payloadLength > MAX_FRAME_PAYLOAD.toLong()) {
                throw FrpProtocolException(FrpProtocolFailure.LENGTH_LIMIT)
            }

            payload = ByteArray(payloadLength.toInt())
            readFullySafely(
                input = input,
                destination = payload,
                truncatedFailure = FrpProtocolFailure.TRUNCATED_BODY,
            )
            return FrpV2Frame(type, flags, payload)
        } finally {
            header.fill(0)
            payload?.fill(0)
        }
    }

    fun writeFrame(output: OutputStream, frame: FrpV2Frame) {
        if (!isKnownFrameType(frame.type)) {
            throw FrpProtocolException(FrpProtocolFailure.UNKNOWN_TYPE)
        }
        if (frame.flags != 0) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_FLAGS)
        }
        if (frame.payload.size > MAX_FRAME_PAYLOAD) {
            throw FrpProtocolException(FrpProtocolFailure.LENGTH_LIMIT)
        }
        val header = ByteArray(FRAME_HEADER_SIZE)
        encodeUInt16(frame.type, header, 0)
        encodeUInt16(frame.flags, header, 2)
        encodeUInt32(frame.payload.size.toLong(), header, 4)
        writeSafely(output, header)
        writeSafely(output, frame.payload)
    }

    fun createClientHello(
        bootstrap: BootstrapInfo,
        algorithms: List<String> = listOf(FRP_AEAD_AES_256_GCM, FRP_AEAD_XCHACHA20_POLY1305),
        secureRandom: SecureRandom = SecureRandom(),
    ): ClientHello {
        val random = ByteArray(CRYPTO_RANDOM_SIZE)
        try {
            secureRandom.nextBytes(random)
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: Exception) {
            throw FrpProtocolException(FrpProtocolFailure.IO_FAILURE)
        }
        return ClientHello(
            bootstrap = bootstrap,
            capabilities = ClientCapabilities(
                message = MessageCapabilities(listOf(MESSAGE_CODEC_JSON)),
                crypto = CryptoCapabilities(algorithms.toList(), random),
            ),
        ).also(::validateClientHello)
    }

    fun createServerHello(
        clientHello: ClientHello,
        secureRandom: SecureRandom = SecureRandom(),
    ): ServerHello {
        validateClientHello(clientHello)
        val algorithm = selectAeadAlgorithm(clientHello.capabilities.crypto.algorithms)
            ?: throw FrpProtocolException(FrpProtocolFailure.UNSUPPORTED)
        val random = ByteArray(CRYPTO_RANDOM_SIZE)
        try {
            secureRandom.nextBytes(random)
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: Exception) {
            throw FrpProtocolException(FrpProtocolFailure.IO_FAILURE)
        }
        return ServerHello(
            selected = ServerSelection(
                message = MessageSelection(MESSAGE_CODEC_JSON),
                crypto = CryptoSelection(algorithm, random),
            ),
        )
    }

    fun encodeClientHello(hello: ClientHello): RawClientHello {
        validateClientHello(hello)
        val payload = FrpJsonEncoding.encodeClientHello(hello, MAX_FRAME_PAYLOAD)
        return RawClientHello(hello, payload)
    }

    fun encodeServerHello(hello: ServerHello): RawServerHello {
        if (hello.error.isEmpty()) {
            validateStandaloneServerHello(hello)
        }
        val payload = FrpJsonEncoding.encodeServerHello(hello, MAX_FRAME_PAYLOAD)
        return RawServerHello(hello, payload)
    }

    fun writeClientHello(output: OutputStream, hello: ClientHello): RawClientHello =
        encodeClientHello(hello).also { raw ->
            writeFrame(output, FrpV2Frame(FRAME_TYPE_CLIENT_HELLO, payload = raw.rawPayload))
        }

    fun writeServerHello(output: OutputStream, hello: ServerHello): RawServerHello =
        encodeServerHello(hello).also { raw ->
            writeFrame(output, FrpV2Frame(FRAME_TYPE_SERVER_HELLO, payload = raw.rawPayload))
        }

    fun readClientHello(input: InputStream): RawClientHello {
        val frame = readFrame(input)
            ?: throw FrpProtocolException(FrpProtocolFailure.TRUNCATED_HEADER)
        if (frame.type != FRAME_TYPE_CLIENT_HELLO) {
            throw FrpProtocolException(FrpProtocolFailure.UNEXPECTED_TYPE)
        }
        val hello = decodeHello(ClientHello.serializer(), frame.payload, FrpJsonValidation::validateClientHello)
        validateClientHello(hello)
        return RawClientHello(hello, frame.payload)
    }

    fun readServerHello(input: InputStream): RawServerHello {
        val frame = readFrame(input)
            ?: throw FrpProtocolException(FrpProtocolFailure.TRUNCATED_HEADER)
        if (frame.type != FRAME_TYPE_SERVER_HELLO) {
            throw FrpProtocolException(FrpProtocolFailure.UNEXPECTED_TYPE)
        }
        val hello = decodeHello(ServerHello.serializer(), frame.payload, FrpJsonValidation::validateServerHello)
        if (hello.error.isEmpty()) {
            validateStandaloneServerHello(hello)
        }
        return RawServerHello(hello, frame.payload)
    }

    fun validateServerHelloForClient(clientHello: ClientHello, serverHello: ServerHello) {
        if (serverHello.error.isNotEmpty()) {
            return
        }
        validateClientHello(clientHello)
        validateStandaloneServerHello(serverHello)
        if (serverHello.selected.crypto.algorithm !in clientHello.capabilities.crypto.algorithms) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_HELLO)
        }
    }

    fun validateClientCryptoNegotiation(
        clientHello: RawClientHello,
        serverHello: RawServerHello,
    ): String {
        val decodedClientHello = decodeHello(
            ClientHello.serializer(),
            clientHello.rawPayload,
            FrpJsonValidation::validateClientHello,
        )
        validateClientHello(decodedClientHello)
        val decodedServerHello = decodeHello(
            ServerHello.serializer(),
            serverHello.rawPayload,
            FrpJsonValidation::validateServerHello,
        )
        if (decodedServerHello.error.isNotEmpty()) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_HELLO)
        }
        validateServerHelloForClient(decodedClientHello, decodedServerHello)
        return decodedServerHello.selected.crypto.algorithm
    }

    fun readMessage(input: InputStream): FrpMessage? {
        val frame = readFrame(input) ?: return null
        return decodeMessageFrame(frame)
    }

    suspend fun readMessage(input: FrpMuxStream): FrpMessage? {
        val frame = readFrame(input) ?: return null
        return decodeMessageFrame(frame)
    }

    fun writeMessage(output: OutputStream, message: FrpMessage) {
        val frame = encodeMessageFrame(message)
        try {
            writeFrame(output, frame)
        } finally {
            frame.payload.fill(0)
        }
    }

    fun encodeMessage(message: FrpMessage): ByteArray {
        val output = ClearingByteArrayOutputStream(128)
        return try {
            writeMessage(output, message)
            output.copyAndClear()
        } catch (failure: Throwable) {
            output.clear()
            throw failure
        }
    }

    fun encodeMessageWithMagic(message: FrpMessage): ByteArray {
        val encoded = encodeMessage(message)
        return try {
            ByteArray(MAGIC_BYTES.size + encoded.size).also { combined ->
                MAGIC_BYTES.copyInto(combined)
                encoded.copyInto(combined, destinationOffset = MAGIC_BYTES.size)
            }
        } finally {
            encoded.fill(0)
        }
    }

    fun encodeMessageFrame(message: FrpMessage): FrpV2Frame {
        val jsonPayload = FrpMessageJson.encode(message, MAX_FRAME_PAYLOAD - MESSAGE_TYPE_SIZE)
        val payload = ByteArray(MESSAGE_TYPE_SIZE + jsonPayload.size)
        return try {
            encodeUInt16(FrpMessageTypes.v2TypeOf(message), payload, 0)
            jsonPayload.copyInto(payload, destinationOffset = MESSAGE_TYPE_SIZE)
            FrpV2Frame(FRAME_TYPE_MESSAGE, payload = payload)
        } finally {
            jsonPayload.fill(0)
            payload.fill(0)
        }
    }

    fun decodeMessageFrame(frame: FrpV2Frame): FrpMessage {
        if (frame.type != FRAME_TYPE_MESSAGE) {
            throw FrpProtocolException(FrpProtocolFailure.UNEXPECTED_TYPE)
        }
        if (frame.payload.size < MESSAGE_TYPE_SIZE) {
            throw FrpProtocolException(FrpProtocolFailure.TRUNCATED_BODY)
        }
        val messageType = decodeUInt16(frame.payload, 0)
        if (!FrpMessageTypes.isKnownV2(messageType)) {
            throw FrpProtocolException(FrpProtocolFailure.UNKNOWN_TYPE)
        }
        val jsonPayload = frame.payload.copyOfRange(MESSAGE_TYPE_SIZE, frame.payload.size)
        return FrpMessageJson.decodeV2(messageType, jsonPayload)
    }

    fun selectAeadAlgorithm(algorithms: List<String>): String? =
        algorithms.firstOrNull(::isSupportedAeadAlgorithm)

    fun isSupportedAeadAlgorithm(algorithm: String): Boolean =
        algorithm == FRP_AEAD_AES_256_GCM || algorithm == FRP_AEAD_XCHACHA20_POLY1305

    private fun validateClientHello(hello: ClientHello) {
        if (MESSAGE_CODEC_JSON !in hello.capabilities.message.codecs ||
            hello.capabilities.crypto.clientRandom?.size != CRYPTO_RANDOM_SIZE ||
            selectAeadAlgorithm(hello.capabilities.crypto.algorithms) == null
        ) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_HELLO)
        }
    }

    private fun validateStandaloneServerHello(hello: ServerHello) {
        if (hello.selected.message.codec != MESSAGE_CODEC_JSON ||
            !isSupportedAeadAlgorithm(hello.selected.crypto.algorithm) ||
            hello.selected.crypto.serverRandom?.size != CRYPTO_RANDOM_SIZE
        ) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_HELLO)
        }
    }

    private fun <T> decodeHello(
        serializer: DeserializationStrategy<T>,
        payload: ByteArray,
        validate: (JsonObject) -> Unit,
    ): T {
        val text = try {
            payload.decodeToString(throwOnInvalidSequence = true)
        } catch (exception: CharacterCodingException) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_JSON)
        }
        return try {
            val objectValue = FrpJsonValidation.requireObject(helloJson.parseToJsonElement(text))
            validate(objectValue)
            helloJson.decodeFromJsonElement(serializer, objectValue)
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: FrpSafeIOException) {
            throw exception
        } catch (exception: SerializationException) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_JSON)
        } catch (exception: IllegalArgumentException) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_FIELD)
        } catch (exception: Exception) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_JSON)
        }
    }

    private fun isKnownFrameType(type: Int): Boolean =
        type == FRAME_TYPE_CLIENT_HELLO || type == FRAME_TYPE_SERVER_HELLO || type == FRAME_TYPE_MESSAGE

    private fun decodeUInt16(bytes: ByteArray, offset: Int): Int =
        ((bytes[offset].toInt() and 0xff) shl 8) or (bytes[offset + 1].toInt() and 0xff)

    private fun decodeUInt32(bytes: ByteArray, offset: Int): Long =
        ((bytes[offset].toLong() and 0xffL) shl 24) or
            ((bytes[offset + 1].toLong() and 0xffL) shl 16) or
            ((bytes[offset + 2].toLong() and 0xffL) shl 8) or
            (bytes[offset + 3].toLong() and 0xffL)

    private fun encodeUInt16(value: Int, destination: ByteArray, offset: Int) {
        if (value !in 0..0xffff) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_FIELD)
        }
        destination[offset] = (value ushr 8).toByte()
        destination[offset + 1] = value.toByte()
    }

    private fun encodeUInt32(value: Long, destination: ByteArray, offset: Int) {
        if (value !in 0..0xffff_ffffL) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_FIELD)
        }
        destination[offset] = (value ushr 24).toByte()
        destination[offset + 1] = (value ushr 16).toByte()
        destination[offset + 2] = (value ushr 8).toByte()
        destination[offset + 3] = value.toByte()
    }

    private const val FRAME_HEADER_SIZE = 8
    private const val MESSAGE_TYPE_SIZE = 2
    private val MAGIC_BYTES = byteArrayOf('F'.code.toByte(), 'R'.code.toByte(), 'P'.code.toByte(), 0, 2, '\r'.code.toByte(), '\n'.code.toByte())
    private val helloJson = Json {
        ignoreUnknownKeys = true
        isLenient = false
        coerceInputValues = true
        explicitNulls = false
        encodeDefaults = false
    }
}
