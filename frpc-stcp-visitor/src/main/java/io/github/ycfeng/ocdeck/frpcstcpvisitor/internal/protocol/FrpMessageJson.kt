@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.nio.charset.CharacterCodingException
import java.util.concurrent.CancellationException
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromJsonElement

internal object FrpMessageJson {
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = false
        coerceInputValues = true
        explicitNulls = false
        encodeDefaults = false
    }

    fun encode(message: FrpMessage, maximumBytes: Int): ByteArray =
        FrpJsonEncoding.encodeMessage(message, maximumBytes)

    fun decodeV1(type: Int, payload: ByteArray): FrpMessage = decode(type, payload, version = 1)

    fun decodeV2(type: Int, payload: ByteArray): FrpMessage = decode(type, payload, version = 2)

    private fun decode(type: Int, payload: ByteArray, version: Int): FrpMessage {
        val text = try {
            payload.decodeToString(throwOnInvalidSequence = true)
        } catch (exception: CharacterCodingException) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_JSON)
        }
        val message = try {
            val objectValue = FrpJsonValidation.requireObject(json.parseToJsonElement(text))
            FrpJsonValidation.validateMessage(type, objectValue)
            when {
                version == 1 && type == FrpMessageTypes.V1_LOGIN || version == 2 && type == FrpMessageTypes.V2_LOGIN ->
                    json.decodeFromJsonElement(Login.serializer(), objectValue)
                version == 1 && type == FrpMessageTypes.V1_LOGIN_RESP || version == 2 && type == FrpMessageTypes.V2_LOGIN_RESP ->
                    json.decodeFromJsonElement(LoginResp.serializer(), objectValue)
                version == 1 && type == FrpMessageTypes.V1_NEW_WORK_CONN || version == 2 && type == FrpMessageTypes.V2_NEW_WORK_CONN ->
                    json.decodeFromJsonElement(NewWorkConn.serializer(), objectValue)
                version == 1 && type == FrpMessageTypes.V1_REQ_WORK_CONN || version == 2 && type == FrpMessageTypes.V2_REQ_WORK_CONN ->
                    json.decodeFromJsonElement(ReqWorkConn.serializer(), objectValue)
                version == 1 && type == FrpMessageTypes.V1_START_WORK_CONN || version == 2 && type == FrpMessageTypes.V2_START_WORK_CONN ->
                    json.decodeFromJsonElement(StartWorkConn.serializer(), objectValue).also(::validatePorts)
                version == 1 && type == FrpMessageTypes.V1_NEW_VISITOR_CONN || version == 2 && type == FrpMessageTypes.V2_NEW_VISITOR_CONN ->
                    json.decodeFromJsonElement(NewVisitorConn.serializer(), objectValue)
                version == 1 && type == FrpMessageTypes.V1_NEW_VISITOR_CONN_RESP || version == 2 && type == FrpMessageTypes.V2_NEW_VISITOR_CONN_RESP ->
                    json.decodeFromJsonElement(NewVisitorConnResp.serializer(), objectValue)
                version == 1 && type == FrpMessageTypes.V1_PING || version == 2 && type == FrpMessageTypes.V2_PING ->
                    json.decodeFromJsonElement(Ping.serializer(), objectValue)
                version == 1 && type == FrpMessageTypes.V1_PONG || version == 2 && type == FrpMessageTypes.V2_PONG ->
                    json.decodeFromJsonElement(Pong.serializer(), objectValue)
                else -> throw FrpProtocolException(FrpProtocolFailure.UNKNOWN_TYPE)
            }
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
        return message
    }

    private fun validatePorts(message: StartWorkConn) {
        if (message.srcPort !in 0..65_535 || message.dstPort !in 0..65_535) {
            throw FrpProtocolException(FrpProtocolFailure.INVALID_FIELD)
        }
    }
}

internal class BoundedByteArrayOutput(
    private val maximumBytes: Int,
) : OutputStream() {
    private val delegate = ByteArrayOutputStream(minOf(maximumBytes, DEFAULT_INITIAL_CAPACITY))
    private var count = 0

    init {
        if (maximumBytes < 0) {
            throw FrpProtocolException(FrpProtocolFailure.LENGTH_LIMIT)
        }
    }

    override fun write(value: Int) {
        requireCapacity(1)
        delegate.write(value)
        count++
    }

    override fun write(bytes: ByteArray, offset: Int, length: Int) {
        if (offset < 0 || length < 0 || offset > bytes.size - length) {
            throw IndexOutOfBoundsException()
        }
        requireCapacity(length)
        delegate.write(bytes, offset, length)
        count += length
    }

    fun toByteArray(): ByteArray = delegate.toByteArray()

    private fun requireCapacity(additional: Int) {
        if (additional > maximumBytes - count) {
            throw FrpProtocolException(FrpProtocolFailure.LENGTH_LIMIT)
        }
    }

    private companion object {
        const val DEFAULT_INITIAL_CAPACITY = 1_024
    }
}
