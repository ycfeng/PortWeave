package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpMuxStream
import java.io.InputStream
import java.io.OutputStream

internal object FrpWireV1 {
    const val MAX_JSON_LENGTH = 10_240
    private const val HEADER_LENGTH = 9

    fun readMessage(input: InputStream): FrpMessage? {
        val type = readByteSafely(input)
        if (type < 0) {
            return null
        }
        if (!FrpMessageTypes.isKnownV1(type)) {
            throw FrpProtocolException(FrpProtocolFailure.UNKNOWN_TYPE)
        }

        val lengthBytes = ByteArray(Long.SIZE_BYTES)
        readFullySafely(
            input = input,
            destination = lengthBytes,
            truncatedFailure = FrpProtocolFailure.TRUNCATED_HEADER,
        )
        val length = decodeLongBigEndian(lengthBytes)
        if (length < 0) {
            throw FrpProtocolException(FrpProtocolFailure.NEGATIVE_LENGTH)
        }
        if (length > MAX_JSON_LENGTH.toLong()) {
            throw FrpProtocolException(FrpProtocolFailure.LENGTH_LIMIT)
        }

        val payload = ByteArray(length.toInt())
        readFullySafely(
            input = input,
            destination = payload,
            truncatedFailure = FrpProtocolFailure.TRUNCATED_BODY,
        )
        return FrpMessageJson.decodeV1(type, payload)
    }

    suspend fun readMessage(input: FrpMuxStream): FrpMessage? {
        val type = readByteSafely(input)
        if (type < 0) return null
        if (!FrpMessageTypes.isKnownV1(type)) {
            throw FrpProtocolException(FrpProtocolFailure.UNKNOWN_TYPE)
        }

        val lengthBytes = ByteArray(Long.SIZE_BYTES)
        var payload: ByteArray? = null
        try {
            readFullySafely(
                input = input,
                destination = lengthBytes,
                truncatedFailure = FrpProtocolFailure.TRUNCATED_HEADER,
            )
            val length = decodeLongBigEndian(lengthBytes)
            if (length < 0) throw FrpProtocolException(FrpProtocolFailure.NEGATIVE_LENGTH)
            if (length > MAX_JSON_LENGTH.toLong()) {
                throw FrpProtocolException(FrpProtocolFailure.LENGTH_LIMIT)
            }

            payload = ByteArray(length.toInt())
            readFullySafely(
                input = input,
                destination = payload,
                truncatedFailure = FrpProtocolFailure.TRUNCATED_BODY,
            )
            return FrpMessageJson.decodeV1(type, payload)
        } finally {
            lengthBytes.fill(0)
            payload?.fill(0)
        }
    }

    fun writeMessage(output: OutputStream, message: FrpMessage) {
        val payload = FrpMessageJson.encode(message, MAX_JSON_LENGTH)
        val header = ByteArray(HEADER_LENGTH)
        try {
            header[0] = FrpMessageTypes.v1TypeOf(message).toByte()
            encodeLongBigEndian(payload.size.toLong(), header, 1)
            writeSafely(output, header)
            writeSafely(output, payload)
        } finally {
            header.fill(0)
            payload.fill(0)
        }
    }

    fun encodeMessage(message: FrpMessage): ByteArray {
        val output = ClearingByteArrayOutputStream(HEADER_LENGTH + 128)
        return try {
            writeMessage(output, message)
            output.copyAndClear()
        } catch (failure: Throwable) {
            output.clear()
            throw failure
        }
    }

    private fun decodeLongBigEndian(bytes: ByteArray): Long {
        var value = 0L
        for (byte in bytes) {
            value = (value shl 8) or (byte.toLong() and 0xffL)
        }
        return value
    }

    private fun encodeLongBigEndian(value: Long, destination: ByteArray, offset: Int) {
        for (index in 0 until Long.SIZE_BYTES) {
            val shift = (Long.SIZE_BYTES - index - 1) * 8
            destination[offset + index] = (value ushr shift).toByte()
        }
    }
}
