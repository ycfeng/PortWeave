package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpSafeIOException
import kotlinx.coroutines.CancellationException

internal object YamuxProtocol {
    const val VERSION = 0
    const val HEADER_SIZE = 12
    const val INITIAL_STREAM_WINDOW = 256L * 1024L
    const val MAX_UINT32 = 0xffff_ffffL
}

internal enum class YamuxFrameType(val wireValue: Int) {
    DATA(0),
    WINDOW_UPDATE(1),
    PING(2),
    GOAWAY(3),
    ;

    companion object {
        fun fromWireValue(value: Int): YamuxFrameType? = entries.firstOrNull { it.wireValue == value }
    }
}

internal object YamuxFlags {
    const val SYN = 0x1
    const val ACK = 0x2
    const val FIN = 0x4
    const val RST = 0x8
    const val ALL = SYN or ACK or FIN or RST
}

internal enum class YamuxGoAwayCode(val wireValue: Long) {
    NORMAL(0),
    PROTOCOL_ERROR(1),
    INTERNAL_ERROR(2),
    ;

    companion object {
        fun fromWireValue(value: Long): YamuxGoAwayCode? = entries.firstOrNull { it.wireValue == value }
    }
}

internal data class YamuxFrameHeader(
    val type: YamuxFrameType,
    val flags: Int,
    val streamId: Long,
    val length: Long,
)

internal interface YamuxPhysicalTransport {
    /** Runs the single session reader in any transport-owned blocking context. */
    suspend fun runReaderLoop(block: suspend () -> Unit) = block()

    /** Returns 1..length or -1. Returning zero is a transport contract failure. */
    suspend fun read(destination: ByteArray, offset: Int, length: Int): Int

    suspend fun writeFully(source: ByteArray, offset: Int = 0, length: Int = source.size - offset)

    /** Implementations must make repeated calls safe. */
    fun close()
}

internal enum class YamuxProtocolError {
    INVALID_VERSION,
    UNKNOWN_TYPE,
    INVALID_FLAGS,
    INVALID_STREAM_ID,
    INVALID_SESSION_ID,
    INVALID_SESSION_FRAME,
    INVALID_STREAM_FRAME,
    INVALID_STREAM_PARITY,
    DUPLICATE_SYN,
    DUPLICATE_ACK,
    DUPLICATE_FIN,
    UNKNOWN_STREAM,
    STREAM_ID_REUSE,
    DATA_AFTER_FIN,
    DATA_FRAME_TOO_LARGE,
    RECEIVE_WINDOW_EXCEEDED,
    SEND_WINDOW_OVERFLOW,
    INVALID_GOAWAY_CODE,
}

internal enum class YamuxTransportOperation {
    READ,
    WRITE,
    READ_CONTRACT,
}

internal enum class YamuxFrameSection {
    HEADER,
    DATA,
}

internal enum class YamuxTimeoutKind {
    WRITE,
    STREAM_OPEN,
    STREAM_CLOSE,
    PING,
    KEEPALIVE,
}

internal enum class YamuxResourceKind {
    STREAMS,
    PENDING_OPENS,
    ACCEPT_BACKLOG,
    RECEIVE_BYTES,
    WRITER_FRAMES,
    WRITER_BYTES,
}

internal sealed class YamuxFailure(message: String) : FrpSafeIOException(message)

internal class YamuxProtocolFailure(
    val reason: YamuxProtocolError,
) : YamuxFailure("Yamux protocol violation: ${reason.name}")

internal class YamuxTransportFailure(
    val operation: YamuxTransportOperation,
) : YamuxFailure("Yamux transport failure: ${operation.name}")

internal class YamuxTruncatedFrameFailure(
    val section: YamuxFrameSection,
) : YamuxFailure("Yamux frame is truncated: ${section.name}")

internal class YamuxTimeoutFailure(
    val kind: YamuxTimeoutKind,
) : YamuxFailure("Yamux timeout: ${kind.name}")

internal class YamuxResourceLimitFailure(
    val resource: YamuxResourceKind,
) : YamuxFailure("Yamux resource limit reached: ${resource.name}")

internal class YamuxSessionClosedFailure : YamuxFailure("Yamux session is closed")

internal class YamuxRemoteGoAwayFailure(
    val code: YamuxGoAwayCode,
) : YamuxFailure("Remote Yamux session terminated: ${code.name}")

internal class YamuxLocalGoAwayFailure(
    val code: YamuxGoAwayCode,
) : YamuxFailure("Local Yamux session terminated: ${code.name}")

internal class YamuxStreamResetFailure : YamuxFailure("Yamux stream was reset")

internal class YamuxStreamClosedFailure : YamuxFailure("Yamux stream write side is closed")

internal class YamuxStreamIdExhaustedFailure : YamuxFailure("Yamux stream IDs are exhausted")

internal class YamuxReceiveBudgetFailure : YamuxFailure("Yamux receive-byte budget is exhausted")

internal data class YamuxPayloadSegments(
    val segments: List<ByteArray>,
    val length: Int,
) {
    override fun toString(): String =
        "YamuxPayloadSegments(segmentCount=${segments.size}, length=$length)"
}

internal object YamuxFrameCodec {
    internal const val MAX_PAYLOAD_SEGMENTS = 4_096

    fun encodeHeader(header: YamuxFrameHeader): ByteArray {
        validateHeader(header)
        return ByteArray(YamuxProtocol.HEADER_SIZE).also { output -> encodeHeaderInto(header, output) }
    }

    fun encodeFrame(
        header: YamuxFrameHeader,
        body: ByteArray?,
        bodyOffset: Int,
        bodyLength: Int,
    ): ByteArray {
        validateHeader(header)
        if (header.type == YamuxFrameType.DATA) {
            require(body != null || bodyLength == 0)
            requireRange(body?.size ?: 0, bodyOffset, bodyLength)
            require(header.length == bodyLength.toLong())
        } else {
            require(body == null)
            require(bodyOffset == 0 && bodyLength == 0)
        }
        return ByteArray(YamuxProtocol.HEADER_SIZE + bodyLength).also { output ->
            encodeHeaderInto(header, output)
            if (bodyLength > 0) {
                checkNotNull(body).copyInto(
                    destination = output,
                    destinationOffset = YamuxProtocol.HEADER_SIZE,
                    startIndex = bodyOffset,
                    endIndex = bodyOffset + bodyLength,
                )
            }
        }
    }

    fun decodeHeader(source: ByteArray, offset: Int = 0): YamuxFrameHeader {
        requireRange(source.size, offset, YamuxProtocol.HEADER_SIZE)
        val version = source[offset].toInt() and 0xff
        if (version != YamuxProtocol.VERSION) {
            throw YamuxProtocolFailure(YamuxProtocolError.INVALID_VERSION)
        }
        val type = YamuxFrameType.fromWireValue(source[offset + 1].toInt() and 0xff)
            ?: throw YamuxProtocolFailure(YamuxProtocolError.UNKNOWN_TYPE)
        val header = YamuxFrameHeader(
            type = type,
            flags = readUInt16(source, offset + 2),
            streamId = readUInt32(source, offset + 4),
            length = readUInt32(source, offset + 8),
        )
        validateHeader(header)
        return header
    }

    private fun encodeHeaderInto(header: YamuxFrameHeader, output: ByteArray) {
        output[0] = YamuxProtocol.VERSION.toByte()
        output[1] = header.type.wireValue.toByte()
        putUInt16(output, 2, header.flags)
        putUInt32(output, 4, header.streamId)
        putUInt32(output, 8, header.length)
    }

    suspend fun readHeaderOrNull(
        transport: YamuxPhysicalTransport,
        scratch: ByteArray,
    ): YamuxFrameHeader? {
        require(scratch.size >= YamuxProtocol.HEADER_SIZE)
        if (
            !readFully(
                transport = transport,
                destination = scratch,
                section = YamuxFrameSection.HEADER,
                allowInitialEof = true,
                length = YamuxProtocol.HEADER_SIZE,
            )
        ) {
            return null
        }
        return decodeHeader(scratch)
    }

    suspend fun readDataSegments(
        transport: YamuxPhysicalTransport,
        length: Long,
        maximumLength: Int,
        segmentSize: Int,
    ): YamuxPayloadSegments {
        if (length < 0L || length > maximumLength.toLong() || length > Int.MAX_VALUE.toLong()) {
            throw YamuxProtocolFailure(YamuxProtocolError.DATA_FRAME_TOO_LARGE)
        }
        require(segmentSize > 0)
        val intLength = length.toInt()
        if (intLength == 0) {
            return YamuxPayloadSegments(emptyList(), 0)
        }
        val boundedSegmentSize = maxOf(
            segmentSize,
            ((intLength - 1) / MAX_PAYLOAD_SEGMENTS) + 1,
        )
        val segmentCount = ((intLength - 1) / boundedSegmentSize) + 1
        val segments = ArrayList<ByteArray>(segmentCount)
        var remaining = intLength
        while (remaining > 0) {
            val segment = ByteArray(minOf(boundedSegmentSize, remaining))
            readFully(transport, segment, YamuxFrameSection.DATA, allowInitialEof = false)
            segments += segment
            remaining -= segment.size
        }
        return YamuxPayloadSegments(segments, intLength)
    }

    suspend fun discardData(
        transport: YamuxPhysicalTransport,
        length: Long,
        maximumLength: Int,
        segmentSize: Int,
    ) {
        if (length < 0L || length > maximumLength.toLong() || length > Int.MAX_VALUE.toLong()) {
            throw YamuxProtocolFailure(YamuxProtocolError.DATA_FRAME_TOO_LARGE)
        }
        require(segmentSize > 0)
        if (length == 0L) return
        val scratch = ByteArray(minOf(segmentSize, length.toInt()))
        var remaining = length.toInt()
        while (remaining > 0) {
            val count = minOf(scratch.size, remaining)
            readFully(
                transport = transport,
                destination = scratch,
                section = YamuxFrameSection.DATA,
                allowInitialEof = false,
                length = count,
            )
            remaining -= count
        }
    }

    fun validateHeader(header: YamuxFrameHeader) {
        if (header.flags !in 0..0xffff || header.flags and YamuxFlags.ALL != header.flags) {
            throw YamuxProtocolFailure(YamuxProtocolError.INVALID_FLAGS)
        }
        validateUInt32(header.streamId, YamuxProtocolError.INVALID_STREAM_ID)
        validateUInt32(header.length, YamuxProtocolError.INVALID_STREAM_FRAME)
        when (header.type) {
            YamuxFrameType.PING -> {
                if (header.streamId != 0L) {
                    throw YamuxProtocolFailure(YamuxProtocolError.INVALID_SESSION_ID)
                }
                if (header.flags != YamuxFlags.SYN && header.flags != YamuxFlags.ACK) {
                    throw YamuxProtocolFailure(YamuxProtocolError.INVALID_SESSION_FRAME)
                }
            }

            YamuxFrameType.GOAWAY -> {
                if (header.streamId != 0L) {
                    throw YamuxProtocolFailure(YamuxProtocolError.INVALID_SESSION_ID)
                }
                if (header.flags != 0) {
                    throw YamuxProtocolFailure(YamuxProtocolError.INVALID_SESSION_FRAME)
                }
                if (YamuxGoAwayCode.fromWireValue(header.length) == null) {
                    throw YamuxProtocolFailure(YamuxProtocolError.INVALID_GOAWAY_CODE)
                }
            }

            YamuxFrameType.DATA,
            YamuxFrameType.WINDOW_UPDATE,
            -> {
                if (header.streamId == 0L) {
                    throw YamuxProtocolFailure(YamuxProtocolError.INVALID_STREAM_ID)
                }
                val opensAndAcknowledges = header.flags and (YamuxFlags.SYN or YamuxFlags.ACK)
                if (opensAndAcknowledges == (YamuxFlags.SYN or YamuxFlags.ACK)) {
                    throw YamuxProtocolFailure(YamuxProtocolError.INVALID_FLAGS)
                }
            }
        }
    }

    private suspend fun readFully(
        transport: YamuxPhysicalTransport,
        destination: ByteArray,
        section: YamuxFrameSection,
        allowInitialEof: Boolean,
        length: Int = destination.size,
    ): Boolean {
        requireRange(destination.size, 0, length)
        var position = 0
        while (position < length) {
            val requested = length - position
            val count = try {
                transport.read(destination, position, requested)
            } catch (failure: CancellationException) {
                throw failure
            } catch (failure: Error) {
                throw failure
            } catch (failure: Exception) {
                throw if (failure is YamuxFailure) {
                    failure
                } else {
                    YamuxTransportFailure(YamuxTransportOperation.READ)
                }
            }
            when {
                count == -1 && position == 0 && allowInitialEof -> return false
                count == -1 -> throw YamuxTruncatedFrameFailure(section)
                count == 0 || count < -1 || count > requested ->
                    throw YamuxTransportFailure(YamuxTransportOperation.READ_CONTRACT)
                else -> position += count
            }
        }
        return true
    }

    private fun readUInt16(source: ByteArray, offset: Int): Int =
        ((source[offset].toInt() and 0xff) shl 8) or
            (source[offset + 1].toInt() and 0xff)

    private fun readUInt32(source: ByteArray, offset: Int): Long =
        ((source[offset].toLong() and 0xffL) shl 24) or
            ((source[offset + 1].toLong() and 0xffL) shl 16) or
            ((source[offset + 2].toLong() and 0xffL) shl 8) or
            (source[offset + 3].toLong() and 0xffL)

    private fun putUInt16(destination: ByteArray, offset: Int, value: Int) {
        destination[offset] = (value ushr 8).toByte()
        destination[offset + 1] = value.toByte()
    }

    private fun putUInt32(destination: ByteArray, offset: Int, value: Long) {
        destination[offset] = (value ushr 24).toByte()
        destination[offset + 1] = (value ushr 16).toByte()
        destination[offset + 2] = (value ushr 8).toByte()
        destination[offset + 3] = value.toByte()
    }

    private fun validateUInt32(value: Long, error: YamuxProtocolError) {
        if (value !in 0L..YamuxProtocol.MAX_UINT32) {
            throw YamuxProtocolFailure(error)
        }
    }

    private fun requireRange(size: Int, offset: Int, length: Int) {
        require(offset >= 0 && length >= 0 && offset <= size - length)
    }
}
