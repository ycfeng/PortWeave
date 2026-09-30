package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.compression

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpSafeIOException
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.CancellationException

internal enum class FrpCompressionFailure(val description: String) {
    INVALID_STREAM("invalid stream"),
    TRUNCATED_HEADER("truncated chunk header"),
    TRUNCATED_CHUNK("truncated chunk"),
    LENGTH_LIMIT("chunk length limit exceeded"),
    CHECKSUM_MISMATCH("chunk checksum mismatch"),
    IO_FAILURE("stream io failure"),
    WRITE_FAILED("stream write failure"),
}

internal class FrpCompressionException(
    val failure: FrpCompressionFailure,
) : FrpSafeIOException("frp compression failure: ${failure.description}")

internal class FrpSnappyFramedOutputStream(
    private val output: OutputStream,
) : OutputStream() {
    private var identifierWritten = false
    private var closed = false
    private var failed = false

    override fun write(value: Int) {
        val single = byteArrayOf(value.toByte())
        try {
            write(single, 0, 1)
        } finally {
            single.fill(0)
        }
    }

    override fun write(source: ByteArray, offset: Int, length: Int) {
        requireRange(source.size, offset, length)
        checkOpen(FrpCompressionFailure.WRITE_FAILED)
        if (length == 0) return
        try {
            writeIdentifierIfNeeded()
            var position = offset
            val end = offset + length
            while (position < end) {
                val count = minOf(MAXIMUM_UNCOMPRESSED_CHUNK_SIZE, end - position)
                writeChunk(source, position, count)
                position += count
            }
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: FrpSafeIOException) {
            failed = true
            throw failure
        } catch (_: Exception) {
            failed = true
            throw FrpCompressionException(FrpCompressionFailure.WRITE_FAILED)
        }
    }

    override fun flush() {
        checkOpen(FrpCompressionFailure.WRITE_FAILED)
        try {
            output.flush()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: FrpSafeIOException) {
            failed = true
            throw failure
        } catch (_: Exception) {
            failed = true
            throw FrpCompressionException(FrpCompressionFailure.WRITE_FAILED)
        }
    }

    override fun close() {
        if (closed) return
        closed = true
        try {
            output.close()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: FrpSafeIOException) {
            throw failure
        } catch (_: Exception) {
            throw FrpCompressionException(FrpCompressionFailure.WRITE_FAILED)
        }
    }

    override fun toString(): String =
        "FrpSnappyFramedOutputStream(identifierWritten=$identifierWritten, closed=$closed, failed=$failed)"

    private fun writeIdentifierIfNeeded() {
        if (identifierWritten) return
        output.write(STREAM_IDENTIFIER)
        identifierWritten = true
    }

    private fun writeChunk(source: ByteArray, offset: Int, length: Int) {
        val encoded = FrpRawSnappy.encode(source, offset, length)
        try {
            val compressionThreshold = length - length / 8
            if (encoded.size < compressionThreshold) {
                writeDataChunk(COMPRESSED_DATA_CHUNK, source, offset, length, encoded, 0, encoded.size)
            } else {
                writeDataChunk(UNCOMPRESSED_DATA_CHUNK, source, offset, length, source, offset, length)
            }
        } finally {
            encoded.fill(0)
        }
    }

    private fun writeDataChunk(
        type: Int,
        plain: ByteArray,
        plainOffset: Int,
        plainLength: Int,
        payload: ByteArray,
        payloadOffset: Int,
        payloadLength: Int,
    ) {
        val chunkLength = CHECKSUM_SIZE + payloadLength
        if (chunkLength > MAXIMUM_FRAME_CHUNK_LENGTH) {
            throw FrpCompressionException(FrpCompressionFailure.LENGTH_LIMIT)
        }
        val header = ByteArray(FRAME_HEADER_SIZE + CHECKSUM_SIZE)
        try {
            header[0] = type.toByte()
            writeLittleEndian24(header, 1, chunkLength)
            writeLittleEndian32(header, FRAME_HEADER_SIZE, maskedCrc32c(plain, plainOffset, plainLength))
            output.write(header)
            output.write(payload, payloadOffset, payloadLength)
        } finally {
            header.fill(0)
        }
    }

    private fun checkOpen(failure: FrpCompressionFailure) {
        if (closed || failed) throw FrpCompressionException(failure)
    }
}

internal class FrpSnappyFramedInputStream(
    private val input: InputStream,
) : InputStream() {
    private var pending = ByteArray(0)
    private var pendingOffset = 0
    private var identifierSeen = false
    private var eof = false
    private var closed = false
    private var failed = false

    override fun read(): Int {
        val single = ByteArray(1)
        return try {
            val count = read(single, 0, 1)
            if (count < 0) -1 else single[0].toInt() and 0xff
        } finally {
            single.fill(0)
        }
    }

    override fun read(destination: ByteArray, offset: Int, length: Int): Int {
        requireRange(destination.size, offset, length)
        checkOpen(FrpCompressionFailure.IO_FAILURE)
        if (length == 0) return 0
        try {
            while (pendingOffset >= pending.size) {
                clearPending()
                if (eof || !readNextDataChunk()) return -1
            }
            val count = minOf(length, pending.size - pendingOffset)
            pending.copyInto(
                destination,
                destinationOffset = offset,
                startIndex = pendingOffset,
                endIndex = pendingOffset + count,
            )
            pendingOffset += count
            return count
        } catch (failure: CancellationException) {
            clearPending()
            throw failure
        } catch (failure: Error) {
            clearPending()
            throw failure
        } catch (failure: FrpSafeIOException) {
            failed = true
            clearPending()
            throw failure
        } catch (_: Exception) {
            failed = true
            clearPending()
            throw FrpCompressionException(FrpCompressionFailure.IO_FAILURE)
        }
    }

    override fun close() {
        if (closed) return
        closed = true
        clearPending()
        try {
            input.close()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: FrpSafeIOException) {
            throw failure
        } catch (_: Exception) {
            throw FrpCompressionException(FrpCompressionFailure.IO_FAILURE)
        }
    }

    override fun toString(): String =
        "FrpSnappyFramedInputStream(identifierSeen=$identifierSeen, closed=$closed, failed=$failed)"

    private fun readNextDataChunk(): Boolean {
        while (true) {
            val type = readByteOrEof()
            if (type < 0) {
                eof = true
                return false
            }
            val header = ByteArray(3)
            try {
                readFully(header, 0, header.size, FrpCompressionFailure.TRUNCATED_HEADER)
                val chunkLength = readLittleEndian24(header, 0)
                when (type) {
                    STREAM_IDENTIFIER_CHUNK -> readIdentifier(chunkLength)
                    COMPRESSED_DATA_CHUNK -> {
                        requireIdentifier()
                        retainDataChunk(compressed = true, chunkLength = chunkLength)
                        if (pending.isNotEmpty()) return true
                    }
                    UNCOMPRESSED_DATA_CHUNK -> {
                        requireIdentifier()
                        retainDataChunk(compressed = false, chunkLength = chunkLength)
                        if (pending.isNotEmpty()) return true
                    }
                    in SKIPPABLE_CHUNK_START..SKIPPABLE_CHUNK_END -> {
                        requireIdentifier()
                        skipFully(chunkLength)
                    }
                    else -> throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
                }
            } finally {
                header.fill(0)
            }
        }
    }

    private fun readIdentifier(chunkLength: Int) {
        if (chunkLength != STREAM_IDENTIFIER_BODY.size) {
            throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
        }
        val body = ByteArray(chunkLength)
        try {
            readFully(body, 0, body.size, FrpCompressionFailure.TRUNCATED_CHUNK)
            if (!body.contentEquals(STREAM_IDENTIFIER_BODY)) {
                throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
            }
            identifierSeen = true
        } finally {
            body.fill(0)
        }
    }

    private fun retainDataChunk(compressed: Boolean, chunkLength: Int) {
        val maximum = CHECKSUM_SIZE + if (compressed) {
            MAXIMUM_ENCODED_BLOCK_SIZE
        } else {
            MAXIMUM_UNCOMPRESSED_CHUNK_SIZE
        }
        if (chunkLength < CHECKSUM_SIZE || chunkLength > maximum) {
            throw FrpCompressionException(FrpCompressionFailure.LENGTH_LIMIT)
        }
        val body = ByteArray(chunkLength)
        var decoded: ByteArray? = null
        try {
            readFully(body, 0, body.size, FrpCompressionFailure.TRUNCATED_CHUNK)
            val expectedChecksum = readLittleEndian32(body, 0)
            decoded = if (compressed) {
                FrpRawSnappy.decode(body, CHECKSUM_SIZE, body.size - CHECKSUM_SIZE)
            } else {
                body.copyOfRange(CHECKSUM_SIZE, body.size)
            }
            if (maskedCrc32c(decoded, 0, decoded.size) != expectedChecksum) {
                throw FrpCompressionException(FrpCompressionFailure.CHECKSUM_MISMATCH)
            }
            pending = decoded
            pendingOffset = 0
            decoded = null
        } finally {
            body.fill(0)
            decoded?.fill(0)
        }
    }

    private fun requireIdentifier() {
        if (!identifierSeen) throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
    }

    private fun readByteOrEof(): Int = try {
        input.read()
    } catch (failure: CancellationException) {
        throw failure
    } catch (failure: Error) {
        throw failure
    } catch (failure: FrpSafeIOException) {
        throw failure
    } catch (_: Exception) {
        throw FrpCompressionException(FrpCompressionFailure.IO_FAILURE)
    }

    private fun readFully(
        destination: ByteArray,
        offset: Int,
        length: Int,
        truncatedFailure: FrpCompressionFailure,
    ) {
        var position = offset
        val end = offset + length
        while (position < end) {
            val count = try {
                input.read(destination, position, end - position)
            } catch (failure: CancellationException) {
                throw failure
            } catch (failure: Error) {
                throw failure
            } catch (failure: FrpSafeIOException) {
                throw failure
            } catch (_: Exception) {
                throw FrpCompressionException(FrpCompressionFailure.IO_FAILURE)
            }
            if (count < 0) throw FrpCompressionException(truncatedFailure)
            if (count == 0) {
                val single = readByteOrEof()
                if (single < 0) throw FrpCompressionException(truncatedFailure)
                destination[position++] = single.toByte()
            } else {
                position += count
            }
        }
    }

    private fun skipFully(length: Int) {
        var remaining = length
        val buffer = ByteArray(SKIP_BUFFER_SIZE)
        try {
            while (remaining > 0) {
                val count = minOf(remaining, buffer.size)
                readFully(buffer, 0, count, FrpCompressionFailure.TRUNCATED_CHUNK)
                buffer.fill(0, 0, count)
                remaining -= count
            }
        } finally {
            buffer.fill(0)
        }
    }

    private fun clearPending() {
        pending.fill(0)
        pending = ByteArray(0)
        pendingOffset = 0
    }

    private fun checkOpen(failure: FrpCompressionFailure) {
        if (closed || failed) throw FrpCompressionException(failure)
    }

    private companion object {
        const val SKIP_BUFFER_SIZE = 8 * 1_024
    }
}

private object FrpRawSnappy {
    fun encode(source: ByteArray, offset: Int, length: Int): ByteArray {
        if (length > MAXIMUM_UNCOMPRESSED_CHUNK_SIZE) {
            throw FrpCompressionException(FrpCompressionFailure.LENGTH_LIMIT)
        }
        val accumulator = ByteAccumulator(MAXIMUM_ENCODED_BLOCK_SIZE)
        val table = IntArray(HASH_TABLE_SIZE)
        try {
            accumulator.writeVarint(length)
            var nextLiteral = 0
            var position = 0
            while (position + MINIMUM_COPY_LENGTH <= length) {
                val value = loadLittleEndian32(source, offset + position)
                val hash = (value * HASH_MULTIPLIER).ushr(32 - HASH_BITS)
                val candidate = table[hash] - 1
                table[hash] = position + 1
                if (candidate >= 0 &&
                    position - candidate <= MAXIMUM_COPY_OFFSET &&
                    equalsFourBytes(source, offset + candidate, offset + position)
                ) {
                    accumulator.writeLiteral(source, offset + nextLiteral, position - nextLiteral)
                    var matchLength = MINIMUM_COPY_LENGTH
                    while (position + matchLength < length &&
                        source[offset + candidate + matchLength] == source[offset + position + matchLength]
                    ) {
                        matchLength += 1
                    }
                    accumulator.writeCopy(position - candidate, matchLength)
                    position += matchLength
                    nextLiteral = position
                } else {
                    position += 1
                }
            }
            accumulator.writeLiteral(source, offset + nextLiteral, length - nextLiteral)
            return accumulator.toByteArray()
        } finally {
            accumulator.clear()
            table.fill(0)
        }
    }

    fun decode(source: ByteArray, offset: Int, length: Int): ByteArray {
        var position = offset
        val end = offset + length
        val decodedLengthResult = readVarint(source, position, end)
        val decodedLength = decodedLengthResult.first
        position = decodedLengthResult.second
        if (decodedLength > MAXIMUM_UNCOMPRESSED_CHUNK_SIZE) {
            throw FrpCompressionException(FrpCompressionFailure.LENGTH_LIMIT)
        }
        val output = ByteArray(decodedLength)
        var outputPosition = 0
        try {
            while (outputPosition < output.size) {
                if (position >= end) throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
                val tag = source[position++].toInt() and 0xff
                when (tag and TAG_TYPE_MASK) {
                    LITERAL_TAG -> {
                        var literalLength = tag ushr 2
                        if (literalLength < SHORT_LITERAL_LIMIT) {
                            literalLength += 1
                        } else {
                            val extraBytes = literalLength - SHORT_LITERAL_LIMIT + 1
                            if (extraBytes !in 1..4 || position > end - extraBytes) {
                                throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
                            }
                            var encodedLength = 0L
                            repeat(extraBytes) { index ->
                                encodedLength = encodedLength or
                                    ((source[position + index].toLong() and 0xffL) shl (index * 8))
                            }
                            position += extraBytes
                            val expanded = encodedLength + 1L
                            if (expanded > Int.MAX_VALUE) {
                                throw FrpCompressionException(FrpCompressionFailure.LENGTH_LIMIT)
                            }
                            literalLength = expanded.toInt()
                        }
                        if (literalLength > output.size - outputPosition || position > end - literalLength) {
                            throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
                        }
                        source.copyInto(
                            output,
                            destinationOffset = outputPosition,
                            startIndex = position,
                            endIndex = position + literalLength,
                        )
                        position += literalLength
                        outputPosition += literalLength
                    }
                    COPY_ONE_BYTE_TAG -> {
                        if (position >= end) throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
                        val copyLength = 4 + ((tag ushr 2) and 0x7)
                        val copyOffset = ((tag and 0xe0) shl 3) or (source[position++].toInt() and 0xff)
                        outputPosition = copy(output, outputPosition, copyOffset, copyLength)
                    }
                    COPY_TWO_BYTE_TAG -> {
                        if (position > end - 2) throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
                        val copyLength = 1 + (tag ushr 2)
                        val copyOffset = readLittleEndian16(source, position)
                        position += 2
                        outputPosition = copy(output, outputPosition, copyOffset, copyLength)
                    }
                    COPY_FOUR_BYTE_TAG -> {
                        if (position > end - 4) throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
                        val copyLength = 1 + (tag ushr 2)
                        val copyOffset = readUnsignedLittleEndian32(source, position)
                        position += 4
                        if (copyOffset > Int.MAX_VALUE) {
                            throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
                        }
                        outputPosition = copy(output, outputPosition, copyOffset.toInt(), copyLength)
                    }
                }
            }
            if (position != end) throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
            return output
        } catch (failure: Throwable) {
            output.fill(0)
            throw failure
        }
    }

    private fun readVarint(source: ByteArray, offset: Int, end: Int): Pair<Int, Int> {
        var value = 0L
        var shift = 0
        var position = offset
        repeat(MAXIMUM_VARINT_BYTES) {
            if (position >= end) throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
            val current = source[position++].toInt() and 0xff
            value = value or ((current and 0x7f).toLong() shl shift)
            if (current and 0x80 == 0) {
                if (value > Int.MAX_VALUE) throw FrpCompressionException(FrpCompressionFailure.LENGTH_LIMIT)
                return value.toInt() to position
            }
            shift += 7
        }
        throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
    }

    private fun copy(output: ByteArray, position: Int, offset: Int, length: Int): Int {
        if (offset <= 0 || offset > position || length > output.size - position) {
            throw FrpCompressionException(FrpCompressionFailure.INVALID_STREAM)
        }
        repeat(length) { index ->
            output[position + index] = output[position - offset + index]
        }
        return position + length
    }

    private fun equalsFourBytes(source: ByteArray, first: Int, second: Int): Boolean =
        source[first] == source[second] &&
            source[first + 1] == source[second + 1] &&
            source[first + 2] == source[second + 2] &&
            source[first + 3] == source[second + 3]

    private const val HASH_BITS = 14
    private const val HASH_TABLE_SIZE = 1 shl HASH_BITS
    private const val HASH_MULTIPLIER = 0x1e35a7bd
    private const val MINIMUM_COPY_LENGTH = 4
    private const val MAXIMUM_COPY_OFFSET = 0xffff
    private const val MAXIMUM_VARINT_BYTES = 5
    private const val TAG_TYPE_MASK = 0x03
    private const val LITERAL_TAG = 0x00
    private const val COPY_ONE_BYTE_TAG = 0x01
    private const val COPY_TWO_BYTE_TAG = 0x02
    private const val COPY_FOUR_BYTE_TAG = 0x03
    private const val SHORT_LITERAL_LIMIT = 60
}

private class ByteAccumulator(
    capacity: Int,
) {
    private val buffer = ByteArray(capacity)
    private var size = 0

    fun writeVarint(value: Int) {
        var remaining = value
        while (remaining >= 0x80) {
            writeByte((remaining and 0x7f) or 0x80)
            remaining = remaining ushr 7
        }
        writeByte(remaining)
    }

    fun writeLiteral(source: ByteArray, offset: Int, length: Int) {
        if (length == 0) return
        val encodedLength = length - 1
        if (length < 60) {
            writeByte(encodedLength shl 2)
        } else {
            val extraBytes = when {
                encodedLength <= 0xff -> 1
                encodedLength <= 0xffff -> 2
                encodedLength <= 0xffffff -> 3
                else -> 4
            }
            writeByte((59 + extraBytes) shl 2)
            repeat(extraBytes) { index -> writeByte(encodedLength ushr (index * 8)) }
        }
        writeBytes(source, offset, length)
    }

    fun writeCopy(offset: Int, length: Int) {
        var remaining = length
        while (remaining > 0) {
            val count = minOf(remaining, MAXIMUM_COPY_LENGTH)
            writeByte(((count - 1) shl 2) or COPY_TWO_BYTE_TAG)
            writeByte(offset)
            writeByte(offset ushr 8)
            remaining -= count
        }
    }

    fun toByteArray(): ByteArray = buffer.copyOf(size)

    fun clear() {
        buffer.fill(0)
        size = 0
    }

    private fun writeByte(value: Int) {
        if (size >= buffer.size) throw FrpCompressionException(FrpCompressionFailure.LENGTH_LIMIT)
        buffer[size++] = value.toByte()
    }

    private fun writeBytes(source: ByteArray, offset: Int, length: Int) {
        if (length > buffer.size - size) throw FrpCompressionException(FrpCompressionFailure.LENGTH_LIMIT)
        source.copyInto(buffer, size, offset, offset + length)
        size += length
    }

    private companion object {
        const val MAXIMUM_COPY_LENGTH = 64
        const val COPY_TWO_BYTE_TAG = 0x02
    }
}

private fun requireRange(size: Int, offset: Int, length: Int) {
    if (offset < 0 || length < 0 || offset > size - length) throw IndexOutOfBoundsException()
}

private fun maskedCrc32c(source: ByteArray, offset: Int, length: Int): Int {
    var crc = -1
    repeat(length) { index ->
        crc = CRC32C_TABLE[(crc xor source[offset + index].toInt()) and 0xff] xor (crc ushr 8)
    }
    return Integer.rotateRight(crc.inv(), 15) + CRC_MASK_DELTA
}

private fun loadLittleEndian32(source: ByteArray, offset: Int): Int =
    (source[offset].toInt() and 0xff) or
        ((source[offset + 1].toInt() and 0xff) shl 8) or
        ((source[offset + 2].toInt() and 0xff) shl 16) or
        ((source[offset + 3].toInt() and 0xff) shl 24)

private fun readLittleEndian16(source: ByteArray, offset: Int): Int =
    (source[offset].toInt() and 0xff) or ((source[offset + 1].toInt() and 0xff) shl 8)

private fun readLittleEndian24(source: ByteArray, offset: Int): Int =
    (source[offset].toInt() and 0xff) or
        ((source[offset + 1].toInt() and 0xff) shl 8) or
        ((source[offset + 2].toInt() and 0xff) shl 16)

private fun readLittleEndian32(source: ByteArray, offset: Int): Int =
    loadLittleEndian32(source, offset)

private fun readUnsignedLittleEndian32(source: ByteArray, offset: Int): Long =
    (source[offset].toLong() and 0xffL) or
        ((source[offset + 1].toLong() and 0xffL) shl 8) or
        ((source[offset + 2].toLong() and 0xffL) shl 16) or
        ((source[offset + 3].toLong() and 0xffL) shl 24)

private fun writeLittleEndian24(destination: ByteArray, offset: Int, value: Int) {
    destination[offset] = value.toByte()
    destination[offset + 1] = (value ushr 8).toByte()
    destination[offset + 2] = (value ushr 16).toByte()
}

private fun writeLittleEndian32(destination: ByteArray, offset: Int, value: Int) {
    destination[offset] = value.toByte()
    destination[offset + 1] = (value ushr 8).toByte()
    destination[offset + 2] = (value ushr 16).toByte()
    destination[offset + 3] = (value ushr 24).toByte()
}

private const val COMPRESSED_DATA_CHUNK = 0x00
private const val UNCOMPRESSED_DATA_CHUNK = 0x01
private const val STREAM_IDENTIFIER_CHUNK = 0xff
private const val SKIPPABLE_CHUNK_START = 0x80
private const val SKIPPABLE_CHUNK_END = 0xfe
private const val FRAME_HEADER_SIZE = 4
private const val CHECKSUM_SIZE = 4
private const val MAXIMUM_UNCOMPRESSED_CHUNK_SIZE = 65_536
private const val MAXIMUM_ENCODED_BLOCK_SIZE = 76_490
private const val MAXIMUM_FRAME_CHUNK_LENGTH = 0x00ff_ffff
private const val CRC_MASK_DELTA = -1_568_478_504
private const val CRC32C_POLYNOMIAL = -2_097_792_136
private val CRC32C_TABLE = IntArray(256) { value ->
    var entry = value
    repeat(8) {
        entry = if (entry and 1 == 0) entry ushr 1 else (entry ushr 1) xor CRC32C_POLYNOMIAL
    }
    entry
}
private val STREAM_IDENTIFIER_BODY = byteArrayOf(0x73, 0x4e, 0x61, 0x50, 0x70, 0x59)
private val STREAM_IDENTIFIER = byteArrayOf(
    STREAM_IDENTIFIER_CHUNK.toByte(),
    STREAM_IDENTIFIER_BODY.size.toByte(),
    0,
    0,
    *STREAM_IDENTIFIER_BODY,
)
