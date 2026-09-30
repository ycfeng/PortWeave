package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.compression

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.FrpContractFixtures
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.PlannedChunkInputStream
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpV1Cfb
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpV1CfbOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.util.zip.CRC32C
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class FrpSnappyFramedTest {
    @Test
    fun emptyStreamRemainsEmptyAndReadable() {
        val encoded = ByteArrayOutputStream().also { output ->
            FrpSnappyFramedOutputStream(output).close()
        }.toByteArray()

        assertArrayEquals(ByteArray(0), encoded)
        assertEquals(-1, FrpSnappyFramedInputStream(ByteArrayInputStream(encoded)).use { it.read() })
    }

    @Test
    fun writerRoundTripsChunkedCompressibleAndLiteralPayloads() {
        val payload = ByteArray(65_537 + 4_096) { index ->
            when {
                index < 65_537 -> "visitor-payload"[index % 15].code.toByte()
                else -> (index and 0xff).toByte()
            }
        }
        val encoded = ByteArrayOutputStream().also { output ->
            FrpSnappyFramedOutputStream(output).use { framed ->
                framed.write(payload, 0, 31_337)
                framed.write(payload, 31_337, payload.size - 31_337)
            }
        }.toByteArray()

        assertTrue(encoded.copyOfRange(0, STREAM_IDENTIFIER.size).contentEquals(STREAM_IDENTIFIER))
        assertArrayEquals(payload, decode(encoded))
    }

    @Test
    fun writerUsesTheGoCompressionThresholdAndSplitsAtSixtyFourKiB() {
        val compressible = ByteArray(65_537) { 'a'.code.toByte() }
        val compressed = encode(compressible)
        val compressedTypes = chunkTypes(compressed)
        assertEquals(listOf(0x00, 0x01), compressedTypes)

        val literal = ByteArray(256) { it.toByte() }
        val uncompressed = encode(literal)
        assertEquals(listOf(0x01), chunkTypes(uncompressed))
        assertArrayEquals(literal, decode(uncompressed))
    }

    @Test
    fun goOracleRawAndFramedVectorsAreBidirectionallyCompatible() {
        val compressedPayload = ByteArray(20) { 'a'.code.toByte() }
        val rawCompressed = FrpContractFixtures.bytes("snappy-raw-compressed")
        val framedCompressed = FrpContractFixtures.bytes("snappy-framed-compressed")

        assertArrayEquals(compressedPayload, decode(compressedFrame(rawCompressed, compressedPayload)))
        assertArrayEquals(rawCompressed, firstCompressedRawBlock(framedCompressed))
        assertArrayEquals(compressedPayload, decode(framedCompressed))
        assertArrayEquals(framedCompressed, encode(compressedPayload))

        val uncompressedPayload = ByteArray(256) { index -> index.toByte() }
        val framedUncompressed = FrpContractFixtures.bytes("snappy-framed-uncompressed")
        assertEquals(listOf(0x01), chunkTypes(framedUncompressed))
        assertArrayEquals(uncompressedPayload, decode(framedUncompressed))
        assertArrayEquals(framedUncompressed, encode(uncompressedPayload))
    }

    @Test
    fun goOracleBoundaryVectorMatchesSixtyFourKiBChunking() {
        val payload = ByteArray(65_537) { 'a'.code.toByte() }
        val framed = FrpContractFixtures.bytes("snappy-framed-boundary-65537")

        assertEquals(listOf(0x00, 0x01), chunkTypes(framed))
        assertArrayEquals(payload, decode(framed))
        assertArrayEquals(framed, encode(payload))
    }

    @Test
    fun goOracleEncryptionCompressionVectorUsesCfbOutsideSnappy() {
        val payload = ByteArray(20) { 'a'.code.toByte() }
        val framed = FrpContractFixtures.bytes("snappy-framed-compressed")
        val encrypted = FrpContractFixtures.bytes("snappy-cfb-framed-compressed")
        val plan = FrpContractFixtures.chunkPlan("snappy-cfb-framed-splits")

        val decryptedFramed = FrpV1Cfb.decrypting(
            PlannedChunkInputStream(encrypted, plan.sizes),
            FrpContractFixtures.syntheticStcpSecret,
        ).use(::readBounded)
        assertArrayEquals(framed, decryptedFramed)

        val decoded = FrpSnappyFramedInputStream(
            FrpV1Cfb.decrypting(
                PlannedChunkInputStream(encrypted, plan.sizes),
                FrpContractFixtures.syntheticStcpSecret,
            ),
        ).use(::readBounded)
        assertArrayEquals(payload, decoded)

        val output = ByteArrayOutputStream()
        FrpSnappyFramedOutputStream(
            FrpV1CfbOutputStream(
                output,
                FrpContractFixtures.syntheticStcpSecret,
                fixedBytes(0x70, FrpV1Cfb.IV_SIZE),
            ),
        ).use { it.write(payload) }
        assertArrayEquals(encrypted, output.toByteArray())
    }

    @Test
    fun readerAcceptsEveryRawSnappyTagAndExtendedLiterals() {
        val copyOnePlain = "abcdabcd".encodeToByteArray()
        val copyOneRaw = byteArrayOf(
            8,
            0x0c,
            'a'.code.toByte(),
            'b'.code.toByte(),
            'c'.code.toByte(),
            'd'.code.toByte(),
            0x01,
            0x04,
        )
        assertArrayEquals(copyOnePlain, decode(compressedFrame(copyOneRaw, copyOnePlain)))

        val literalTwenty = "abcdefghijklmnopqrst".encodeToByteArray()
        val copyTwoPlain = literalTwenty + literalTwenty
        val copyTwoRaw = byteArrayOf(40, 0x4c) + literalTwenty + byteArrayOf(0x4e, 20, 0)
        assertArrayEquals(copyTwoPlain, decode(compressedFrame(copyTwoRaw, copyTwoPlain)))

        val copyFourPlain = copyOnePlain
        val copyFourRaw = byteArrayOf(
            8,
            0x0c,
            'a'.code.toByte(),
            'b'.code.toByte(),
            'c'.code.toByte(),
            'd'.code.toByte(),
            0x0f,
            0x04,
            0,
            0,
            0,
        )
        assertArrayEquals(copyFourPlain, decode(compressedFrame(copyFourRaw, copyFourPlain)))

        val extendedLiteral = ByteArray(60) { it.toByte() }
        val extendedRaw = byteArrayOf(60, 0xf0.toByte(), 59) + extendedLiteral
        assertArrayEquals(extendedLiteral, decode(compressedFrame(extendedRaw, extendedLiteral)))
    }

    @Test
    fun readerAcceptsRepeatedIdentifiersAndSkippableChunks() {
        val payload = "bounded-synthetic-payload".encodeToByteArray()
        val skippable = byteArrayOf(0x80.toByte(), 3, 0, 0, 1, 2, 3)
        val encoded = STREAM_IDENTIFIER + skippable + STREAM_IDENTIFIER + uncompressedFrame(payload, includeIdentifier = false)

        assertArrayEquals(payload, decode(encoded))
    }

    @Test
    fun readerRejectsMissingHeadersReservedChunksTruncationAndOversize() {
        assertFailure(
            FrpCompressionFailure.INVALID_STREAM,
            uncompressedFrame("payload".encodeToByteArray(), includeIdentifier = false),
        )
        assertFailure(
            FrpCompressionFailure.INVALID_STREAM,
            STREAM_IDENTIFIER + byteArrayOf(0x02, 0, 0, 0),
        )
        assertFailure(FrpCompressionFailure.TRUNCATED_HEADER, byteArrayOf(0xff.toByte(), 6, 0))
        assertFailure(
            FrpCompressionFailure.TRUNCATED_CHUNK,
            STREAM_IDENTIFIER + byteArrayOf(0x01, 8, 0, 0, 1, 2),
        )
        val oversize = STREAM_IDENTIFIER + byteArrayOf(0x01, 5, 0, 1)
        assertFailure(FrpCompressionFailure.LENGTH_LIMIT, oversize)
    }

    @Test
    fun readerRejectsChecksumMismatchAndInvalidRawCopyOffsets() {
        val payload = "checksum-payload".encodeToByteArray()
        val mutated = uncompressedFrame(payload).also { it[it.lastIndex] = (it.last().toInt() xor 1).toByte() }
        assertFailure(FrpCompressionFailure.CHECKSUM_MISMATCH, mutated)

        val invalidRaw = byteArrayOf(4, 0x0e, 0, 0)
        assertFailure(
            FrpCompressionFailure.INVALID_STREAM,
            compressedFrame(invalidRaw, ByteArray(4)),
        )
    }

    @Test
    fun structuralSummariesNeverContainPayloadData() {
        val marker = "unsafe-payload-marker"
        val output = FrpSnappyFramedOutputStream(ByteArrayOutputStream())
        output.write(marker.encodeToByteArray())
        val input = FrpSnappyFramedInputStream(ByteArrayInputStream(encode(marker.encodeToByteArray())))

        assertFalse(output.toString().contains(marker))
        assertFalse(input.toString().contains(marker))
        output.close()
        input.close()
    }

    private fun encode(payload: ByteArray): ByteArray = ByteArrayOutputStream().also { output ->
        FrpSnappyFramedOutputStream(output).use { it.write(payload) }
    }.toByteArray()

    private fun decode(encoded: ByteArray): ByteArray =
        FrpSnappyFramedInputStream(ByteArrayInputStream(encoded)).use(::readBounded)

    private fun readBounded(input: InputStream): ByteArray {
        val output = ByteArrayOutputStream()
        val buffer = ByteArray(257)
        var total = 0
        while (true) {
            val count = input.read(buffer)
            if (count < 0) break
            assertTrue(count > 0)
            total += count
            assertTrue(total <= 2 * 65_536)
            output.write(buffer, 0, count)
        }
        buffer.fill(0)
        return output.toByteArray()
    }

    private fun compressedFrame(raw: ByteArray, plain: ByteArray): ByteArray =
        STREAM_IDENTIFIER + dataFrame(0x00, raw, plain)

    private fun uncompressedFrame(payload: ByteArray, includeIdentifier: Boolean = true): ByteArray =
        (if (includeIdentifier) STREAM_IDENTIFIER else ByteArray(0)) + dataFrame(0x01, payload, payload)

    private fun dataFrame(type: Int, payload: ByteArray, plain: ByteArray): ByteArray {
        val chunkLength = 4 + payload.size
        val result = ByteArray(4 + chunkLength)
        result[0] = type.toByte()
        result[1] = chunkLength.toByte()
        result[2] = (chunkLength ushr 8).toByte()
        result[3] = (chunkLength ushr 16).toByte()
        val checksum = maskedCrc32c(plain)
        result[4] = checksum.toByte()
        result[5] = (checksum ushr 8).toByte()
        result[6] = (checksum ushr 16).toByte()
        result[7] = (checksum ushr 24).toByte()
        payload.copyInto(result, 8)
        return result
    }

    private fun chunkTypes(encoded: ByteArray): List<Int> {
        val types = mutableListOf<Int>()
        var position = STREAM_IDENTIFIER.size
        while (position < encoded.size) {
            val type = encoded[position].toInt() and 0xff
            val length = (encoded[position + 1].toInt() and 0xff) or
                ((encoded[position + 2].toInt() and 0xff) shl 8) or
                ((encoded[position + 3].toInt() and 0xff) shl 16)
            types += type
            position += 4 + length
        }
        assertEquals(encoded.size, position)
        return types
    }

    private fun firstCompressedRawBlock(encoded: ByteArray): ByteArray {
        val position = STREAM_IDENTIFIER.size
        assertTrue(encoded.size >= position + 8)
        assertEquals(0x00, encoded[position].toInt() and 0xff)
        val length = (encoded[position + 1].toInt() and 0xff) or
            ((encoded[position + 2].toInt() and 0xff) shl 8) or
            ((encoded[position + 3].toInt() and 0xff) shl 16)
        assertEquals(encoded.size, position + 4 + length)
        return encoded.copyOfRange(position + 8, position + 4 + length)
    }

    private fun fixedBytes(start: Int, count: Int): ByteArray =
        ByteArray(count) { index -> (start + index).toByte() }

    private fun assertFailure(expected: FrpCompressionFailure, encoded: ByteArray) {
        val failure = assertThrows(FrpCompressionException::class.java) {
            decode(encoded)
        }
        assertEquals(expected, failure.failure)
        assertFalse(failure.toString().contains(encoded.joinToString()))
    }

    private fun maskedCrc32c(payload: ByteArray): Int {
        val crc = CRC32C()
        crc.update(payload)
        return Integer.rotateRight(crc.value.toInt(), 15) + 0xa282ead8L.toInt()
    }

    private companion object {
        val STREAM_IDENTIFIER = byteArrayOf(
            0xff.toByte(),
            6,
            0,
            0,
            's'.code.toByte(),
            'N'.code.toByte(),
            'a'.code.toByte(),
            'P'.code.toByte(),
            'p'.code.toByte(),
            'Y'.code.toByte(),
        )
    }
}
