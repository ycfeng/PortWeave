package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.FrpContractFixtures
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.PlannedChunkInputStream
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpMuxStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.CancellationException
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class FrpWireProtocolContractTest {
    @Test
    fun manifestLimitsMatchTheWireImplementations() {
        assertEquals(FrpContractFixtures.manifest.limits.wireV1JsonLength, FrpWireV1.MAX_JSON_LENGTH)
        assertEquals(FrpContractFixtures.manifest.limits.wireV2FramePayload, FrpWireV2.MAX_FRAME_PAYLOAD)
    }

    @Test
    fun v1GoldenMessagesDecodeAndEncodeByteForByte() {
        listOf(
            "wire-v1-login-response",
            "wire-v1-login-token",
            "wire-v1-new-visitor-conn-response",
            "wire-v1-new-visitor-stcp",
            "wire-v1-start-work-conn",
        ).forEach { entryId ->
            val source = FrpContractFixtures.bytes(entryId)
            val input = ByteArrayInputStream(source)
            val message = FrpWireV1.readMessage(input) ?: throw AssertionError("Missing v1 message: $entryId")

            assertFixtureMessage(entryId, message)
            assertNull(FrpWireV1.readMessage(input))
            assertArrayEquals(entryId, source, FrpWireV1.encodeMessage(message))
        }
    }

    @Test
    fun v2GoldenMessagesDecodeAndEncodeByteForByte() {
        listOf(
            "wire-v2-new-visitor-conn-response",
            "wire-v2-start-work-conn",
        ).forEach { entryId ->
            val source = FrpContractFixtures.bytes(entryId)
            val input = ByteArrayInputStream(source)
            val message = FrpWireV2.readMessage(input) ?: throw AssertionError("Missing v2 message: $entryId")

            assertFixtureMessage(entryId, message)
            assertNull(FrpWireV2.readMessage(input))
            assertArrayEquals(entryId, source, FrpWireV2.encodeMessage(message))
        }

        val visitorSource = FrpContractFixtures.bytes("wire-v2-new-visitor-stcp")
        val visitorInput = ByteArrayInputStream(visitorSource)
        FrpWireV2.readMagic(visitorInput)
        val visitor = FrpWireV2.readMessage(visitorInput)
            ?: throw AssertionError("Missing v2 visitor message")
        assertFixtureMessage("wire-v2-new-visitor-stcp", visitor)
        assertNull(FrpWireV2.readMessage(visitorInput))
        val visitorOutput = ByteArrayOutputStream()
        FrpWireV2.writeMagic(visitorOutput)
        FrpWireV2.writeMessage(visitorOutput, visitor)
        assertArrayEquals(visitorSource, visitorOutput.toByteArray())
    }

    @Test
    fun v2BootstrapGoldensPreserveHelloTranscriptsAndMessages() {
        val profiles = listOf(
            Profile(
                name = "aes-256-gcm",
                algorithm = FRP_AEAD_AES_256_GCM,
                algorithms = listOf(FRP_AEAD_AES_256_GCM, FRP_AEAD_XCHACHA20_POLY1305),
                clientRandomStart = 0x00,
                serverRandomStart = 0x20,
            ),
            Profile(
                name = "xchacha20-poly1305",
                algorithm = FRP_AEAD_XCHACHA20_POLY1305,
                algorithms = listOf(FRP_AEAD_XCHACHA20_POLY1305, FRP_AEAD_AES_256_GCM),
                clientRandomStart = 0x40,
                serverRandomStart = 0x60,
            ),
        )

        profiles.forEach { profile ->
            val clientSource = FrpContractFixtures.bytes("wire-v2-${profile.name}-client-bootstrap")
            val clientInput = ByteArrayInputStream(clientSource)
            FrpWireV2.readMagic(clientInput)
            val clientHello = FrpWireV2.readClientHello(clientInput)
            val login = FrpWireV2.readMessage(clientInput) as? Login
                ?: throw AssertionError("Missing bootstrap login for ${profile.name}")
            assertNull(FrpWireV2.readMessage(clientInput))
            assertEquals(BootstrapInfo(transport = "tcp", tls = true, tcpMux = true), clientHello.value.bootstrap)
            assertEquals(listOf(FrpWireV2.MESSAGE_CODEC_JSON), clientHello.value.capabilities.message.codecs)
            assertEquals(profile.algorithms, clientHello.value.capabilities.crypto.algorithms)
            assertArrayEquals(
                fixedBytes(profile.clientRandomStart, FrpWireV2.CRYPTO_RANDOM_SIZE),
                clientHello.value.capabilities.crypto.clientRandom,
            )
            assertFixtureMessage("wire-v1-login-token", login)

            val clientOutput = ByteArrayOutputStream()
            FrpWireV2.writeMagic(clientOutput)
            FrpWireV2.writeClientHello(clientOutput, clientHello.value)
            FrpWireV2.writeMessage(clientOutput, login)
            assertArrayEquals("client bootstrap ${profile.name}", clientSource, clientOutput.toByteArray())

            val serverSource = FrpContractFixtures.bytes("wire-v2-${profile.name}-server-bootstrap")
            val serverInput = ByteArrayInputStream(serverSource)
            val serverHello = FrpWireV2.readServerHello(serverInput)
            val loginResponse = FrpWireV2.readMessage(serverInput) as? LoginResp
                ?: throw AssertionError("Missing bootstrap login response for ${profile.name}")
            assertNull(FrpWireV2.readMessage(serverInput))
            assertEquals(FrpWireV2.MESSAGE_CODEC_JSON, serverHello.value.selected.message.codec)
            assertEquals(profile.algorithm, serverHello.value.selected.crypto.algorithm)
            assertArrayEquals(
                fixedBytes(profile.serverRandomStart, FrpWireV2.CRYPTO_RANDOM_SIZE),
                serverHello.value.selected.crypto.serverRandom,
            )
            FrpWireV2.validateServerHelloForClient(clientHello.value, serverHello.value)
            assertFixtureMessage("wire-v1-login-response", loginResponse)

            val serverOutput = ByteArrayOutputStream()
            FrpWireV2.writeServerHello(serverOutput, serverHello.value)
            FrpWireV2.writeMessage(serverOutput, loginResponse)
            assertArrayEquals("server bootstrap ${profile.name}", serverSource, serverOutput.toByteArray())
        }
    }

    @Test
    fun plaintextControlGoldensRoundTripAsMessageSequences() {
        assertControlSequence(
            entryId = "control-v1-client-plain",
            decoder = FrpWireV1::readMessage,
            encoder = FrpWireV1::encodeMessage,
            expectedTypes = listOf(Ping::class.java, NewWorkConn::class.java),
        )
        assertControlSequence(
            entryId = "control-v1-server-plain",
            decoder = FrpWireV1::readMessage,
            encoder = FrpWireV1::encodeMessage,
            expectedTypes = listOf(Pong::class.java, ReqWorkConn::class.java),
        )
        assertControlSequence(
            entryId = "control-v2-client-plain",
            decoder = FrpWireV2::readMessage,
            encoder = FrpWireV2::encodeMessage,
            expectedTypes = listOf(Ping::class.java, NewWorkConn::class.java),
        )
        assertControlSequence(
            entryId = "control-v2-server-plain",
            decoder = FrpWireV2::readMessage,
            encoder = FrpWireV2::encodeMessage,
            expectedTypes = listOf(Pong::class.java, ReqWorkConn::class.java),
        )
    }

    @Test
    fun manifestChunkPlansAndDeterministicRandomSplitsDecode() {
        val v1Plan = FrpContractFixtures.chunkPlan("wire-v1-bytewise")
        val v1Input = PlannedChunkInputStream(bytesForPath(v1Plan.source), v1Plan.sizes)
        assertTrue(FrpWireV1.readMessage(v1Input) is Login)
        assertNull(FrpWireV1.readMessage(v1Input))

        val v2Plan = FrpContractFixtures.chunkPlan("wire-v2-frame-splits")
        val v2Source = bytesForPath(v2Plan.source)
        val v2Input = PlannedChunkInputStream(v2Source, v2Plan.sizes)
        FrpWireV2.readMagic(v2Input)
        assertEquals(FRP_AEAD_AES_256_GCM, FrpWireV2.readClientHello(v2Input).value.capabilities.crypto.algorithms.first())
        assertTrue(FrpWireV2.readMessage(v2Input) is Login)
        assertNull(FrpWireV2.readMessage(v2Input))

        val random = Random(0x5eed)
        val randomSizes = List(64) { random.nextInt(1, 33) }
        val randomInput = PlannedChunkInputStream(v2Source, randomSizes)
        FrpWireV2.readMagic(randomInput)
        FrpWireV2.readClientHello(randomInput)
        assertTrue(FrpWireV2.readMessage(randomInput) is Login)
        assertNull(FrpWireV2.readMessage(randomInput))
    }

    @Test
    fun manifestWireMutationsFailClosed() {
        assertFailure(FrpProtocolFailure.LENGTH_LIMIT) {
            FrpWireV1.readMessage(ByteArrayInputStream(FrpContractFixtures.mutated("wire-v1-length-max-plus-one")))
        }
        assertFailure(FrpProtocolFailure.NEGATIVE_LENGTH) {
            FrpWireV1.readMessage(ByteArrayInputStream(FrpContractFixtures.mutated("wire-v1-negative-length")))
        }
        assertFailure(FrpProtocolFailure.TRUNCATED_BODY) {
            FrpWireV1.readMessage(ByteArrayInputStream(FrpContractFixtures.mutated("wire-v1-truncated")))
        }
        assertFailure(FrpProtocolFailure.UNKNOWN_TYPE) {
            FrpWireV1.readMessage(ByteArrayInputStream(FrpContractFixtures.mutated("wire-v1-unknown-type")))
        }

        assertV2BootstrapFailure("wire-v2-error-flags", FrpProtocolFailure.INVALID_FLAGS)
        assertV2BootstrapFailure("wire-v2-payload-max-plus-one", FrpProtocolFailure.LENGTH_LIMIT)
        assertV2BootstrapFailure("wire-v2-truncated", FrpProtocolFailure.TRUNCATED_BODY)

        val unknownMessageInput = ByteArrayInputStream(FrpContractFixtures.mutated("wire-v2-unknown-message-type"))
        FrpWireV2.readMagic(unknownMessageInput)
        assertFailure(FrpProtocolFailure.UNKNOWN_TYPE) { FrpWireV2.readMessage(unknownMessageInput) }
    }

    @Test
    fun frameAndJsonBoundsAreCheckedBeforePayloadAllocation() {
        val exactV1Payload = "{}".padEnd(FrpWireV1.MAX_JSON_LENGTH, ' ').encodeToByteArray()
        assertTrue(FrpWireV1.readMessage(ByteArrayInputStream(v1Frame(FrpMessageTypes.V1_PONG, exactV1Payload))) is Pong)

        val exactV2Payload = ByteArray(FrpWireV2.MAX_FRAME_PAYLOAD)
        val exactV2 = FrpV2Frame(FrpWireV2.FRAME_TYPE_MESSAGE, payload = exactV2Payload)
        val exactOutput = ByteArrayOutputStream()
        FrpWireV2.writeFrame(exactOutput, exactV2)
        assertEquals(FrpWireV2.MAX_FRAME_PAYLOAD, FrpWireV2.readFrame(ByteArrayInputStream(exactOutput.toByteArray()))?.payload?.size)

        assertFailure(FrpProtocolFailure.LENGTH_LIMIT) {
            FrpWireV2.writeFrame(
                ByteArrayOutputStream(),
                FrpV2Frame(FrpWireV2.FRAME_TYPE_MESSAGE, payload = ByteArray(FrpWireV2.MAX_FRAME_PAYLOAD + 1)),
            )
        }
        assertFailure(FrpProtocolFailure.LENGTH_LIMIT) {
            FrpMessageJson.encode(Login(hostname = "x".repeat(128)), maximumBytes = 16)
        }
    }

    @Test
    fun jsonCompatibilityIsTolerantForUnknownFieldsAndStrictForTypesAndUtf8() {
        val ping = FrpMessageJson.decodeV1(
            FrpMessageTypes.V1_PING,
            """{"privilege_key":"fixture","timestamp":7,"future":{"enabled":true}}""".encodeToByteArray(),
        ) as Ping
        assertEquals("fixture", ping.privilegeKey)
        assertEquals(7L, ping.timestamp)

        assertFailure(FrpProtocolFailure.INVALID_FIELD) {
            FrpMessageJson.decodeV1(
                FrpMessageTypes.V1_PING,
                """{"timestamp":"7"}""".encodeToByteArray(),
            )
        }
        assertFailure(FrpProtocolFailure.INVALID_JSON) {
            FrpMessageJson.decodeV1(FrpMessageTypes.V1_PONG, byteArrayOf(0xc3.toByte()))
        }
    }

    @Test
    fun jsonEncodingMatchesGoEscapingAndUtf8MapOrdering() {
        val encoded = FrpMessageJson.encode(
            Login(
                metas = linkedMapOf(
                    "\uD800\uDC00" to "<",
                    "\uE000" to "&",
                    "a" to "\u2028",
                ),
            ),
            FrpWireV1.MAX_JSON_LENGTH,
        ).decodeToString()
        val expected =
            "{\"metas\":{\"a\":\"\\u2028\",\"\uE000\":\"\\u0026\",\"\uD800\uDC00\":\"\\u003c\"},\"client_spec\":{}}"

        assertEquals(expected, encoded)
    }

    @Test
    fun protocolSummariesAndFailuresDoNotExposeRuntimeValues() {
        val markers = listOf(
            "host-leak-marker",
            "token-leak-marker",
            "run-leak-marker",
            "proxy-leak-marker",
            "address-leak-marker",
            "error-leak-marker",
        )
        val startWorkConn = StartWorkConn(
            proxyName = markers[3],
            srcAddr = markers[4],
            dstAddr = markers[4],
            srcPort = 54_321,
            dstPort = 61_234,
            error = markers[5],
        )
        val values = listOf(
            Login(
                hostname = markers[0],
                privilegeKey = markers[1],
                runId = markers[2],
            ),
            NewVisitorConn(proxyName = markers[3], signKey = markers[1]),
            startWorkConn,
            ServerHello(error = markers[5]),
            FrpV2Frame(FrpWireV2.FRAME_TYPE_MESSAGE, payload = markers[1].encodeToByteArray()),
        )
        values.map(Any::toString).forEach { summary ->
            markers.forEach { marker -> assertFalse("Summary leaked runtime data", summary.contains(marker)) }
        }
        assertTrue(values.first().toString().contains("privilegeKey=<redacted>"))
        assertFalse(startWorkConn.toString().contains("54321"))
        assertFalse(startWorkConn.toString().contains("61234"))
        assertTrue(startWorkConn.toString().contains("srcPortPresent=true"))
        assertTrue(startWorkConn.toString().contains("dstPortPresent=true"))
        assertFalse(FrpProtocolException(FrpProtocolFailure.INVALID_JSON).toString().contains(markers[1]))
    }

    @Test
    fun cancellationAndJvmErrorsPropagateWhileIoIsMapped() {
        val cancellation = CancellationException("cancel-marker")
        val cancellationInput = object : InputStream() {
            override fun read(): Int = throw cancellation
        }
        assertSame(cancellation, assertThrows(CancellationException::class.java) { FrpWireV1.readMessage(cancellationInput) })

        val fatal = AssertionError("fatal-marker")
        val fatalInput = object : InputStream() {
            override fun read(): Int = throw fatal
        }
        assertSame(fatal, assertThrows(AssertionError::class.java) { FrpWireV1.readMessage(fatalInput) })

        val ioFailure = assertThrows(FrpProtocolException::class.java) {
            FrpWireV1.readMessage(object : InputStream() {
                override fun read(): Int = throw IOException("io-detail-marker")
            })
        }
        assertEquals(FrpProtocolFailure.IO_FAILURE, ioFailure.failure)
        assertFalse(ioFailure.toString().contains("io-detail-marker"))

        val writeFailure = assertThrows(FrpProtocolException::class.java) {
            FrpWireV1.writeMessage(object : OutputStream() {
                override fun write(value: Int) = throw IOException("write-detail-marker")
                override fun write(bytes: ByteArray, offset: Int, length: Int) = throw IOException("write-detail-marker")
            }, Pong())
        }
        assertEquals(FrpProtocolFailure.WRITE_FAILED, writeFailure.failure)
        assertFalse(writeFailure.toString().contains("write-detail-marker"))
    }

    @Test
    fun suspendReadersPreserveCoalescedPayloadAcrossSingleByteChunks() = runBlocking {
        val trailing = byteArrayOf(0x51, 0x52, 0x53)
        listOf(false, true).forEach { useV2 ->
            val response = if (useV2) {
                FrpWireV2.encodeMessage(NewVisitorConnResp(proxyName = "fixture"))
            } else {
                FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "fixture"))
            }
            val stream = ChunkedMuxStream(response + trailing, maximumChunkSize = 1)

            val decoded = if (useV2) {
                FrpWireV2.readMessage(stream)
            } else {
                FrpWireV1.readMessage(stream)
            }

            assertEquals(NewVisitorConnResp(proxyName = "fixture"), decoded)
            val remaining = ByteArray(trailing.size)
            var remainingCount = 0
            while (remainingCount < remaining.size) {
                remainingCount += stream.read(
                    remaining,
                    remainingCount,
                    remaining.size - remainingCount,
                )
            }
            assertArrayEquals(trailing, remaining)
        }
    }

    @Test
    fun suspendIoFailsClosedOnZeroReadsAndPreservesFatalIdentity() = runBlocking {
        val zeroRead = ChunkedMuxStream(ByteArray(0), returnZero = true)
        val protocolFailure = try {
            FrpWireV1.readMessage(zeroRead)
            throw AssertionError("Expected zero-length read to fail")
        } catch (failure: FrpProtocolException) {
            failure
        }
        assertEquals(FrpProtocolFailure.IO_FAILURE, protocolFailure.failure)

        val cancellation = CancellationException("cancel-marker")
        val cancelled = ThrowingMuxStream(cancellation)
        try {
            FrpWireV1.readMessage(cancelled)
            throw AssertionError("Expected cancellation")
        } catch (failure: CancellationException) {
            assertSame(cancellation, failure)
        }

        val fatal = AssertionError("fatal-marker")
        val failed = ThrowingMuxStream(fatal)
        try {
            FrpWireV2.readMessage(failed)
            throw AssertionError("Expected fatal failure")
        } catch (failure: AssertionError) {
            assertSame(fatal, failure)
        }
    }

    @Test
    fun suspendReadersApplyTheSameFramingFailures() = runBlocking {
        suspend fun assertSuspendFailure(
            expected: FrpProtocolFailure,
            source: ByteArray,
            reader: suspend (FrpMuxStream) -> FrpMessage?,
        ) {
            val failure = try {
                reader(ChunkedMuxStream(source, maximumChunkSize = 3))
                throw AssertionError("Expected protocol failure: $expected")
            } catch (failure: FrpProtocolException) {
                failure
            }
            assertEquals(expected, failure.failure)
        }

        assertSuspendFailure(
            FrpProtocolFailure.LENGTH_LIMIT,
            FrpContractFixtures.mutated("wire-v1-length-max-plus-one"),
            FrpWireV1::readMessage,
        )
        assertSuspendFailure(
            FrpProtocolFailure.NEGATIVE_LENGTH,
            FrpContractFixtures.mutated("wire-v1-negative-length"),
            FrpWireV1::readMessage,
        )
        assertSuspendFailure(
            FrpProtocolFailure.TRUNCATED_BODY,
            FrpContractFixtures.mutated("wire-v1-truncated"),
            FrpWireV1::readMessage,
        )
        assertSuspendFailure(
            FrpProtocolFailure.UNKNOWN_TYPE,
            FrpContractFixtures.mutated("wire-v1-unknown-type"),
            FrpWireV1::readMessage,
        )

        val v2Response = FrpWireV2.encodeMessage(NewVisitorConnResp(proxyName = "fixture"))
        assertSuspendFailure(
            FrpProtocolFailure.TRUNCATED_HEADER,
            v2Response.copyOf(4),
            FrpWireV2::readMessage,
        )
        assertSuspendFailure(
            FrpProtocolFailure.INVALID_FLAGS,
            v2Response.copyOf().also { it[3] = 1 },
            FrpWireV2::readMessage,
        )
        assertSuspendFailure(
            FrpProtocolFailure.LENGTH_LIMIT,
            v2Response.copyOf(8).also {
                ByteBuffer.wrap(it, 4, 4).order(ByteOrder.BIG_ENDIAN).putInt(FrpWireV2.MAX_FRAME_PAYLOAD + 1)
            },
            FrpWireV2::readMessage,
        )
        assertSuspendFailure(
            FrpProtocolFailure.TRUNCATED_BODY,
            v2Response.copyOf(v2Response.size - 1),
            FrpWireV2::readMessage,
        )
        assertSuspendFailure(
            FrpProtocolFailure.UNKNOWN_TYPE,
            v2Response.copyOf().also {
                it[8] = 0x7f
                it[9] = 0x7f
            },
            FrpWireV2::readMessage,
        )
    }

    private fun assertV2BootstrapFailure(mutationId: String, expected: FrpProtocolFailure) {
        val input = ByteArrayInputStream(FrpContractFixtures.mutated(mutationId))
        FrpWireV2.readMagic(input)
        assertFailure(expected) {
            FrpWireV2.readClientHello(input)
            FrpWireV2.readMessage(input)
        }
    }

    private fun assertControlSequence(
        entryId: String,
        decoder: (InputStream) -> FrpMessage?,
        encoder: (FrpMessage) -> ByteArray,
        expectedTypes: List<Class<out FrpMessage>>,
    ) {
        val source = FrpContractFixtures.bytes(entryId)
        val input = ByteArrayInputStream(source)
        val messages = mutableListOf<FrpMessage>()
        while (true) {
            val message = decoder(input) ?: break
            messages += message
        }
        assertEquals(expectedTypes, messages.map { it.javaClass })
        val output = ByteArrayOutputStream()
        messages.forEach { output.write(encoder(it)) }
        assertArrayEquals(entryId, source, output.toByteArray())
    }

    private fun assertFixtureMessage(entryId: String, message: FrpMessage) {
        when (entryId.removePrefix("wire-v2-").removePrefix("wire-v1-")) {
            "login-token" -> {
                message as Login
                assertEquals("0.69.1-fixture", message.version)
                assertEquals("fixture-client", message.hostname)
                assertEquals("android", message.os)
                assertEquals("arm64", message.arch)
                assertEquals("fixture-user", message.user)
                assertEquals(1_700_000_000L, message.timestamp)
                assertEquals(32, message.privilegeKey.length)
                assertEquals(1, message.poolCount)
                assertEquals(ClientSpec(), message.clientSpec)
            }
            "login-response" -> assertEquals(
                LoginResp(version = "0.69.1-fixture", runId = "run_fixture_001"),
                message,
            )
            "new-visitor-conn-response" -> assertEquals(
                NewVisitorConnResp(proxyName = "fixture-user.fixture-service"),
                message,
            )
            "new-visitor-stcp" -> {
                message as NewVisitorConn
                assertEquals("run_fixture_001", message.runId)
                assertEquals("fixture-user.fixture-service", message.proxyName)
                assertEquals(32, message.signKey.length)
                assertEquals(1_700_000_180L, message.timestamp)
                assertFalse(message.useEncryption)
                assertFalse(message.useCompression)
            }
            "start-work-conn" -> assertEquals(
                StartWorkConn(
                    proxyName = "fixture-user.fixture-service",
                    srcAddr = "127.0.0.1",
                    dstAddr = "127.0.0.1",
                    srcPort = 7000,
                    dstPort = 4096,
                ),
                message,
            )
            else -> throw AssertionError("Unknown fixture message: $entryId")
        }
    }

    private class ChunkedMuxStream(
        source: ByteArray,
        private val maximumChunkSize: Int = Int.MAX_VALUE,
        private val returnZero: Boolean = false,
    ) : FrpMuxStream {
        private val input = source.copyOf()
        private var position = 0

        override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int {
            if (returnZero) return 0
            if (position >= input.size) return -1
            val count = minOf(length, maximumChunkSize, input.size - position)
            input.copyInto(destination, offset, position, position + count)
            position += count
            return count
        }

        override suspend fun write(source: ByteArray, offset: Int, length: Int) = Unit

        override suspend fun closeWrite() = Unit

        override suspend fun reset() = Unit
    }

    private class ThrowingMuxStream(
        private val failure: Throwable,
    ) : FrpMuxStream {
        override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int = throw failure

        override suspend fun write(source: ByteArray, offset: Int, length: Int) = throw failure

        override suspend fun closeWrite() = Unit

        override suspend fun reset() = Unit
    }

    private fun assertFailure(expected: FrpProtocolFailure, operation: () -> Unit) {
        val failure = assertThrows(FrpProtocolException::class.java) { operation() }
        assertEquals(expected, failure.failure)
    }

    private fun bytesForPath(path: String): ByteArray {
        val entry = FrpContractFixtures.manifest.entries.single { it.path == path }
        return FrpContractFixtures.bytes(entry.id)
    }

    private fun v1Frame(type: Int, payload: ByteArray): ByteArray {
        val frame = ByteArray(1 + Long.SIZE_BYTES + payload.size)
        frame[0] = type.toByte()
        ByteBuffer.wrap(frame, 1, Long.SIZE_BYTES)
            .order(ByteOrder.BIG_ENDIAN)
            .putLong(payload.size.toLong())
        payload.copyInto(frame, destinationOffset = 1 + Long.SIZE_BYTES)
        return frame
    }

    private fun fixedBytes(start: Int, count: Int): ByteArray =
        ByteArray(count) { index -> (start + index).toByte() }

    private data class Profile(
        val name: String,
        val algorithm: String,
        val algorithms: List<String>,
        val clientRandomStart: Int,
        val serverRandomStart: Int,
    )
}
