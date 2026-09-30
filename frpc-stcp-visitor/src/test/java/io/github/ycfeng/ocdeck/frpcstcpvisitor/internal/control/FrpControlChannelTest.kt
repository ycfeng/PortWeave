package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.FrpContractFixtures
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FRP_AEAD_AES_256_GCM
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FRP_AEAD_XCHACHA20_POLY1305
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpMessage
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV1
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV2
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.Login
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.LoginResp
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.NewWorkConn
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.Ping
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.Pong
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.RawClientHello
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.ReqWorkConn
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.security.SecureRandom
import java.util.ArrayDeque
import java.util.concurrent.CancellationException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class FrpControlChannelTest {
    @Test
    fun v1BootstrapAndEncryptedControlMatchPinnedGoBytes() {
        val loginBytes = FrpContractFixtures.bytes("wire-v1-login-token")
        val login = FrpWireV1.readMessage(ByteArrayInputStream(loginBytes)) as Login
        val input = ByteArrayInputStream(
            FrpContractFixtures.bytes("wire-v1-login-response") +
                FrpContractFixtures.bytes("control-v1-server-cfb"),
        )
        val output = ByteArrayOutputStream()
        val loggedIn = FrpControlBootstrap(
            config = configFor(login, FrpWireProtocol.V1),
            secureRandom = SequenceSecureRandom(0x80),
        ).exchange(input, output, login)

        assertEquals("run_fixture_001", loggedIn.runId)
        assertTrue(loggedIn.channel.readMessage() is Pong)
        assertTrue(loggedIn.channel.readMessage() is ReqWorkConn)
        clientMessages(FrpWireProtocol.V1).forEach(loggedIn.channel::writeMessage)
        loggedIn.channel.close()

        assertArrayEquals(
            loginBytes + FrpContractFixtures.bytes("control-v1-client-cfb"),
            output.toByteArray(),
        )
        assertFalse(loggedIn.toString().contains("run_fixture_001"))
        assertFalse(loggedIn.toString().contains(FrpContractFixtures.syntheticToken))
    }

    @Test
    fun v2BootstrapAndEncryptedControlMatchPinnedGoBytesForBothAlgorithms() {
        listOf(
            V2Profile("aes-256-gcm", clientNonceStart = 0xa0),
            V2Profile("xchacha20-poly1305", clientNonceStart = 0xc0),
        ).forEach { profile ->
            val clientBootstrap = FrpContractFixtures.bytes(
                "wire-v2-${profile.name}-client-bootstrap",
            )
            val parsed = parseClientBootstrap(clientBootstrap)
            val input = ByteArrayInputStream(
                FrpContractFixtures.bytes("wire-v2-${profile.name}-server-bootstrap") +
                    FrpContractFixtures.bytes("control-v2-${profile.name}-server"),
            )
            val output = ByteArrayOutputStream()
            val clientRandomStart = parsed.hello.value.capabilities.crypto.clientRandom?.first()?.toInt()
                ?.and(0xff)
                ?: throw AssertionError("Fixture client random is missing")
            val loggedIn = FrpControlBootstrap(
                config = configFor(
                    login = parsed.login,
                    protocol = FrpWireProtocol.V2,
                    algorithms = parsed.hello.value.capabilities.crypto.algorithms,
                ),
                secureRandom = SequenceSecureRandom(clientRandomStart, profile.clientNonceStart),
            ).exchange(input, output, parsed.login)

            assertTrue(loggedIn.channel.readMessage() is Pong)
            assertTrue(loggedIn.channel.readMessage() is ReqWorkConn)
            clientMessages(FrpWireProtocol.V2).forEach(loggedIn.channel::writeMessage)
            loggedIn.channel.close()

            assertArrayEquals(
                profile.name,
                clientBootstrap + FrpContractFixtures.bytes("control-v2-${profile.name}-client"),
                output.toByteArray(),
            )
        }
    }

    @Test
    fun plaintextMessageChannelsPreservePinnedSequences() {
        listOf(
            PlainVector(FrpWireProtocol.V1, "control-v1-server-plain", "control-v1-client-plain"),
            PlainVector(FrpWireProtocol.V2, "control-v2-server-plain", "control-v2-client-plain"),
        ).forEach { vector ->
            val output = ByteArrayOutputStream()
            val channel = FrpMessageChannel(
                protocol = vector.protocol,
                input = ByteArrayInputStream(FrpContractFixtures.bytes(vector.serverEntry)),
                output = output,
            )

            assertTrue(channel.readMessage() is Pong)
            assertTrue(channel.readMessage() is ReqWorkConn)
            clientMessages(vector.protocol).forEach(channel::writeMessage)
            channel.close()

            assertArrayEquals(vector.clientEntry, FrpContractFixtures.bytes(vector.clientEntry), output.toByteArray())
        }
    }

    @Test
    fun loginRejectionDoesNotExposeServerTextOrCredentials() {
        val marker = "server-login-detail-marker"
        val token = "token-detail-marker"
        val login = Login(
            version = "fixture",
            hostname = "host-detail-marker",
            os = "android",
            arch = "arm64",
            privilegeKey = "auth-detail-marker",
            timestamp = 1L,
            poolCount = 1,
        )
        val response = FrpWireV1.encodeMessage(LoginResp(error = marker))
        val config = FrpControlConfig(token = token, version = "fixture", arch = "arm64")

        val failure = failureOf {
            FrpControlBootstrap(config).exchange(
                ByteArrayInputStream(response),
                ByteArrayOutputStream(),
                login,
            )
        }

        assertTrue(failure is FrpControlException)
        assertEquals(FrpControlFailure.LOGIN_REJECTED, (failure as FrpControlException).failure)
        assertEquals(null, failure.cause)
        assertFalse(failure.toString().contains(marker))
        assertFalse(failure.toString().contains(token))
        assertFalse(config.toString().contains(token))
        assertFalse(config.toString().contains("host-detail-marker"))
        assertTrue(config.toString().contains("token=<redacted>"))
    }

    @Test
    fun invalidControlConfigurationUsesATypeOnlyFailure() {
        val failure = failureOf {
            FrpControlConfig(
                token = "secret-config-marker",
                reconnectInitialDelayMillis = 20_001L,
            )
        }

        assertTrue(failure is FrpControlException)
        assertEquals(FrpControlFailure.INVALID_CONFIGURATION, (failure as FrpControlException).failure)
        assertFalse(failure.toString().contains("secret-config-marker"))
    }

    @Test
    fun defaultsMatchGoAuthenticationAndNormalizeJvmArchitectures() {
        val config = FrpControlConfig(token = "default token marker")

        assertTrue(config.authenticateHeartbeats)
        assertTrue(config.authenticateNewWorkConnections)
        assertEquals(10_000L, config.loginTimeoutMillis)
        assertEquals(60_000L, config.reconnectLoginTimeoutMillis)
        assertEquals(10_000L, config.workHandshakeTimeoutMillis)
        assertEquals(32, config.maxActiveWorkConnections)
        assertEquals(FrpControlConfig.DISABLED_HEARTBEAT_MILLIS, config.heartbeatIntervalMillis)
        assertEquals("arm64", normalizeFrpGoArch("aarch64"))
        assertEquals("arm64", normalizeFrpGoArch("arm64-v8a"))
        assertEquals("arm", normalizeFrpGoArch("arm"))
        assertEquals("arm", normalizeFrpGoArch("armv7l"))
        assertEquals("arm", normalizeFrpGoArch("armeabi-v7a"))
        assertEquals("amd64", normalizeFrpGoArch("x86_64"))
        assertEquals("amd64", normalizeFrpGoArch("AMD64"))
        assertEquals("386", normalizeFrpGoArch("x86"))
        assertEquals("386", normalizeFrpGoArch("i386"))
        assertEquals("386", normalizeFrpGoArch("i686"))
        assertEquals("loongarch-64", normalizeFrpGoArch("  LoongArch 64!  "))
        assertEquals("unknown", normalizeFrpGoArch("***"))
    }

    @Test
    fun activeWorkConnectionLimitIsBoundedAtThirtyTwo() {
        val failure = failureOf { FrpControlConfig(maxActiveWorkConnections = 33) }

        assertTrue(failure is FrpControlException)
        assertEquals(FrpControlFailure.INVALID_CONFIGURATION, (failure as FrpControlException).failure)
    }

    @Test
    fun workHandshakeTimeoutMustBePositive() {
        val failure = failureOf { FrpControlConfig(workHandshakeTimeoutMillis = 0L) }

        assertTrue(failure is FrpControlException)
        assertEquals(FrpControlFailure.INVALID_CONFIGURATION, (failure as FrpControlException).failure)
    }

    @Test
    fun reconnectLoginTimeoutMustBePositive() {
        val failure = failureOf { FrpControlConfig(reconnectLoginTimeoutMillis = 0L) }

        assertTrue(failure is FrpControlException)
        assertEquals(FrpControlFailure.INVALID_CONFIGURATION, (failure as FrpControlException).failure)
    }

    @Test
    fun concurrentControlWritesAreSerializedAsWholeMessages() {
        val output = BlockingConcurrentOutputStream()
        val channel = FrpMessageChannel(
            protocol = FrpWireProtocol.V1,
            input = ByteArrayInputStream(ByteArray(0)),
            output = output,
        )
        val firstFailure = AtomicReference<Throwable?>()
        val secondFailure = AtomicReference<Throwable?>()
        val first = Thread(
            { firstFailure.set(runCatching { channel.writeMessage(Ping(timestamp = 1L)) }.exceptionOrNull()) },
            "control-writer-first",
        )
        val secondStarted = CountDownLatch(1)
        val second = Thread(
            {
                secondStarted.countDown()
                secondFailure.set(runCatching { channel.writeMessage(Ping(timestamp = 2L)) }.exceptionOrNull())
            },
            "control-writer-second",
        )

        first.start()
        assertTrue(output.firstWriteEntered.await(5, TimeUnit.SECONDS))
        second.start()
        assertTrue(secondStarted.await(5, TimeUnit.SECONDS))
        assertFalse(output.secondWriteEntered.await(100, TimeUnit.MILLISECONDS))
        output.releaseFirstWrite.countDown()
        first.join(5_000L)
        second.join(5_000L)

        assertFalse(first.isAlive)
        assertFalse(second.isAlive)
        assertEquals(null, firstFailure.get())
        assertEquals(null, secondFailure.get())
        assertEquals(1, output.maximumConcurrentWrites.get())
        val input = ByteArrayInputStream(output.toByteArray())
        assertEquals(1L, (FrpWireV1.readMessage(input) as Ping).timestamp)
        assertEquals(2L, (FrpWireV1.readMessage(input) as Ping).timestamp)
        assertEquals(-1, input.read())
    }

    @Test
    fun channelClosePreservesJvmErrorAheadOfCancellationAndOrdinaryIo() {
        val cancellation = CancellationException("close cancellation marker")
        val error = AssertionError("close error marker")
        val marker = "close server endpoint token marker"
        val channel = FrpMessageChannel(
            protocol = FrpWireProtocol.V1,
            input = CloseFailureInputStream(IOException(marker)),
            output = CloseFailureOutputStream(error),
            closeOwner = AutoCloseable { throw cancellation },
        )

        val failure = runCatching { channel.close() }.exceptionOrNull()

        assertTrue(failure === error)
        assertTrue(error.suppressed.any { it === cancellation })
        assertFalse(channel.toString().contains(marker))
    }

    private fun configFor(
        login: Login,
        protocol: FrpWireProtocol,
        algorithms: List<String> = listOf(FRP_AEAD_AES_256_GCM, FRP_AEAD_XCHACHA20_POLY1305),
    ): FrpControlConfig = FrpControlConfig(
        token = FrpContractFixtures.syntheticToken,
        user = login.user,
        version = login.version,
        hostname = login.hostname,
        os = login.os,
        arch = login.arch,
        clientId = login.clientId,
        metas = login.metas,
        wireProtocol = protocol,
        preferredAeadAlgorithms = algorithms,
    )

    private fun clientMessages(protocol: FrpWireProtocol): List<FrpMessage> {
        val input = ByteArrayInputStream(
            FrpContractFixtures.bytes(
                if (protocol == FrpWireProtocol.V1) "control-v1-client-plain" else "control-v2-client-plain",
            ),
        )
        return buildList {
            while (true) {
                val message = when (protocol) {
                    FrpWireProtocol.V1 -> FrpWireV1.readMessage(input)
                    FrpWireProtocol.V2 -> FrpWireV2.readMessage(input)
                } ?: break
                add(message)
            }
        }.also { messages ->
            assertTrue(messages[0] is Ping)
            assertTrue(messages[1] is NewWorkConn)
        }
    }

    private fun parseClientBootstrap(bytes: ByteArray): ParsedClientBootstrap {
        val input = ByteArrayInputStream(bytes)
        FrpWireV2.readMagic(input)
        val hello = FrpWireV2.readClientHello(input)
        val login = FrpWireV2.readMessage(input) as Login
        assertEquals(-1, input.read())
        return ParsedClientBootstrap(hello, login)
    }

    private fun failureOf(block: () -> Unit): Throwable? =
        try {
            block()
            null
        } catch (failure: Throwable) {
            failure
        }

    private data class V2Profile(
        val name: String,
        val clientNonceStart: Int,
    )

    private data class PlainVector(
        val protocol: FrpWireProtocol,
        val serverEntry: String,
        val clientEntry: String,
    )

    private data class ParsedClientBootstrap(
        val hello: RawClientHello,
        val login: Login,
    )

    private class SequenceSecureRandom(vararg starts: Int) : SecureRandom() {
        private val starts = ArrayDeque<Int>().apply { starts.forEach(::addLast) }

        override fun nextBytes(bytes: ByteArray) {
            val start = starts.pollFirst()
                ?: throw AssertionError("Unexpected SecureRandom request")
            repeat(bytes.size) { index -> bytes[index] = (start + index).toByte() }
        }
    }

    private class BlockingConcurrentOutputStream : OutputStream() {
        val firstWriteEntered = CountDownLatch(1)
        val secondWriteEntered = CountDownLatch(1)
        val releaseFirstWrite = CountDownLatch(1)
        val maximumConcurrentWrites = AtomicInteger(0)
        private val activeWrites = AtomicInteger(0)
        private val writeCalls = AtomicInteger(0)
        private val delegate = ByteArrayOutputStream()

        override fun write(value: Int) {
            write(byteArrayOf(value.toByte()), 0, 1)
        }

        override fun write(source: ByteArray, offset: Int, length: Int) {
            val active = activeWrites.incrementAndGet()
            maximumConcurrentWrites.accumulateAndGet(active, ::maxOf)
            try {
                if (writeCalls.incrementAndGet() == 1) {
                    firstWriteEntered.countDown()
                    check(releaseFirstWrite.await(5, TimeUnit.SECONDS))
                } else {
                    secondWriteEntered.countDown()
                }
                synchronized(delegate) { delegate.write(source, offset, length) }
            } finally {
                activeWrites.decrementAndGet()
            }
        }

        fun toByteArray(): ByteArray = synchronized(delegate) { delegate.toByteArray() }
    }

    private class CloseFailureInputStream(private val failure: Throwable) : InputStream() {
        override fun read(): Int = -1

        override fun close() {
            throw failure
        }
    }

    private class CloseFailureOutputStream(private val failure: Throwable) : OutputStream() {
        override fun write(value: Int) = Unit

        override fun close() {
            throw failure
        }
    }
}
