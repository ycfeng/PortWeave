package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxFrameCodec
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxFrameType
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxGoAwayCode
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxPhysicalTransport
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxProtocol
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxProtocolFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxTermination
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxTransportFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.createYamuxClientSession
import io.github.ycfeng.ocdeck.frpcstcpvisitor.interop.InteropTlsMaterial
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketAddress
import java.net.SocketException
import java.nio.file.Files
import java.security.MessageDigest
import java.security.SecureRandom
import java.security.cert.CertificateException
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.concurrent.CancellationException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class FrpTransportTest {
    @Test
    fun productionDefaultsMatchPinnedFrpTransport() {
        val config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000))
        val yamux = productionFrpYamuxConfig()

        assertTrue(config.tlsEnabled)
        assertEquals(10_000L, config.connectTimeoutMillis)
        assertEquals(10_000L, config.tlsHandshakeTimeoutMillis)
        assertEquals(6L * 1_024L * 1_024L, yamux.receiveWindowSize)
        assertEquals(6 * 1_024 * 1_024, yamux.maxDataFrameSize)
        assertEquals(33, yamux.maxStreams)
        assertEquals(33, yamux.maxPendingOpens)
        assertTrue(yamux.maxStreams >= FrpControlConfig.DEFAULT_MAX_ACTIVE_WORK_CONNECTIONS + 1)
        assertTrue(yamux.maxPendingOpens >= yamux.maxStreams)
        assertTrue(yamux.keepAliveEnabled)
        assertEquals(30_000L, yamux.keepAliveIntervalMillis)
        assertFalse(config.toString().contains("fixture.invalid"))
    }

    @Test
    fun pinnedTrustManagerAcceptsOnlyTheConfiguredLeafCertificate() {
        val directory = Files.createTempDirectory("frp-pinned-tls-test-")
        try {
            val material = InteropTlsMaterial.create(directory, SecureRandom())
            val certificate = Files.newInputStream(material.certificatePath).use { input ->
                CertificateFactory.getInstance("X.509").generateCertificate(input) as X509Certificate
            }
            val pin = MessageDigest.getInstance("SHA-256").digest(certificate.encoded)
                .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }

            PinnedCertificateTrustManager(pin).checkServerTrusted(arrayOf(certificate), "RSA")
            assertThrows(CertificateException::class.java) {
                PinnedCertificateTrustManager("0".repeat(64))
                    .checkServerTrusted(arrayOf(certificate), "RSA")
            }
            assertTrue(
                tlsClientSocketFactory(
                    TcpTlsTransportConfig(
                        endpoint = FrpTcpEndpoint("fixture.invalid", 7_000),
                        tlsServerCertificateSha256 = pin,
                    ),
                ) is PinnedTlsClientSocketFactory,
            )

            val caDirectory = Files.createDirectories(directory.resolve("ca"))
            val caMaterial = InteropTlsMaterial.create(caDirectory, SecureRandom(), certificateAuthority = true)
            val caCertificate = Files.newInputStream(caMaterial.certificatePath).use { input ->
                CertificateFactory.getInstance("X.509").generateCertificate(input) as X509Certificate
            }
            val caPin = MessageDigest.getInstance("SHA-256").digest(caCertificate.encoded)
                .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
            assertThrows(CertificateException::class.java) {
                PinnedCertificateTrustManager(caPin).checkServerTrusted(arrayOf(caCertificate), "RSA")
            }
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun transportConfigRejectsInvalidTlsCertificatePins() {
        assertThrows(FrpTransportException::class.java) {
            TcpTlsTransportConfig(
                endpoint = FrpTcpEndpoint("fixture.invalid", 7_000),
                tlsServerCertificateSha256 = "A".repeat(64),
            )
        }
    }

    @Test
    fun ordinaryConnectFailureIsNotReclassifiedAsClosed() = runBlocking {
        val socket = TestSocket(connectFailure = IOException("synthetic endpoint marker"))
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000), tlsEnabled = false),
            socketFactory = TcpSocketFactory { socket },
            dispatcher = Dispatchers.IO,
        )

        val failure = suspendFailureOf { dialer.connect() }

        assertTrue(failure is FrpTransportException)
        assertEquals(FrpTransportFailure.CONNECT_FAILED, (failure as FrpTransportException).failure)
        assertEquals(1, socket.closeCount.get())
        assertFalse(failure.toString().contains("synthetic endpoint marker"))
        assertEquals(null, failure.cause)
    }

    @Test
    fun tlsHandshakeFailureClosesBothLayersAndUsesTypedFailure() = runBlocking {
        val socket = TestSocket()
        val tls = TestTlsSocket(handshakeFailure = IOException("synthetic tls marker"))
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000)),
            socketFactory = TcpSocketFactory { socket },
            tlsSocketFactory = TlsClientSocketFactory { actual, _ ->
                assertSame(socket, actual)
                tls
            },
            dispatcher = Dispatchers.IO,
        )

        val failure = suspendFailureOf { dialer.connect() }

        assertTrue(failure is FrpTransportException)
        assertEquals(FrpTransportFailure.TLS_HANDSHAKE_FAILED, (failure as FrpTransportException).failure)
        assertEquals(1, tls.closeCount.get())
        assertEquals(1, socket.closeCount.get())
        assertFalse(failure.toString().contains("synthetic tls marker"))
    }

    @Test
    fun tlsWrapsTheSocketBeforeAnyApplicationPreambleIsWritten() = runBlocking {
        val socket = TestSocket()
        val tls = TestTlsSocket()
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000)),
            socketFactory = TcpSocketFactory { socket },
            tlsSocketFactory = TlsClientSocketFactory { actual, _ ->
                assertSame(socket, actual)
                assertEquals(0, socket.writtenBytes().size)
                tls
            },
            dispatcher = Dispatchers.IO,
        )

        val transport = dialer.connect()

        assertEquals(0, socket.writtenBytes().size)
        transport.close()
        assertEquals(1, tls.closeCount.get())
        assertEquals(1, socket.closeCount.get())
    }

    @Test
    fun routedDialUsesResolvedAddressWhileTlsKeepsConfiguredEndpoint() = runBlocking {
        val socket = TestSocket()
        val tls = TestTlsSocket()
        val configuredEndpoint = FrpTcpEndpoint("fixture.invalid", 7_000)
        val resolvedAddress = InetAddress.getByAddress(byteArrayOf(1, 2, 3, 4))
        var resolvedHost: String? = null
        var tlsEndpoint: FrpTcpEndpoint? = null
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(configuredEndpoint),
            routeProvider = TcpDialRouteProvider {
                object : TcpDialRoute {
                    override fun resolve(host: String): List<InetAddress> {
                        resolvedHost = host
                        return listOf(resolvedAddress)
                    }

                    override fun createSocket(): Socket = socket
                }
            },
            tlsSocketFactory = TlsClientSocketFactory { actual, endpoint ->
                assertSame(socket, actual)
                tlsEndpoint = endpoint
                tls
            },
            dispatcher = Dispatchers.IO,
        )

        val transport = dialer.connect()

        assertEquals(configuredEndpoint.host, resolvedHost)
        assertEquals(InetSocketAddress(resolvedAddress, configuredEndpoint.port), socket.connectedEndpoint)
        assertEquals(configuredEndpoint, tlsEndpoint)
        transport.close()
    }

    @Test
    fun routedDialFallsBackToTheNextResolvedAddress() = runBlocking {
        val firstSocket = TestSocket(connectFailure = IOException("synthetic first route failure"))
        val secondSocket = TestSocket()
        val sockets = ArrayDeque(listOf(firstSocket, secondSocket))
        val tls = TestTlsSocket()
        val configuredEndpoint = FrpTcpEndpoint("fixture.invalid", 7_000)
        val firstAddress = InetAddress.getByAddress(byteArrayOf(1, 2, 3, 4))
        val secondAddress = InetAddress.getByAddress(byteArrayOf(5, 6, 7, 8))
        var tlsEndpoint: FrpTcpEndpoint? = null
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(configuredEndpoint),
            routeProvider = TcpDialRouteProvider {
                object : TcpDialRoute {
                    override fun resolve(host: String): List<InetAddress> = listOf(firstAddress, secondAddress)

                    override fun createSocket(): Socket = sockets.removeFirst()
                }
            },
            tlsSocketFactory = TlsClientSocketFactory { actual, endpoint ->
                assertSame(secondSocket, actual)
                tlsEndpoint = endpoint
                tls
            },
            dispatcher = Dispatchers.IO,
        )

        val transport = dialer.connect()

        assertEquals(InetSocketAddress(firstAddress, configuredEndpoint.port), firstSocket.connectedEndpoint)
        assertEquals(1, firstSocket.closeCount.get())
        assertEquals(InetSocketAddress(secondAddress, configuredEndpoint.port), secondSocket.connectedEndpoint)
        assertEquals(configuredEndpoint, tlsEndpoint)
        transport.close()
        assertEquals(1, secondSocket.closeCount.get())
    }

    @Test
    fun routedDialFallsBackWhenTheFirstTlsHandshakeFails() = runBlocking {
        val firstSocket = TestSocket()
        val secondSocket = TestSocket()
        val sockets = ArrayDeque(listOf(firstSocket, secondSocket))
        val firstTls = TestTlsSocket(handshakeFailure = IOException("synthetic first tls route failure"))
        val secondTls = TestTlsSocket()
        val configuredEndpoint = FrpTcpEndpoint("fixture.invalid", 7_000)
        val firstAddress = InetAddress.getByAddress(byteArrayOf(1, 2, 3, 4))
        val secondAddress = InetAddress.getByAddress(byteArrayOf(5, 6, 7, 8))
        val tlsEndpoints = mutableListOf<FrpTcpEndpoint>()
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(configuredEndpoint),
            routeProvider = TcpDialRouteProvider {
                object : TcpDialRoute {
                    override fun resolve(host: String): List<InetAddress> = listOf(firstAddress, secondAddress)

                    override fun createSocket(): Socket = sockets.removeFirst()
                }
            },
            tlsSocketFactory = TlsClientSocketFactory { actual, endpoint ->
                tlsEndpoints += endpoint
                when (actual) {
                    firstSocket -> firstTls
                    secondSocket -> secondTls
                    else -> error("unexpected socket")
                }
            },
            dispatcher = Dispatchers.IO,
        )

        val transport = dialer.connect()

        assertEquals(InetSocketAddress(firstAddress, configuredEndpoint.port), firstSocket.connectedEndpoint)
        assertEquals(1, firstTls.closeCount.get())
        assertEquals(1, firstSocket.closeCount.get())
        assertEquals(InetSocketAddress(secondAddress, configuredEndpoint.port), secondSocket.connectedEndpoint)
        assertEquals(listOf(configuredEndpoint, configuredEndpoint), tlsEndpoints)
        transport.close()
        assertEquals(1, secondTls.closeCount.get())
        assertEquals(1, secondSocket.closeCount.get())
    }

    @Test
    fun routedDialReturnsTheLastTlsFailureAfterAllCandidatesFail() = runBlocking {
        val firstSocket = TestSocket()
        val secondSocket = TestSocket()
        val sockets = ArrayDeque(listOf(firstSocket, secondSocket))
        val firstTls = TestTlsSocket(handshakeFailure = IOException("synthetic first tls failure"))
        val secondTls = TestTlsSocket(handshakeFailure = IOException("synthetic second tls failure"))
        val tlsSockets = ArrayDeque(listOf(firstTls, secondTls))
        val configuredEndpoint = FrpTcpEndpoint("fixture.invalid", 7_000)
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(configuredEndpoint),
            routeProvider = TcpDialRouteProvider {
                object : TcpDialRoute {
                    override fun resolve(host: String): List<InetAddress> = listOf(
                        InetAddress.getByAddress(byteArrayOf(1, 2, 3, 4)),
                        InetAddress.getByAddress(byteArrayOf(5, 6, 7, 8)),
                    )

                    override fun createSocket(): Socket = sockets.removeFirst()
                }
            },
            tlsSocketFactory = TlsClientSocketFactory { _, endpoint ->
                assertEquals(configuredEndpoint, endpoint)
                tlsSockets.removeFirst()
            },
            dispatcher = Dispatchers.IO,
        )

        val failure = suspendFailureOf { dialer.connect() }

        assertTrue(failure is FrpTransportException)
        assertEquals(FrpTransportFailure.TLS_HANDSHAKE_FAILED, (failure as FrpTransportException).failure)
        assertEquals(1, firstTls.closeCount.get())
        assertEquals(1, secondTls.closeCount.get())
        assertEquals(1, firstSocket.closeCount.get())
        assertEquals(1, secondSocket.closeCount.get())
    }

    @Test
    fun routedDialFallsBackAfterACandidateTlsHandshakeTimeout() = runBlocking {
        val firstSocket = TestSocket()
        val secondSocket = TestSocket()
        val sockets = ArrayDeque(listOf(firstSocket, secondSocket))
        val firstTls = TestTlsSocket(blockHandshakeUntilClose = true)
        val secondTls = TestTlsSocket()
        val tlsSockets = ArrayDeque(listOf(firstTls, secondTls))
        val configuredEndpoint = FrpTcpEndpoint("fixture.invalid", 7_000)
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(
                endpoint = configuredEndpoint,
                tlsHandshakeTimeoutMillis = 25L,
            ),
            routeProvider = TcpDialRouteProvider {
                object : TcpDialRoute {
                    override fun resolve(host: String): List<InetAddress> = listOf(
                        InetAddress.getByAddress(byteArrayOf(1, 2, 3, 4)),
                        InetAddress.getByAddress(byteArrayOf(5, 6, 7, 8)),
                    )

                    override fun createSocket(): Socket = sockets.removeFirst()
                }
            },
            tlsSocketFactory = TlsClientSocketFactory { _, endpoint ->
                assertEquals(configuredEndpoint, endpoint)
                tlsSockets.removeFirst()
            },
            dispatcher = Dispatchers.IO,
        )

        val transport = dialer.connect()

        assertEquals(1, firstTls.closeCount.get())
        assertEquals(1, firstSocket.closeCount.get())
        transport.close()
        assertEquals(1, secondTls.closeCount.get())
        assertEquals(1, secondSocket.closeCount.get())
    }

    @Test
    fun routedDialFallsBackAfterACandidateTcpConnectTimeout() = runBlocking {
        val firstSocket = TestSocket(blockConnectUntilClose = true)
        val secondSocket = TestSocket()
        val sockets = ArrayDeque(listOf(firstSocket, secondSocket))
        val configuredEndpoint = FrpTcpEndpoint("fixture.invalid", 7_000)
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(
                endpoint = configuredEndpoint,
                tlsEnabled = false,
                connectTimeoutMillis = 100L,
            ),
            routeProvider = TcpDialRouteProvider {
                object : TcpDialRoute {
                    override fun resolve(host: String): List<InetAddress> = listOf(
                        InetAddress.getByAddress(byteArrayOf(1, 2, 3, 4)),
                        InetAddress.getByAddress(byteArrayOf(5, 6, 7, 8)),
                    )

                    override fun createSocket(): Socket = sockets.removeFirst()
                }
            },
            dispatcher = Dispatchers.IO,
        )

        val transport = dialer.connect()

        assertEquals(1, firstSocket.closeCount.get())
        transport.close()
        assertEquals(1, secondSocket.closeCount.get())
    }

    @Test
    fun cancellationClosesBlockedConnectAndPreservesItsCause() = runBlocking {
        val socket = TestSocket(blockConnectUntilClose = true)
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000), tlsEnabled = false),
            socketFactory = TcpSocketFactory { socket },
            dispatcher = Dispatchers.IO,
        )
        val connection = async { dialer.connect() }
        assertTrue(withContext(Dispatchers.IO) { socket.connectStarted.await(5, TimeUnit.SECONDS) })
        val cancellation = CancellationException("synthetic cancellation marker")

        connection.cancel(cancellation)
        val failure = suspendFailureOf { connection.await() }

        assertOriginalCancellation(cancellation, failure)
        assertEquals(cancellation.message, failure?.message)
        withTimeout(5_000L) {
            while (dialer.diagnostics().activeOperations != 0) yield()
        }
        assertEquals(1, socket.closeCount.get())
        assertTrue(dialer.diagnostics().closed)
    }

    @Test
    fun cancellationClosesASocketCreatedAfterTheDialWasCancelled() = runBlocking {
        val socket = TestSocket()
        val factoryStarted = CountDownLatch(1)
        val releaseFactory = CountDownLatch(1)
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000), tlsEnabled = false),
            socketFactory = TcpSocketFactory {
                factoryStarted.countDown()
                releaseFactory.await()
                socket
            },
            dispatcher = Dispatchers.IO,
        )
        val connection = async { dialer.connect() }
        assertTrue(withContext(Dispatchers.IO) { factoryStarted.await(5, TimeUnit.SECONDS) })

        connection.cancel(CancellationException("late socket cancellation marker"))
        releaseFactory.countDown()
        assertTrue(suspendFailureOf { connection.await() } is CancellationException)
        withTimeout(5_000L) {
            while (dialer.diagnostics().activeOperations != 0) yield()
        }

        assertEquals(1, socket.closeCount.get())
        assertTrue(dialer.diagnostics().closed)
    }

    @Test
    fun cancellationClosesATlsWrapperCreatedAfterTheDialWasCancelled() = runBlocking {
        val socket = TestSocket()
        val tls = TestTlsSocket()
        val factoryStarted = CountDownLatch(1)
        val releaseFactory = CountDownLatch(1)
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000)),
            socketFactory = TcpSocketFactory { socket },
            tlsSocketFactory = TlsClientSocketFactory { _, _ ->
                factoryStarted.countDown()
                releaseFactory.await()
                tls
            },
            dispatcher = Dispatchers.IO,
        )
        val connection = async { dialer.connect() }
        assertTrue(withContext(Dispatchers.IO) { factoryStarted.await(5, TimeUnit.SECONDS) })

        connection.cancel(CancellationException("late tls cancellation marker"))
        releaseFactory.countDown()
        assertTrue(suspendFailureOf { connection.await() } is CancellationException)
        withTimeout(5_000L) {
            while (dialer.diagnostics().activeOperations != 0) yield()
        }

        assertEquals(1, tls.closeCount.get())
        assertEquals(1, socket.closeCount.get())
        assertTrue(dialer.diagnostics().closed)
    }

    @Test
    fun cancellationAtDialResultHandoffClosesTlsAndSocketExactlyOnce() = runBlocking {
        val socket = TestSocket()
        val tls = TestTlsSocket()
        val handoffEntered = CompletableDeferred<Unit>()
        val releaseHandoff = CompletableDeferred<Unit>()
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000)),
            socketFactory = TcpSocketFactory { socket },
            tlsSocketFactory = TlsClientSocketFactory { _, _ -> tls },
            dispatcher = Dispatchers.IO,
            beforeResultHandoff = {
                handoffEntered.complete(Unit)
                withContext(NonCancellable) { releaseHandoff.await() }
            },
        )
        val connection = async { dialer.connect() }
        handoffEntered.await()
        val cancellation = CancellationException("dial handoff cancellation marker")

        connection.cancel(cancellation)
        releaseHandoff.complete(Unit)
        assertOriginalCancellation(cancellation, suspendFailureOf { connection.await() })

        assertEquals(1, tls.closeCount.get())
        assertEquals(1, socket.closeCount.get())
        assertTrue(dialer.diagnostics().closed)
    }

    @Test
    fun dialHandoffCleanupJvmErrorOutranksCancellation() = runBlocking {
        val error = SyntheticTransportError()
        val socket = TestSocket(closeFailure = error)
        val handoffEntered = CompletableDeferred<Unit>()
        val releaseHandoff = CompletableDeferred<Unit>()
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000), tlsEnabled = false),
            socketFactory = TcpSocketFactory { socket },
            dispatcher = Dispatchers.IO,
            beforeResultHandoff = {
                handoffEntered.complete(Unit)
                withContext(NonCancellable) { releaseHandoff.await() }
            },
        )
        val callerScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val connection = callerScope.async { dialer.connect() }
        handoffEntered.await()
        val cancellation = CancellationException("dial fatal handoff cancellation marker")

        connection.cancel(cancellation)
        releaseHandoff.complete(Unit)
        val failure = suspendFailureOf { connection.await() }

        assertSame(error, failure)
        assertTrue(error.suppressed.any { it === cancellation || it.cause === cancellation })
        assertEquals(1, socket.closeCount.get())
        callerScope.cancel()
    }

    @Test
    fun cancellationAtConnectorResultHandoffClosesConnectionExactlyOnce() = runBlocking {
        val socket = TestSocket(blockReadUntilClose = true)
        val handoffEntered = CompletableDeferred<Unit>()
        val releaseHandoff = CompletableDeferred<Unit>()
        val parentScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val connector = TcpTlsYamuxConnector(
            transportConfig = TcpTlsTransportConfig(
                endpoint = FrpTcpEndpoint("fixture.invalid", 7_000),
                tlsEnabled = false,
            ),
            parentScope = parentScope,
            socketFactory = TcpSocketFactory { socket },
            transportDispatcher = Dispatchers.IO,
            yamuxConfig = YamuxConfig(keepAliveEnabled = false),
            beforeResultHandoff = {
                handoffEntered.complete(Unit)
                withContext(NonCancellable) { releaseHandoff.await() }
            },
        )
        val connection = async { connector.connect() }
        handoffEntered.await()
        val cancellation = CancellationException("connector handoff cancellation marker")

        connection.cancel(cancellation)
        releaseHandoff.complete(Unit)
        assertOriginalCancellation(cancellation, suspendFailureOf { connection.await() })
        withTimeout(5_000L) {
            while (socket.closeCount.get() != 1) yield()
        }

        assertEquals(1, socket.closeCount.get())
        parentScope.cancel()
    }

    @Test
    fun connectorHandoffCleanupJvmErrorRemainsObservableAfterCancellation() = runBlocking {
        val error = SyntheticTransportError()
        val socket = TestSocket(blockReadUntilClose = true, closeFailure = error)
        val handoffEntered = CompletableDeferred<Unit>()
        val releaseHandoff = CompletableDeferred<Unit>()
        val parentScope = CoroutineScope(
            SupervisorJob() + Dispatchers.Default + CoroutineExceptionHandler { _, _ -> },
        )
        val callerScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val connector = TcpTlsYamuxConnector(
            transportConfig = TcpTlsTransportConfig(
                endpoint = FrpTcpEndpoint("fixture.invalid", 7_000),
                tlsEnabled = false,
            ),
            parentScope = parentScope,
            socketFactory = TcpSocketFactory { socket },
            transportDispatcher = Dispatchers.IO,
            yamuxConfig = YamuxConfig(keepAliveEnabled = false),
            beforeResultHandoff = {
                handoffEntered.complete(Unit)
                withContext(NonCancellable) { releaseHandoff.await() }
            },
        )
        val connection = callerScope.async { connector.connect() }
        handoffEntered.await()
        val cancellation = CancellationException("connector fatal handoff cancellation marker")

        connection.cancel(cancellation)
        releaseHandoff.complete(Unit)
        val failure = suspendFailureOf { connection.await() }

        assertSame(error, failure)
        assertTrue(error.suppressed.any { it === cancellation || it.cause === cancellation })
        assertEquals(1, socket.closeCount.get())
        callerScope.cancel()
        parentScope.cancel()
    }

    @Test
    fun outerTimeoutCancellationPreservesIdentityAndClosesBlockedConnect() = runBlocking {
        val socket = TestSocket(blockConnectUntilClose = true)
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000), tlsEnabled = false),
            socketFactory = TcpSocketFactory { socket },
            dispatcher = Dispatchers.IO,
        )
        val observed = AtomicReference<Throwable?>()
        val connection = async {
            try {
                dialer.connect()
            } catch (failure: CancellationException) {
                observed.set(failure)
                throw failure
            }
        }
        assertTrue(withContext(Dispatchers.IO) { socket.connectStarted.await(5, TimeUnit.SECONDS) })
        val timeout = newTimeoutCancellation()

        connection.cancel(timeout)
        val failure = suspendFailureOf { connection.await() }

        assertOriginalCancellation(timeout, observed.get())
        assertOriginalCancellation(timeout, failure)
        withTimeout(5_000L) {
            while (dialer.diagnostics().activeOperations != 0) yield()
        }
        assertEquals(1, socket.closeCount.get())
    }

    @Test
    fun outerTimeoutCancellationPreservesIdentityAndClosesBlockedTlsHandshake() = runBlocking {
        val socket = TestSocket()
        val tls = TestTlsSocket(blockHandshakeUntilClose = true)
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000)),
            socketFactory = TcpSocketFactory { socket },
            tlsSocketFactory = TlsClientSocketFactory { _, _ -> tls },
            dispatcher = Dispatchers.IO,
        )
        val observed = AtomicReference<Throwable?>()
        val connection = async {
            try {
                dialer.connect()
            } catch (failure: CancellationException) {
                observed.set(failure)
                throw failure
            }
        }
        assertTrue(withContext(Dispatchers.IO) { tls.handshakeStarted.await(5, TimeUnit.SECONDS) })
        val timeout = newTimeoutCancellation()

        connection.cancel(timeout)
        val failure = suspendFailureOf { connection.await() }

        assertOriginalCancellation(timeout, observed.get())
        assertOriginalCancellation(timeout, failure)
        withTimeout(5_000L) {
            while (dialer.diagnostics().activeOperations != 0) yield()
        }
        assertEquals(1, tls.closeCount.get())
        assertEquals(1, socket.closeCount.get())
    }

    @Test
    fun connectBudgetTimeoutMapsToTypedFailureAndClosesSocket() = runBlocking {
        val socket = TestSocket(blockConnectUntilClose = true)
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(
                endpoint = FrpTcpEndpoint("fixture.invalid", 7_000),
                tlsEnabled = false,
                connectTimeoutMillis = 25L,
            ),
            socketFactory = TcpSocketFactory { socket },
            dispatcher = Dispatchers.IO,
        )

        val failure = suspendFailureOf { dialer.connect() }

        assertTrue(failure is FrpTransportException)
        assertEquals(FrpTransportFailure.CONNECT_TIMEOUT, (failure as FrpTransportException).failure)
        withTimeout(5_000L) {
            while (dialer.diagnostics().activeOperations != 0) yield()
        }
        assertEquals(1, socket.closeCount.get())
    }

    @Test
    fun tlsBudgetTimeoutMapsToTypedFailureAndClosesBothLayers() = runBlocking {
        val socket = TestSocket()
        val tls = TestTlsSocket(blockHandshakeUntilClose = true)
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(
                endpoint = FrpTcpEndpoint("fixture.invalid", 7_000),
                tlsHandshakeTimeoutMillis = 25L,
            ),
            socketFactory = TcpSocketFactory { socket },
            tlsSocketFactory = TlsClientSocketFactory { _, _ -> tls },
            dispatcher = Dispatchers.IO,
        )

        val failure = suspendFailureOf { dialer.connect() }

        assertTrue(failure is FrpTransportException)
        assertEquals(FrpTransportFailure.TLS_HANDSHAKE_TIMEOUT, (failure as FrpTransportException).failure)
        withTimeout(5_000L) {
            while (dialer.diagnostics().activeOperations != 0) yield()
        }
        assertEquals(1, tls.closeCount.get())
        assertEquals(1, socket.closeCount.get())
    }

    @Test
    fun injectedCancellationKeepsItsOriginalInstance() = runTest {
        val cancellation = CancellationException("injected cancellation marker")
        val socket = TestSocket(connectFailure = cancellation)
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000), tlsEnabled = false),
            socketFactory = TcpSocketFactory { socket },
            dispatcher = UnconfinedTestDispatcher(testScheduler),
        )

        assertOriginalCancellation(cancellation, suspendFailureOf { dialer.connect() })
        assertEquals(1, socket.closeCount.get())
    }

    @Test
    fun dispatcherRejectionResumesDialAndTransportOperationsWithoutLeaks() = runBlocking {
        val marker = "dispatcher endpoint token marker"
        val ordinaryDialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000), tlsEnabled = false),
            socketFactory = TcpSocketFactory { throw AssertionError("socket factory must not run") },
            dispatcher = RejectingDispatcher(IOException(marker)),
        )

        val ordinaryFailure = withTimeout(5_000L) { suspendFailureOf { ordinaryDialer.connect() } }
        assertTrue("unexpected rejection failure: $ordinaryFailure", ordinaryFailure is FrpTransportException)
        assertEquals(FrpTransportFailure.CONNECT_FAILED, (ordinaryFailure as FrpTransportException).failure)
        assertEquals(0, ordinaryDialer.diagnostics().activeOperations)
        assertFalse(ordinaryFailure.toString().contains(marker))

        val cancellation = CancellationException("dispatcher cancellation marker")
        val cancellationDialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000), tlsEnabled = false),
            socketFactory = TcpSocketFactory { throw AssertionError("socket factory must not run") },
            dispatcher = RejectingDispatcher(cancellation),
        )
        assertOriginalCancellation(
            cancellation,
            withTimeout(5_000L) { suspendFailureOf { cancellationDialer.connect() } },
        )
        assertEquals(0, cancellationDialer.diagnostics().activeOperations)

        val error = SyntheticTransportError()
        val errorDialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000), tlsEnabled = false),
            socketFactory = TcpSocketFactory { throw AssertionError("socket factory must not run") },
            dispatcher = RejectingDispatcher(error),
        )
        assertSame(error, withTimeout(5_000L) { suspendFailureOf { errorDialer.connect() } })
        assertEquals(0, errorDialer.diagnostics().activeOperations)

        val transport = TcpTlsYamuxTransport(
            input = ByteArrayInputStream(ByteArray(0)),
            output = ByteArrayOutputStream(),
            closeController = TransportCloseController(),
            dispatcher = RejectingDispatcher(IOException(marker)),
        )
        val readFailure = withTimeout(5_000L) {
            suspendFailureOf { transport.read(ByteArray(1), 0, 1) }
        }
        assertTrue(readFailure is FrpTransportException)
        assertEquals(FrpTransportFailure.READ_FAILED, (readFailure as FrpTransportException).failure)
        assertEquals(0, transport.diagnostics().activeReads)
        assertFalse(readFailure.toString().contains(marker))
        val writeFailure = withTimeout(5_000L) {
            suspendFailureOf { transport.writeFully(byteArrayOf(1)) }
        }
        assertTrue(writeFailure is FrpTransportException)
        assertEquals(FrpTransportFailure.WRITE_FAILED, (writeFailure as FrpTransportException).failure)
        assertEquals(0, transport.diagnostics().activeWrites)
    }

    @Test
    fun cancelledBlockingOperationLateJvmErrorEscapesRunnableAndReleasesDiagnostics() = runBlocking {
        val dispatcher = QueueingDispatcher()
        val error = AssertionError("late blocking operation error marker")
        val dialer = TcpTlsTransportDialer(
            config = TcpTlsTransportConfig(FrpTcpEndpoint("fixture.invalid", 7_000), tlsEnabled = false),
            socketFactory = TcpSocketFactory { throw error },
            dispatcher = dispatcher,
        )
        val callerScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val connection = callerScope.async { dialer.connect() }
        assertTrue(withContext(Dispatchers.IO) { dispatcher.queued.await(5, TimeUnit.SECONDS) })
        val task = dispatcher.take()
        val cancellation = CancellationException("queued operation cancellation marker")

        connection.cancel(cancellation)
        assertOriginalCancellation(cancellation, suspendFailureOf { connection.await() })

        val uncaught = AtomicReference<Throwable?>()
        val uncaughtSignal = CountDownLatch(1)
        val thread = Thread(task, "frp-late-error-fixture").apply {
            uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, failure ->
                uncaught.set(failure)
                uncaughtSignal.countDown()
            }
        }
        thread.start()
        assertTrue(withContext(Dispatchers.IO) { uncaughtSignal.await(5, TimeUnit.SECONDS) })
        withContext(Dispatchers.IO) { thread.join(5_000L) }

        assertFalse(thread.isAlive)
        assertSame(error, uncaught.get())
        assertEquals(0, dialer.diagnostics().activeOperations)
        callerScope.cancel()
    }

    @Test
    fun positiveLengthZeroReadIsATypedTransportFailure() = runBlocking {
        val transport = TcpTlsYamuxTransport(
            input = ZeroInputStream(),
            output = ByteArrayOutputStream(),
            closeController = TransportCloseController(),
            dispatcher = Dispatchers.IO,
        )

        val failure = suspendFailureOf { transport.read(ByteArray(1), 0, 1) }

        assertTrue(failure is FrpTransportException)
        assertEquals(FrpTransportFailure.READ_FAILED, (failure as FrpTransportException).failure)
        assertEquals(0, transport.diagnostics().activeReads)
    }

    @Test
    fun readerLoopUsesOneDispatcherEntryForSequentialReadsThroughSharedClose() = runBlocking {
        val dispatcher = QueueingDispatcher()
        val transport = TcpTlsYamuxTransport(
            input = ByteArrayInputStream(byteArrayOf(7, 9)),
            output = ByteArrayOutputStream(),
            closeController = TransportCloseController(),
            dispatcher = dispatcher,
        )
        val shared = SharedCloseYamuxPhysicalTransport(transport)
        val observed = AtomicReference<ByteArray?>()
        val result = async(Dispatchers.Default) {
            shared.runReaderLoop {
                val destination = ByteArray(2)
                assertEquals(1, shared.read(destination, 0, 1))
                assertEquals(1, shared.read(destination, 1, 1))
                observed.set(destination)
            }
        }
        assertTrue(withContext(Dispatchers.IO) { dispatcher.queued.await(5, TimeUnit.SECONDS) })

        dispatcher.take().run()

        withTimeout(5_000L) { result.await() }
        assertTrue(checkNotNull(observed.get()).contentEquals(byteArrayOf(7, 9)))
        assertEquals(1, dispatcher.dispatchCount.get())
        assertEquals(0, transport.diagnostics().activeReads)
    }

    @Test
    fun cancellingDirectReaderClosesBlockedSocketAndPreservesCause() = runBlocking {
        val socket = TestSocket(blockReadUntilClose = true)
        val closeController = TransportCloseController().also { it.attachSocket(socket) }
        val transport = TcpTlsYamuxTransport(
            input = socket.getInputStream(),
            output = socket.getOutputStream(),
            closeController = closeController,
            dispatcher = Dispatchers.IO,
        )
        val reader = async {
            transport.runReaderLoop {
                transport.read(ByteArray(1), 0, 1)
            }
        }
        assertTrue(withContext(Dispatchers.IO) { socket.readStarted.await(5, TimeUnit.SECONDS) })
        val cancellation = CancellationException("direct reader cancellation marker")

        reader.cancel(cancellation)
        val failure = withTimeout(5_000L) { suspendFailureOf { reader.await() } }

        assertOriginalCancellation(cancellation, failure)
        assertEquals(1, socket.closeCount.get())
        assertEquals(0, transport.diagnostics().activeReads)
        assertTrue(transport.diagnostics().closed)
    }

    @Test
    fun readerLoopJvmErrorPreservesIdentityAndReleasesDiagnostics() = runBlocking {
        val error = SyntheticTransportError()
        val transport = TcpTlsYamuxTransport(
            input = object : InputStream() {
                override fun read(): Int = throw error

                override fun read(destination: ByteArray, offset: Int, length: Int): Int = throw error
            },
            output = ByteArrayOutputStream(),
            closeController = TransportCloseController(),
            dispatcher = Dispatchers.IO,
        )

        val failure = suspendFailureOf {
            transport.runReaderLoop {
                transport.read(ByteArray(1), 0, 1)
            }
        }

        assertSame(error, failure)
        assertEquals(0, transport.diagnostics().activeReads)
    }

    @Test
    fun readerLoopSetupFailureTerminatesSessionInsteadOfLeavingItOpen() = runBlocking {
        val closeCount = AtomicInteger(0)
        val transport = object : YamuxPhysicalTransport {
            override suspend fun runReaderLoop(block: suspend () -> Unit) {
                throw IOException("reader setup detail marker")
            }

            override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int =
                throw AssertionError("read must not run")

            override suspend fun writeFully(source: ByteArray, offset: Int, length: Int) = Unit

            override fun close() {
                closeCount.incrementAndGet()
            }
        }
        val parentScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val session = createYamuxClientSession(
            transport = transport,
            parentScope = parentScope,
            config = YamuxConfig(keepAliveEnabled = false),
        )
        try {
            val failure = withTimeout(5_000L) { suspendFailureOf { session.awaitTermination() } }
            assertTrue(failure is YamuxTransportFailure)
            assertTrue(session.diagnostics().terminated)
            assertEquals(1, closeCount.get())
        } finally {
            suspendFailureOf { session.close() }
            parentScope.cancel()
        }
    }

    @Test
    fun productionReaderLoopKeepsTransportOpenLongEnoughToWriteProtocolGoAway() = runBlocking {
        val invalidHeader = ByteArray(YamuxProtocol.HEADER_SIZE).also { it[0] = 1 }
        val output = ByteArrayOutputStream()
        val transport = TcpTlsYamuxTransport(
            input = ByteArrayInputStream(invalidHeader),
            output = output,
            closeController = TransportCloseController(),
            dispatcher = Dispatchers.IO,
        )
        val shared = SharedCloseYamuxPhysicalTransport(transport)
        val parentScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val session = createYamuxClientSession(
            transport = shared,
            parentScope = parentScope,
            config = YamuxConfig(keepAliveEnabled = false),
        )
        try {
            val failure = withTimeout(5_000L) { suspendFailureOf { session.awaitTermination() } }
            assertTrue(failure is YamuxProtocolFailure)
            val goAway = YamuxFrameCodec.decodeHeader(output.toByteArray())
            assertEquals(YamuxFrameType.GOAWAY, goAway.type)
            assertEquals(YamuxGoAwayCode.PROTOCOL_ERROR.wireValue, goAway.length)
            assertTrue(transport.diagnostics().closed)
        } finally {
            suspendFailureOf { session.close() }
            parentScope.cancel()
        }
    }

    @Test
    fun muxAwaitTerminationWaitsForSharedPhysicalCloseCompletion() = runBlocking {
        val parentScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val transport = BarrierClosingPhysicalTransport()
        val sharedTransport = SharedCloseYamuxPhysicalTransport(transport)
        val session = createYamuxClientSession(
            transport = sharedTransport,
            parentScope = parentScope,
            config = YamuxConfig(keepAliveEnabled = false),
        )
        val mux = YamuxMuxConnection(session, sharedTransport, parentScope)
        try {
            withTimeout(5_000L) { transport.readStarted.await() }

            val closeCall = async(Dispatchers.IO) { mux.close() }
            assertTrue(withContext(Dispatchers.IO) { transport.closeStarted.await(5, TimeUnit.SECONDS) })
            assertEquals(1, transport.closeCount.get())
            assertFalse(closeCall.isCompleted)
            val termination = async { mux.awaitTermination() }
            yield()

            assertFalse(termination.isCompleted)
            transport.releaseClose.countDown()
            assertEquals(null, withTimeout(5_000L) { suspendFailureOf { closeCall.await() } })
            assertEquals(null, withTimeout(5_000L) { suspendFailureOf { termination.await() } })
            assertEquals(YamuxTermination.Closed, withTimeout(5_000L) { session.awaitTermination() })
            assertEquals(1, transport.closeCount.get())
        } finally {
            transport.releaseClose.countDown()
            mux.close()
            parentScope.cancel()
        }
    }

    @Test
    fun muxCloseSynchronouslyLinearizesLogicalAndPhysicalCloseWithQueuedDispatcher() = runTest {
        val queuedDispatcher = StandardTestDispatcher(testScheduler)
        val parentScope = CoroutineScope(SupervisorJob() + queuedDispatcher)
        val transport = BarrierClosingPhysicalTransport()
        val sharedTransport = SharedCloseYamuxPhysicalTransport(transport)
        val session = createYamuxClientSession(
            transport = sharedTransport,
            parentScope = parentScope,
            config = YamuxConfig(keepAliveEnabled = false),
        )
        val mux = YamuxMuxConnection(session, sharedTransport, parentScope)
        val closeFailure = AtomicReference<Throwable?>()
        val closeReturned = CountDownLatch(1)
        val closeThread = Thread(
            {
                closeFailure.set(runCatching { mux.close() }.exceptionOrNull())
                closeReturned.countDown()
            },
            "frp-mux-close-fixture",
        )
        try {
            closeThread.start()
            assertTrue(withContext(Dispatchers.IO) { transport.closeStarted.await(5, TimeUnit.SECONDS) })

            assertTrue(session.diagnostics().ending)
            assertEquals(1, transport.closeCount.get())
            assertSame(closeThread, transport.closeThread.get())
            assertEquals(1L, closeReturned.count)

            transport.releaseClose.countDown()
            assertTrue(withContext(Dispatchers.IO) { closeReturned.await(5, TimeUnit.SECONDS) })
            withContext(Dispatchers.IO) { closeThread.join(5_000L) }

            assertFalse(closeThread.isAlive)
            assertEquals(null, closeFailure.get())
            assertEquals(1, transport.closeCount.get())
            advanceUntilIdle()
            assertEquals(null, suspendFailureOf { mux.awaitTermination() })
        } finally {
            transport.releaseClose.countDown()
            advanceUntilIdle()
            parentScope.cancel()
        }
    }

    @Test
    fun muxForceTerminationAfterPhysicalCloseUnblocksQueuedLogicalCleanup() = runTest {
        val queuedDispatcher = StandardTestDispatcher(testScheduler)
        val parentScope = CoroutineScope(SupervisorJob() + queuedDispatcher)
        val transport = BarrierClosingPhysicalTransport()
        val sharedTransport = SharedCloseYamuxPhysicalTransport(transport)
        val session = createYamuxClientSession(
            transport = sharedTransport,
            parentScope = parentScope,
            config = YamuxConfig(keepAliveEnabled = false),
        )
        val mux = YamuxMuxConnection(session, sharedTransport, parentScope)
        val closeFailure = AtomicReference<Throwable?>()
        val closeReturned = CountDownLatch(1)
        val closeThread = Thread(
            {
                closeFailure.set(runCatching { mux.close() }.exceptionOrNull())
                closeReturned.countDown()
            },
            "frp-mux-force-close-fixture",
        )
        try {
            closeThread.start()
            assertTrue(transport.closeStarted.await(5, TimeUnit.SECONDS))
            transport.releaseClose.countDown()
            assertTrue(closeReturned.await(5, TimeUnit.SECONDS))
            closeThread.join(5_000L)
            assertEquals(null, closeFailure.get())

            val sessionTermination = async(start = CoroutineStart.UNDISPATCHED) {
                session.awaitTermination()
            }
            val muxTermination = async(start = CoroutineStart.UNDISPATCHED) {
                mux.awaitTermination()
            }
            mux.forceTerminationAfterClose()
            assertFalse(sessionTermination.isCompleted)
            assertFalse(muxTermination.isCompleted)
            advanceUntilIdle()
            assertEquals(YamuxTermination.Closed, sessionTermination.await())
            muxTermination.await()
            assertEquals(YamuxTermination.Closed, session.awaitTermination())
            assertEquals(null, suspendFailureOf { mux.awaitTermination() })
            assertEquals(1, transport.closeCount.get())
        } finally {
            transport.releaseClose.countDown()
            mux.forceTerminationAfterClose()
            advanceUntilIdle()
            parentScope.cancel()
        }
    }

    @Test
    fun explicitMuxClosePreservesPhysicalJvmError() = runBlocking {
        val error = SyntheticTransportError()
        val parentScope = CoroutineScope(
            SupervisorJob() + Dispatchers.Default + CoroutineExceptionHandler { _, _ -> },
        )
        val transport = ErrorClosingPhysicalTransport(error)
        val sharedTransport = SharedCloseYamuxPhysicalTransport(transport)
        val session = createYamuxClientSession(
            transport = sharedTransport,
            parentScope = parentScope,
            config = YamuxConfig(keepAliveEnabled = false),
        )
        val mux = YamuxMuxConnection(session, sharedTransport, parentScope)
        try {
            withTimeout(5_000L) { transport.readStarted.await() }

            assertSame(error, runCatching { mux.close() }.exceptionOrNull())
            assertSame(error, withTimeout(5_000L) { suspendFailureOf { mux.awaitTermination() } })
            assertSame(error, withTimeout(5_000L) { suspendFailureOf { mux.awaitTermination() } })
            assertSame(error, runCatching { mux.close() }.exceptionOrNull())
            assertEquals(1, transport.closeCount.get())
            assertFalse(mux.toString().contains(error.message.orEmpty()))
        } finally {
            parentScope.cancel()
        }
    }

    @Test
    fun explicitMuxClosePersistsPhysicalCancellationForAwaitersAndCallers() = runBlocking {
        val cancellation = CancellationException("physical close cancellation marker")
        val parentScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val transport = CancellationClosingPhysicalTransport(cancellation)
        val sharedTransport = SharedCloseYamuxPhysicalTransport(transport)
        val session = createYamuxClientSession(
            transport = sharedTransport,
            parentScope = parentScope,
            config = YamuxConfig(keepAliveEnabled = false),
        )
        val mux = YamuxMuxConnection(session, sharedTransport, parentScope)
        try {
            withTimeout(5_000L) { transport.readStarted.await() }

            assertOriginalCancellation(cancellation, runCatching { mux.close() }.exceptionOrNull())
            val awaitFailure = withTimeout(5_000L) { suspendFailureOf { mux.awaitTermination() } }
            val closeFailure = runCatching { mux.close() }.exceptionOrNull()

            assertOriginalCancellation(cancellation, awaitFailure)
            assertOriginalCancellation(cancellation, closeFailure)
            assertEquals(1, transport.closeCount.get())
        } finally {
            parentScope.cancel()
        }
    }

    @Test
    fun sharedCloseControllerClosesEachAttachedLayerOnlyOnce() {
        val controller = TransportCloseController()
        val socket = TestSocket()
        val tls = TestTlsSocket()
        controller.attachSocket(socket)
        controller.attachTlsSocket(tls)

        controller.close()
        controller.close()

        assertEquals(1, tls.closeCount.get())
        assertEquals(1, socket.closeCount.get())
        assertTrue(controller.isClosed)

        val lateSocket = TestSocket()
        val failure = assertThrows(FrpTransportException::class.java) {
            controller.attachSocket(lateSocket)
        }
        assertEquals(FrpTransportFailure.CLOSED, failure.failure)
        assertEquals(1, lateSocket.closeCount.get())
    }

    @Test
    fun closeControllerClosesRawSocketBeforeTlsWrapper() {
        val rawClosed = CountDownLatch(1)
        val closeOrder = mutableListOf<String>()
        val socket = RawCloseReleasingSocket(rawClosed, closeOrder)
        val tls = RawCloseAwaitingTlsSocket(rawClosed, closeOrder)
        val controller = TransportCloseController()
        controller.attachSocket(socket)
        controller.attachTlsSocket(tls)

        controller.close()

        assertEquals(listOf("socket", "tls"), closeOrder)
        assertEquals(1, socket.closeCount.get())
        assertEquals(1, tls.closeCount.get())
        assertTrue(controller.isClosed)
    }

    @Test
    fun closeControllerPreservesJvmErrorAheadOfCancellation() {
        val cancellation = CancellationException("close cancellation marker")
        val error = SyntheticTransportError()
        val controller = TransportCloseController()
        val socket = TestSocket(closeFailure = error)
        val tls = TestTlsSocket(closeFailure = cancellation)
        controller.attachSocket(socket)
        controller.attachTlsSocket(tls)

        val failure = runCatching { controller.close() }.exceptionOrNull()

        assertSame(error, failure)
        assertTrue(error.suppressed.any { it === cancellation })
        assertEquals(1, tls.closeCount.get())
        assertEquals(1, socket.closeCount.get())
    }

    @Test
    fun concurrentCloseWaitsForOwnerAndReplaysItsJvmError() {
        val error = SyntheticTransportError()
        val controller = TransportCloseController()
        val socket = BarrierErrorCloseSocket(error)
        controller.attachSocket(socket)
        val ownerFailure = AtomicReference<Throwable?>()
        val followerFailure = AtomicReference<Throwable?>()
        val followerReturned = CountDownLatch(1)
        val owner = Thread(
            { ownerFailure.set(runCatching { controller.close() }.exceptionOrNull()) },
            "frp-close-owner-fixture",
        )
        val follower = Thread(
            {
                followerFailure.set(runCatching { controller.close() }.exceptionOrNull())
                followerReturned.countDown()
            },
            "frp-close-follower-fixture",
        )

        owner.start()
        assertTrue(socket.closeStarted.await(5, TimeUnit.SECONDS))
        follower.start()
        assertFalse(followerReturned.await(100, TimeUnit.MILLISECONDS))

        socket.releaseClose.countDown()
        owner.join(5_000L)
        follower.join(5_000L)

        assertFalse(owner.isAlive)
        assertFalse(follower.isAlive)
        assertSame(error, ownerFailure.get())
        assertSame(error, followerFailure.get())
        assertEquals(1, socket.closeCount.get())
        assertSame(error, runCatching { controller.close() }.exceptionOrNull())
    }

    @Test
    fun closeOwnerCanReenterWithoutWaitingForItself() {
        val controller = TransportCloseController()
        val socket = ReentrantCloseSocket(controller)
        controller.attachSocket(socket)
        val failure = AtomicReference<Throwable?>()
        val thread = Thread(
            { failure.set(runCatching { controller.close() }.exceptionOrNull()) },
            "frp-reentrant-close-fixture",
        )

        thread.start()
        thread.join(5_000L)

        assertFalse(thread.isAlive)
        assertEquals(null, failure.get())
        assertEquals(1, socket.closeCount.get())
        assertTrue(controller.isClosed)
    }

    @Test
    fun blockingStreamCloseResetsOnceAndRejectsLaterIo() = runTest {
        val stream = RecordingMuxStream(readBytes = byteArrayOf(1, 2, 3))
        val adapter = YamuxStreamBlockingIo(stream, this, BlockingIoThreadGuard {})
        val destination = ByteArray(3)

        assertEquals(3, adapter.input.read(destination))
        adapter.output.write(byteArrayOf(4, 5))
        adapter.close()
        adapter.close()
        advanceUntilIdle()
        adapter.awaitReset()

        assertEquals(1, stream.resetCount.get())
        assertEquals(listOf<Byte>(4, 5), stream.written.toList())
        assertTrue(adapter.isClosed)
        val readFailure = assertThrows(FrpTransportException::class.java) {
            adapter.input.read(ByteArray(1))
        }
        assertEquals(FrpTransportFailure.CLOSED, readFailure.failure)
        val writeFailure = assertThrows(FrpTransportException::class.java) {
            adapter.output.write(1)
        }
        assertEquals(FrpTransportFailure.CLOSED, writeFailure.failure)
    }

    @Test
    fun blockingStreamResetPreservesJvmErrorForAwaiter() = runTest {
        val error = SyntheticTransportError()
        val stream = RecordingMuxStream(resetFailure = error)
        val adapter = YamuxStreamBlockingIo(stream, this, BlockingIoThreadGuard {})

        adapter.close()
        advanceUntilIdle()

        assertSame(error, suspendFailureOf { adapter.awaitReset() })
    }

    @Test
    fun mainThreadGuardRejectsBlockingAdapters() {
        val failure = AtomicReference<Throwable?>()
        val thread = Thread(
            { failure.set(runCatching { NoMainThreadBlockingIo.check() }.exceptionOrNull()) },
            "main",
        )

        thread.start()
        thread.join(5_000L)

        assertFalse(thread.isAlive)
        assertTrue(failure.get() is IllegalStateException)
    }

    private suspend fun suspendFailureOf(block: suspend () -> Unit): Throwable? =
        try {
            block()
            null
        } catch (failure: Throwable) {
            failure
        }

    private suspend fun newTimeoutCancellation(): TimeoutCancellationException = try {
        withTimeout(1L) { awaitCancellation() }
    } catch (failure: TimeoutCancellationException) {
        failure
    }

    private fun assertOriginalCancellation(expected: CancellationException, actual: Throwable?) {
        assertTrue(actual is CancellationException)
        val expectedChain = generateSequence(expected as Throwable?) { it.cause }.toList()
        val actualChain = generateSequence(actual) { it.cause }.toList()
        assertTrue(actualChain.any { candidate -> expectedChain.any { it === candidate } })
    }

    private class TestSocket(
        private val connectFailure: Throwable? = null,
        private val blockConnectUntilClose: Boolean = false,
        private val blockReadUntilClose: Boolean = false,
        private val closeFailure: Throwable? = null,
    ) : Socket() {
        val closeCount = AtomicInteger(0)
        val connectStarted = CountDownLatch(1)
        val readStarted = CountDownLatch(1)
        var connectedEndpoint: SocketAddress? = null
            private set
        private val closedSignal = CountDownLatch(1)
        private val input = object : InputStream() {
            override fun read(): Int {
                readStarted.countDown()
                if (blockReadUntilClose) closedSignal.await()
                return -1
            }

            override fun read(destination: ByteArray, offset: Int, length: Int): Int = read()
        }
        private val output = ByteArrayOutputStream()

        fun writtenBytes(): ByteArray = output.toByteArray()

        override fun connect(endpoint: SocketAddress?, timeout: Int) {
            connectStarted.countDown()
            connectedEndpoint = endpoint
            connectFailure?.let { throw it }
            if (blockConnectUntilClose) {
                closedSignal.await()
                throw SocketException("closed")
            }
        }

        override fun setTcpNoDelay(on: Boolean) = Unit

        override fun setKeepAlive(on: Boolean) = Unit

        override fun getInputStream(): InputStream = input

        override fun getOutputStream(): OutputStream = output

        override fun close() {
            if (closeCount.incrementAndGet() == 1) {
                closedSignal.countDown()
                closeFailure?.let { throw it }
            }
        }
    }

    private class TestTlsSocket(
        private val handshakeFailure: Throwable? = null,
        private val blockHandshakeUntilClose: Boolean = false,
        private val closeFailure: Throwable? = null,
    ) : TlsClientSocket {
        val closeCount = AtomicInteger(0)
        val handshakeStarted = CountDownLatch(1)
        private val closedSignal = CountDownLatch(1)
        override val inputStream: InputStream = ByteArrayInputStream(ByteArray(0))
        override val outputStream: OutputStream = ByteArrayOutputStream()

        override fun startHandshake() {
            handshakeStarted.countDown()
            handshakeFailure?.let { throw it }
            if (blockHandshakeUntilClose) {
                closedSignal.await()
                throw SocketException("closed")
            }
        }

        override fun close() {
            if (closeCount.incrementAndGet() == 1) {
                closedSignal.countDown()
                closeFailure?.let { throw it }
            }
        }
    }

    private class RawCloseReleasingSocket(
        private val rawClosed: CountDownLatch,
        private val closeOrder: MutableList<String>,
    ) : Socket() {
        val closeCount = AtomicInteger(0)

        override fun close() {
            if (closeCount.incrementAndGet() == 1) {
                closeOrder += "socket"
                rawClosed.countDown()
            }
        }
    }

    private class RawCloseAwaitingTlsSocket(
        private val rawClosed: CountDownLatch,
        private val closeOrder: MutableList<String>,
    ) : TlsClientSocket {
        val closeCount = AtomicInteger(0)
        override val inputStream: InputStream = ByteArrayInputStream(ByteArray(0))
        override val outputStream: OutputStream = ByteArrayOutputStream()

        override fun startHandshake() = Unit

        override fun close() {
            if (closeCount.incrementAndGet() == 1) {
                closeOrder += "tls"
                check(rawClosed.await(1, TimeUnit.SECONDS)) {
                    "raw socket was not closed before the TLS wrapper"
                }
            }
        }
    }

    private class BarrierErrorCloseSocket(
        private val error: Error,
    ) : Socket() {
        val closeStarted = CountDownLatch(1)
        val releaseClose = CountDownLatch(1)
        val closeCount = AtomicInteger(0)

        override fun close() {
            closeCount.incrementAndGet()
            closeStarted.countDown()
            releaseClose.await()
            throw error
        }
    }

    private class ReentrantCloseSocket(
        private val controller: TransportCloseController,
    ) : Socket() {
        val closeCount = AtomicInteger(0)

        override fun close() {
            closeCount.incrementAndGet()
            controller.close()
        }
    }

    private class RejectingDispatcher(private val failure: Throwable) : CoroutineDispatcher() {
        override fun dispatch(context: CoroutineContext, block: Runnable) {
            throw failure
        }
    }

    private class QueueingDispatcher : CoroutineDispatcher() {
        private val tasks = LinkedBlockingQueue<Runnable>()
        val queued = CountDownLatch(1)
        val dispatchCount = AtomicInteger(0)

        override fun dispatch(context: CoroutineContext, block: Runnable) {
            dispatchCount.incrementAndGet()
            tasks.add(block)
            queued.countDown()
        }

        fun take(): Runnable = tasks.poll(5, TimeUnit.SECONDS)
            ?: throw AssertionError("Blocking operation was not dispatched")
    }

    private class ZeroInputStream : InputStream() {
        override fun read(): Int = 0

        override fun read(destination: ByteArray, offset: Int, length: Int): Int = 0
    }

    private class BarrierClosingPhysicalTransport : YamuxPhysicalTransport {
        val readStarted = CompletableDeferred<Unit>()
        val closeStarted = CountDownLatch(1)
        val releaseClose = CountDownLatch(1)
        val closeCount = AtomicInteger(0)
        val closeThread = AtomicReference<Thread?>()
        private val closeSignal = CompletableDeferred<Unit>()

        override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int {
            readStarted.complete(Unit)
            closeSignal.await()
            throw IOException("physical read detail marker")
        }

        override suspend fun writeFully(source: ByteArray, offset: Int, length: Int) = Unit

        override fun close() {
            closeSignal.complete(Unit)
            closeCount.incrementAndGet()
            closeThread.compareAndSet(null, Thread.currentThread())
            closeStarted.countDown()
            releaseClose.await()
        }
    }

    private class ErrorClosingPhysicalTransport(
        private val error: Error,
    ) : YamuxPhysicalTransport {
        val readStarted = CompletableDeferred<Unit>()
        val closeCount = AtomicInteger(0)
        private val closeSignal = CompletableDeferred<Unit>()

        override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int {
            readStarted.complete(Unit)
            closeSignal.await()
            throw IOException("physical read detail marker")
        }

        override suspend fun writeFully(source: ByteArray, offset: Int, length: Int) = Unit

        override fun close() {
            closeSignal.complete(Unit)
            if (closeCount.incrementAndGet() == 1) throw error
        }
    }

    private class CancellationClosingPhysicalTransport(
        private val cancellation: CancellationException,
    ) : YamuxPhysicalTransport {
        val readStarted = CompletableDeferred<Unit>()
        val closeCount = AtomicInteger(0)
        private val closeSignal = CompletableDeferred<Unit>()

        override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int {
            readStarted.complete(Unit)
            closeSignal.await()
            throw IOException("physical read detail marker")
        }

        override suspend fun writeFully(source: ByteArray, offset: Int, length: Int) = Unit

        override fun close() {
            closeSignal.complete(Unit)
            if (closeCount.incrementAndGet() == 1) throw cancellation
        }
    }

    private class RecordingMuxStream(
        private val readBytes: ByteArray = ByteArray(0),
        private val resetFailure: Throwable? = null,
    ) : FrpMuxStream {
        private var read = false
        val resetCount = AtomicInteger(0)
        val written = mutableListOf<Byte>()

        override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int {
            if (read) return -1
            read = true
            val count = minOf(length, readBytes.size)
            readBytes.copyInto(destination, offset, 0, count)
            return count
        }

        override suspend fun write(source: ByteArray, offset: Int, length: Int) {
            repeat(length) { index -> written += source[offset + index] }
        }

        override suspend fun closeWrite() = Unit

        override suspend fun reset() {
            resetCount.incrementAndGet()
            resetFailure?.let { throw it }
        }
    }

    private class SyntheticTransportError : Error("synthetic transport error marker")
}
