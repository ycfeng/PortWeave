package dev.portweave.tunnel.engine.ssh

import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.SshAuthMethod
import dev.portweave.core.model.SshServerIdentity
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.tunnel.engine.TunnelErrorCode
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.file.Files
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import dev.portweave.tunnel.engine.RuntimeWorkPolicy
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.bouncycastle.openssl.PKCS8Generator
import org.bouncycastle.openssl.jcajce.JcaPEMWriter
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder
import org.apache.sshd.common.config.keys.KeyUtils
import org.apache.sshd.server.SshServer
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator
import org.apache.sshd.server.forward.AcceptAllForwardingFilter
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class SshTunnelIntegrationTest {
    @Test
    fun serverSideDisconnectIsReportedImmediatelyWithoutPolling() = withHarness { harness ->
        runBlocking {
            val config = harness.config(SshAuthMethod.PASSWORD)
            val pin = harness.backend.discoverHostKey(config).sha256Fingerprint
            val policy = RuntimeWorkPolicy()
            val credentials = SshCredentials(Password.toByteArray(), hostFingerprint = pin.toByteArray())
            val handle = try {
                SshTunnelBackend(policy).start(config, credentials)
            } finally {
                credentials.close()
            }
            try {
                val lost = async(Dispatchers.Default) { handle.awaitConnectionLost() }
                delay(300)
                assertTrue(lost.isActive)
                val droppedAt = System.nanoTime()
                harness.dropSessions()
                withTimeout(10_000) { lost.await() }
                // An idle background tunnel used to be polled every 30s; the closed socket reports itself.
                assertTrue((System.nanoTime() - droppedAt) / 1_000_000 < 2_000)
                assertEquals(1L, policy.counters.snapshot()["sshStateChecks"])
                assertTrue(!handle.isConnected)
            } finally {
                handle.stop()
            }
        }
    }

    @Test
    fun reconnectRebindsSamePortAfterClosingAnActiveClient() = withHarness { harness ->
        runBlocking {
            val config = harness.config(SshAuthMethod.PASSWORD)
            val pin = harness.backend.discoverHostKey(config).sha256Fingerprint
            repeat(3) {
                val credentials = SshCredentials(Password.toByteArray(), hostFingerprint = pin.toByteArray())
                val handle = try {
                    harness.backend.start(config, credentials)
                } finally {
                    credentials.close()
                }
                try {
                    Socket("127.0.0.1", config.localPort).use { client ->
                        client.soTimeout = 5_000
                        client.getOutputStream().write(42)
                        assertEquals(42, client.getInputStream().read())
                        // The listener side closes first, as during network recovery.
                        handle.stop()
                        assertEquals(-1, client.getInputStream().read())
                    }
                } finally {
                    handle.stop()
                }
            }
        }
    }

    @Test
    fun passwordAuthenticationCarriesTcpAndReleasesLocalPort() = withHarness { harness ->
        runBlocking {
            val config = harness.config(SshAuthMethod.PASSWORD)
            val discovered = harness.backend.discoverHostKey(config)
            val credentials = SshCredentials(
                password = Password.toByteArray(),
                hostFingerprint = discovered.sha256Fingerprint.toByteArray(),
            )

            val handle = try {
                harness.backend.start(config, credentials)
            } finally {
                credentials.close()
            }
            val payload = ByteArray(128 * 1024) { index -> (index % 251).toByte() }
            assertArrayEquals(payload, roundTrip(handle.localEndpoint.port, payload))
            assertTrue(handle.capabilities.byteCounters)
            assertTrue(handle.capabilities.activeConnectionCount)
            assertTrue(handle.capabilities.handshakeLatency)
            // Byte snapshots are coalesced; stopping must flush the exact final counters.
            handle.stop()
            assertTrue((handle.metrics.value.bytesUploaded ?: 0) >= payload.size)
            assertTrue((handle.metrics.value.bytesDownloaded ?: 0) >= payload.size)
            assertTrue((handle.metrics.value.handshakeLatencyMillis ?: -1) >= 0)
            handle.stop()

            ServerSocket(config.localPort, 1, InetAddress.getByName("127.0.0.1")).use { }
        }
    }

    @Test
    fun oneHundredConcurrentClientsAndTcpHalfCloseRemainCorrect() = withHarness { harness ->
        runBlocking {
            val config = harness.config(SshAuthMethod.PASSWORD)
            val pin = harness.backend.discoverHostKey(config).sha256Fingerprint
            val credentials = SshCredentials(
                password = Password.toByteArray(),
                hostFingerprint = pin.toByteArray(),
            )
            val handle = try {
                harness.backend.start(config, credentials)
            } finally {
                credentials.close()
            }
            val clientCount = 100
            val latch = CountDownLatch(clientCount)
            val failures = CopyOnWriteArrayList<Throwable>()
            repeat(clientCount) { index ->
                thread(isDaemon = true) {
                    runCatching {
                        val payload = "client-$index-${"x".repeat(4_096)}".toByteArray()
                        assertArrayEquals(payload, roundTrip(handle.localEndpoint.port, payload))
                    }.onFailure(failures::add)
                    latch.countDown()
                }
            }
            check(latch.await(30, TimeUnit.SECONDS)) { "Concurrent clients timed out" }
            if (failures.isNotEmpty()) throw AssertionError("Concurrent forwarding failed", failures.first())

            val halfClosedPayload = "half-close".repeat(1_024).toByteArray()
            val echoed = Socket("127.0.0.1", handle.localEndpoint.port).use { socket ->
                socket.soTimeout = 5_000
                socket.getOutputStream().write(halfClosedPayload)
                socket.shutdownOutput()
                ByteArray(halfClosedPayload.size).also { received ->
                    var offset = 0
                    while (offset < received.size) {
                        val read = socket.getInputStream().read(received, offset, received.size - offset)
                        check(read > 0) { "Half-closed connection ended before echo completed" }
                        offset += read
                    }
                    check(socket.getInputStream().read() == -1) { "Expected remote half-close" }
                }
            }
            assertArrayEquals(halfClosedPayload, echoed)
            handle.stop()
        }
    }

    @Test
    fun rsaPrivateKeyAuthenticationCarriesTcp() = withHarness { harness ->
        runBlocking {
            val config = harness.config(SshAuthMethod.PRIVATE_KEY)
            val discovered = harness.backend.discoverHostKey(config)
            val credentials = SshCredentials(
                privateKey = harness.privateKeyPem(harness.rsaKeyPair),
                hostFingerprint = discovered.sha256Fingerprint.toByteArray(),
            )
            val handle = try {
                harness.backend.start(config, credentials)
            } finally {
                credentials.close()
            }

            assertArrayEquals("private-key-echo".toByteArray(), roundTrip(handle.localEndpoint.port, "private-key-echo".toByteArray()))
            handle.stop()
        }
    }

    @Test
    fun ed25519PrivateKeyAuthenticationCarriesTcp() = withHarness { harness ->
        runBlocking {
            val config = harness.config(SshAuthMethod.PRIVATE_KEY)
            val pin = harness.backend.discoverHostKey(config).sha256Fingerprint
            val credentials = SshCredentials(
                privateKey = harness.privateKeyPem(harness.ed25519KeyPair),
                hostFingerprint = pin.toByteArray(),
            )
            val handle = try {
                harness.backend.start(config, credentials)
            } finally {
                credentials.close()
            }
            val payload = "ed25519-echo".toByteArray()
            assertArrayEquals(payload, roundTrip(handle.localEndpoint.port, payload))
            handle.stop()
        }
    }

    @Test
    fun encryptedPrivateKeyAcceptsCorrectAndRejectsWrongPassphrase() = withHarness { harness ->
        runBlocking {
            val config = harness.config(SshAuthMethod.PRIVATE_KEY)
            val pin = harness.backend.discoverHostKey(config).sha256Fingerprint
            val encrypted = harness.encryptedPrivateKeyPem(harness.rsaKeyPair, "correct-passphrase")
            val accepted = SshCredentials(
                privateKey = encrypted.copyOf(),
                passphrase = "correct-passphrase".toByteArray(),
                hostFingerprint = pin.toByteArray(),
            )
            val handle = try {
                harness.backend.start(config, accepted)
            } finally {
                accepted.close()
            }
            handle.stop()

            val wrong = SshCredentials(
                privateKey = encrypted,
                passphrase = "wrong-passphrase".toByteArray(),
                hostFingerprint = pin.toByteArray(),
            )
            val failure = assertThrows(SshBackendException::class.java) {
                runBlocking { harness.backend.start(config, wrong) }
            }
            wrong.close()
            assertEquals(TunnelErrorCode.SSH_PASSPHRASE_INVALID, failure.tunnelError.code)
            assertEquals("ssh_passphrase_invalid", failure.tunnelError.messageKey)
            ServerSocket(config.localPort, 1, InetAddress.getByName("127.0.0.1")).use { }
        }
    }

    @Test
    fun encryptedPrivateKeyRequiresPassphraseBeforeAuthentication() = withHarness { harness ->
        runBlocking {
            val config = harness.config(SshAuthMethod.PRIVATE_KEY)
            val pin = harness.backend.discoverHostKey(config).sha256Fingerprint
            val encrypted = harness.encryptedPrivateKeyPem(harness.rsaKeyPair, "correct-passphrase")
            listOf(null, byteArrayOf()).forEach { passphrase ->
                SshCredentials(
                    privateKey = encrypted.copyOf(),
                    passphrase = passphrase,
                    hostFingerprint = pin.toByteArray(),
                ).use { credentials ->
                    val failure = assertThrows(SshBackendException::class.java) {
                        runBlocking { harness.backend.start(config, credentials) }
                    }
                    assertEquals(TunnelErrorCode.SSH_PASSPHRASE_REQUIRED, failure.tunnelError.code)
                    assertEquals("ssh_passphrase_required", failure.tunnelError.messageKey)
                }
            }
            encrypted.fill(0)
            ServerSocket(config.localPort, 1, InetAddress.getByName("127.0.0.1")).use { }
        }
    }

    @Test
    fun invalidPrivateKeyIsNotReportedAsServerRejection() = withHarness { harness ->
        runBlocking {
            val config = harness.config(SshAuthMethod.PRIVATE_KEY)
            val pin = harness.backend.discoverHostKey(config).sha256Fingerprint
            SshCredentials(
                privateKey = "not-a-private-key".toByteArray(),
                hostFingerprint = pin.toByteArray(),
            ).use { credentials ->
                val failure = assertThrows(SshBackendException::class.java) {
                    runBlocking { harness.backend.start(config, credentials) }
                }
                assertEquals(TunnelErrorCode.SSH_PRIVATE_KEY_INVALID, failure.tunnelError.code)
                assertEquals("ssh_private_key_invalid", failure.tunnelError.messageKey)
            }
            ServerSocket(config.localPort, 1, InetAddress.getByName("127.0.0.1")).use { }
        }
    }

    @Test
    fun serverRejectionIdentifiesTheAuthenticationMethod() = withHarness { harness ->
        runBlocking {
            listOf(SshAuthMethod.PRIVATE_KEY, SshAuthMethod.PASSWORD).forEach { method ->
                val base = harness.config(method)
                val config = base.copy(spec = (base.spec as TunnelSpec.Ssh).copy(username = "wrong-user"))
                val pin = harness.backend.discoverHostKey(config).sha256Fingerprint
                SshCredentials(
                    password = if (method == SshAuthMethod.PASSWORD) Password.toByteArray() else null,
                    privateKey = if (method == SshAuthMethod.PRIVATE_KEY) harness.privateKeyPem(harness.rsaKeyPair) else null,
                    hostFingerprint = pin.toByteArray(),
                ).use { credentials ->
                    val failure = assertThrows(SshBackendException::class.java) {
                        runBlocking { harness.backend.start(config, credentials) }
                    }
                    assertEquals(TunnelErrorCode.SSH_AUTH_FAILED, failure.tunnelError.code)
                    assertEquals(
                        if (method == SshAuthMethod.PRIVATE_KEY) "ssh_public_key_rejected" else "ssh_password_rejected",
                        failure.tunnelError.messageKey,
                    )
                }
                ServerSocket(config.localPort, 1, InetAddress.getByName("127.0.0.1")).use { }
            }
        }
    }

    @Test
    fun realHandshakeRejectsMismatchedHostIdentityBeforeBinding() = withHarness { harness ->
        val config = harness.config(SshAuthMethod.PASSWORD)
        val wrongPin = SshHostKeyFingerprint.sha256("not-the-server-key".toByteArray())

        val failure = assertThrows(SshBackendException::class.java) {
            runBlocking {
                harness.backend.start(
                    config,
                    SshCredentials(Password.toByteArray(), hostFingerprint = wrongPin.toByteArray()),
                )
            }
        }

        assertEquals(TunnelErrorCode.SSH_HOST_KEY_MISMATCH, failure.tunnelError.code)
        ServerSocket(config.localPort, 1, InetAddress.getByName("127.0.0.1")).use { }
    }

    @Test
    fun unreachableRemoteTargetFailsOnlyTheClientConnection() = withHarness { harness ->
        runBlocking {
            val unavailablePort = ServerSocket(0, 1, InetAddress.getByName("127.0.0.1")).use { it.localPort }
            val base = harness.config(SshAuthMethod.PASSWORD)
            val config = base.copy(
                spec = (base.spec as TunnelSpec.Ssh).copy(remoteTargetPort = unavailablePort),
            )
            val pin = harness.backend.discoverHostKey(config).sha256Fingerprint
            val credentials = SshCredentials(
                password = Password.toByteArray(),
                hostFingerprint = pin.toByteArray(),
            )
            val handle = try {
                harness.backend.start(config, credentials)
            } finally {
                credentials.close()
            }

            Socket("127.0.0.1", handle.localEndpoint.port).use { socket ->
                socket.soTimeout = 5_000
                socket.getOutputStream().write(1)
                val result = runCatching { socket.getInputStream().read() }
                check(result.isFailure || result.getOrNull() == -1)
            }
            check(handle.isConnected) { "One unreachable target must not tear down the SSH session" }
            handle.stop()
        }
    }

    private fun <T> withHarness(block: (SshHarness) -> T): T = SshHarness().use(block)

    private fun roundTrip(port: Int, payload: ByteArray): ByteArray = Socket("127.0.0.1", port).use { socket ->
        socket.soTimeout = 5_000
        socket.getOutputStream().write(payload)
        socket.getOutputStream().flush()
        val received = ByteArray(payload.size)
        var offset = 0
        while (offset < received.size) {
            val read = socket.getInputStream().read(received, offset, received.size - offset)
            check(read > 0) { "Echo connection closed early" }
            offset += read
        }
        received
    }

    private companion object {
        const val Username = "portweave-test"
        const val Password = "stage-b-password"
    }
}

private class SshHarness : AutoCloseable {
    private val tempDirectory = Files.createTempDirectory("portweave-ssh-test")
    val rsaKeyPair: KeyPair = KeyPairGenerator.getInstance("RSA").apply { initialize(3072) }.generateKeyPair()
    val ed25519KeyPair: KeyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair()
    private val echo = EchoServer()
    private val ssh = SshServer.setUpDefaultServer().apply {
        host = "127.0.0.1"
        port = 0
        keyPairProvider = SimpleGeneratorHostKeyProvider(tempDirectory.resolve("host-key")).apply {
            algorithm = KeyUtils.RSA_ALGORITHM
            keySize = 3072
        }
        passwordAuthenticator = PasswordAuthenticator { username, password, _ ->
            username == "portweave-test" && password == "stage-b-password"
        }
        publickeyAuthenticator = PublickeyAuthenticator { username, key, _ ->
            username == "portweave-test" && listOf(rsaKeyPair, ed25519KeyPair)
                .any { key.encoded.contentEquals(it.public.encoded) }
        }
        forwardingFilter = AcceptAllForwardingFilter.INSTANCE
        start()
    }
    val backend = SshTunnelBackend()

    fun dropSessions() = ssh.activeSessions.forEach { it.close(true) }

    fun config(authMethod: SshAuthMethod): TunnelConfig {
        val localPort = ServerSocket(0, 1, InetAddress.getByName("127.0.0.1")).use { it.localPort }
        return TunnelConfig.empty(TunnelKind.SSH, "Integration", localPort, 1L).copy(
            autoRecover = false,
            spec = TunnelSpec.Ssh(
                endpoint = EndpointSource.Remote("127.0.0.1", ssh.port),
                identity = SshServerIdentity("integration.test", ssh.port),
                username = "portweave-test",
                authMethod = authMethod,
                remoteTargetHost = "127.0.0.1",
                remoteTargetPort = echo.port,
                connectTimeoutSeconds = 5,
                keepAliveSeconds = 5,
            ),
        )
    }

    fun privateKeyPem(keyPair: KeyPair): ByteArray {
        val payload = Base64.getMimeEncoder(64, "\n".toByteArray()).encodeToString(keyPair.private.encoded)
        return "-----BEGIN PRIVATE KEY-----\n$payload\n-----END PRIVATE KEY-----\n".toByteArray()
    }

    @Suppress("DEPRECATION")
    fun encryptedPrivateKeyPem(keyPair: KeyPair, passphrase: String): ByteArray {
        val encryptor = JceOpenSSLPKCS8EncryptorBuilder(PKCS8Generator.AES_256_CBC)
            .setRandom(SecureRandom())
            .setPasssword(passphrase.toCharArray())
            .build()
        val writer = java.io.StringWriter()
        JcaPEMWriter(writer).use { it.writeObject(JcaPKCS8Generator(keyPair.private, encryptor)) }
        return writer.toString().toByteArray()
    }

    override fun close() {
        runCatching { ssh.stop(false) }
        echo.close()
        tempDirectory.toFile().deleteRecursively()
    }
}

private class EchoServer : AutoCloseable {
    private val running = AtomicBoolean(true)
    private val server = ServerSocket(0, 128, InetAddress.getByName("127.0.0.1"))
    private val clients = CopyOnWriteArrayList<Socket>()
    val port: Int get() = server.localPort
    private val acceptThread = thread(name = "portweave-echo", isDaemon = true) {
        while (running.get()) {
            val socket = runCatching { server.accept() }.getOrNull() ?: break
            clients += socket
            thread(name = "portweave-echo-client", isDaemon = true) {
                socket.use { client ->
                    val buffer = ByteArray(8 * 1024)
                    while (true) {
                        val read = client.getInputStream().read(buffer)
                        if (read < 0) break
                        client.getOutputStream().write(buffer, 0, read)
                        client.getOutputStream().flush()
                    }
                }
                clients -= socket
            }
        }
    }

    override fun close() {
        running.set(false)
        runCatching { server.close() }
        clients.forEach { runCatching { it.close() } }
        acceptThread.join(2_000)
    }
}
