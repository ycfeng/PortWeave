package dev.portweave.tunnel.engine.ssh

import com.jcraft.jsch.HostKeyRepository
import com.jcraft.jsch.JSchException
import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.SshAuthMethod
import dev.portweave.core.model.SshServerIdentity
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.tunnel.engine.TunnelErrorCode
import java.util.concurrent.CopyOnWriteArrayList
import java.net.SocketTimeoutException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.Job
import kotlinx.coroutines.withContext
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class SshTunnelBackendTest {
    @Test
    fun cancelledCallerStillDisconnectsSession() = runBlocking {
        val key = "test-host-key".toByteArray()
        val session = RecordingSession("test", key)
        session.setHostKeyRepository(PinnedSshHostKeyRepository(SshHostKeyFingerprint.sha256(key)))
        session.connect(1)
        val handle = SshBackendHandle(session, 15432)
        val job = Job()
        runCatching {
            withContext(job) {
                job.cancel()
                handle.stop()
            }
        }
        assertFalse(session.isConnected)
        assertTrue("disconnect" in session.operations)
    }

    @Test
    fun discoveryPerformsNoAuthenticationAndReturnsPin() = runBlocking {
        val hostKey = "discover-host-key".toByteArray()
        val factory = RecordingFactory(hostKey)

        val discovered = SshTunnelBackend(factory).discoverHostKey(config())

        assertEquals(SshHostKeyFingerprint.sha256(hostKey), discovered.sha256Fingerprint)
        val operations = factory.sessions.single().operations
        assertFalse("password" in operations)
        assertFalse("identity" in operations)
        assertFalse("forward" in operations)
        assertTrue(operations.indexOf("repository") < operations.indexOf("connect"))
        assertTrue("config:kex" in operations)
        assertTrue("config:server_host_key" in operations)
    }

    @Test
    fun passwordAuthenticationBindsLoopbackOnlyAndStopsSession() = runBlocking {
        val hostKey = "password-host-key".toByteArray()
        val factory = RecordingFactory(hostKey)
        val backend = SshTunnelBackend(factory)
        val credentials = SshCredentials(
            password = "password".toByteArray(),
            hostFingerprint = SshHostKeyFingerprint.sha256(hostKey).toByteArray(),
        )

        val handle = backend.start(config(), credentials)
        handle.stop()

        val session = factory.sessions.single()
        assertTrue(session.operations.indexOf("repository") < session.operations.indexOf("password"))
        assertTrue(session.operations.indexOf("connect") < session.operations.indexOf("forward"))
        assertEquals("127.0.0.1", session.bindAddress)
        assertFalse(session.isConnected)
    }

    @Test
    fun privateKeyAuthenticationUsesBytesWithoutPassword() = runBlocking {
        val hostKey = "private-key-host".toByteArray()
        val factory = RecordingFactory(hostKey)
        val credentials = SshCredentials(
            privateKey = "private-key".toByteArray(),
            passphrase = "phrase".toByteArray(),
            hostFingerprint = SshHostKeyFingerprint.sha256(hostKey).toByteArray(),
        )

        SshTunnelBackend(factory).start(config(SshAuthMethod.PRIVATE_KEY), credentials).stop()

        assertTrue("identity" in factory.sessions.single().operations)
        assertFalse("password" in factory.sessions.single().operations)
    }

    @Test
    fun hostIdentityMismatchFailsBeforeForwarding() {
        val factory = RecordingFactory("actual-key".toByteArray())
        val credentials = SshCredentials(
            password = "password".toByteArray(),
            hostFingerprint = SshHostKeyFingerprint.sha256("different-key".toByteArray()).toByteArray(),
        )

        val failure = assertThrows(SshBackendException::class.java) {
            runBlocking { SshTunnelBackend(factory).start(config(), credentials) }
        }

        assertEquals(TunnelErrorCode.SSH_HOST_KEY_MISMATCH, failure.tunnelError.code)
        assertFalse("forward" in factory.sessions.single().operations)
    }

    @Test
    fun connectionFailuresKeepTheirSpecificReasonAndReleaseSession() {
        val cases = listOf(
            Triple(JSchException("invalid privatekey: sensitive input"), TunnelErrorCode.SSH_PRIVATE_KEY_INVALID, "ssh_private_key_invalid"),
            Triple(JSchException("Incorrect passphrase provided."), TunnelErrorCode.SSH_PASSPHRASE_INVALID, "ssh_passphrase_invalid"),
            Triple(JSchException("Algorithm negotiation fail: server proposal"), TunnelErrorCode.SSH_ALGORITHM_UNSUPPORTED, "ssh_algorithm_unsupported"),
            Triple(JSchException("Auth fail for methods 'publickey'"), TunnelErrorCode.SSH_AUTH_FAILED, "ssh_public_key_rejected"),
            Triple(JSchException("wrapped", JSchException("Auth fail")), TunnelErrorCode.SSH_AUTH_FAILED, "ssh_public_key_rejected"),
            Triple(JSchException("wrapped", SocketTimeoutException()), TunnelErrorCode.CONNECT_TIMEOUT, "connect_timeout"),
        )
        cases.forEach { (exception, code, messageKey) ->
            val hostKey = "failure-host".toByteArray()
            val factory = RecordingFactory(hostKey, connectFailure = exception)
            SshCredentials(
                privateKey = "private-key".toByteArray(),
                hostFingerprint = SshHostKeyFingerprint.sha256(hostKey).toByteArray(),
            ).use { credentials ->
                val failure = assertThrows(SshBackendException::class.java) {
                    runBlocking { SshTunnelBackend(factory).start(config(SshAuthMethod.PRIVATE_KEY), credentials) }
                }
                assertEquals(code, failure.tunnelError.code)
                assertEquals(messageKey, failure.tunnelError.messageKey)
                assertEquals(code == TunnelErrorCode.CONNECT_TIMEOUT, failure.tunnelError.retryable)
                assertFalse(failure.message.orEmpty().contains("sensitive input"))
                assertTrue("disconnect" in factory.sessions.single().operations)
                assertFalse("forward" in factory.sessions.single().operations)
            }
        }
    }

    @Test
    fun passwordRejectionDoesNotClaimPrivateKeyFailure() {
        val hostKey = "failure-host".toByteArray()
        val factory = RecordingFactory(hostKey, connectFailure = JSchException("Auth fail"))
        SshCredentials(
            password = "wrong-password".toByteArray(),
            hostFingerprint = SshHostKeyFingerprint.sha256(hostKey).toByteArray(),
        ).use { credentials ->
            val failure = assertThrows(SshBackendException::class.java) {
                runBlocking { SshTunnelBackend(factory).start(config(), credentials) }
            }
            assertEquals("ssh_password_rejected", failure.tunnelError.messageKey)
        }
    }

    @Test
    fun discoveryReportsNegotiationFailureInsteadOfForwardFailure() {
        val factory = RecordingFactory("host".toByteArray(), connectFailure = JSchException("Algorithm negotiation fail"))
        val failure = assertThrows(SshBackendException::class.java) {
            runBlocking { SshTunnelBackend(factory).discoverHostKey(config()) }
        }
        assertEquals(TunnelErrorCode.SSH_ALGORITHM_UNSUPPORTED, failure.tunnelError.code)
        assertTrue("disconnect" in factory.sessions.single().operations)
    }
}

private class RecordingFactory(private val hostKey: ByteArray, private val connectFailure: Exception? = null) : SshSessionFactory {
    val sessions = CopyOnWriteArrayList<RecordingSession>()
    override fun create(username: String, host: String, port: Int): SshSessionHandle =
        RecordingSession(host, hostKey, connectFailure).also(sessions::add)
}

private class RecordingSession(
    private val host: String,
    private val hostKey: ByteArray,
    private val connectFailure: Exception? = null,
) : SshSessionHandle {
    val operations = mutableListOf<String>()
    private var repository: HostKeyRepository? = null
    var bindAddress: String? = null
    override var isConnected: Boolean = false
        private set

    override fun setHostKeyRepository(repository: HostKeyRepository) {
        operations += "repository"
        this.repository = repository
    }
    override fun setConfig(key: String, value: String) { operations += "config:$key" }
    override fun setServerAliveInterval(intervalMillis: Int) { operations += "keepalive" }
    override fun addIdentity(name: String, privateKey: ByteArray, passphrase: ByteArray?) { operations += "identity" }
    override fun setPassword(password: ByteArray) { operations += "password" }
    override fun connect(timeoutMillis: Int) {
        operations += "connect"
        connectFailure?.let { throw it }
        if (repository?.check(host, hostKey) != HostKeyRepository.OK) throw JSchException("host rejected")
        isConnected = true
    }
    override fun setPortForwardingL(
        bindAddress: String,
        localPort: Int,
        remoteHost: String,
        remotePort: Int,
    ): Int {
        operations += "forward"
        this.bindAddress = bindAddress
        return localPort
    }
    override fun disconnect() {
        operations += "disconnect"
        isConnected = false
    }
}

private fun config(auth: SshAuthMethod = SshAuthMethod.PASSWORD): TunnelConfig = TunnelConfig.empty(
    TunnelKind.SSH,
    "SSH test",
    15432,
    1L,
).copy(
    spec = TunnelSpec.Ssh(
        endpoint = EndpointSource.Remote("ssh.example.test", 22),
        identity = SshServerIdentity("ssh.example.test", 22),
        username = "tester",
        authMethod = auth,
        remoteTargetHost = "database.internal",
        remoteTargetPort = 5432,
    ),
)
