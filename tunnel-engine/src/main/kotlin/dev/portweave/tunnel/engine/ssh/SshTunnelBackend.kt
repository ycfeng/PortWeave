package dev.portweave.tunnel.engine.ssh

import com.jcraft.jsch.HostKeyRepository
import com.jcraft.jsch.Channel
import com.jcraft.jsch.ChannelDirectTCPIP
import com.jcraft.jsch.JSch
import com.jcraft.jsch.JSchException
import com.jcraft.jsch.Session
import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.SshAuthMethod
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.tunnel.engine.BackendHandle
import dev.portweave.tunnel.engine.BackendCapabilities
import dev.portweave.tunnel.engine.BackendMetrics
import dev.portweave.tunnel.engine.LocalEndpoint
import dev.portweave.tunnel.engine.TunnelError
import dev.portweave.tunnel.engine.TunnelErrorCode
import java.net.BindException
import java.net.ConnectException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.security.Security
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.CoroutineStart
import org.bouncycastle.jce.provider.BouncyCastleProvider

class SshCredentials(
    val password: ByteArray? = null,
    val privateKey: ByteArray? = null,
    val passphrase: ByteArray? = null,
    val hostFingerprint: ByteArray,
) : AutoCloseable {
    override fun close() {
        password?.fill(0)
        privateKey?.fill(0)
        passphrase?.fill(0)
        hostFingerprint.fill(0)
    }

    override fun toString(): String = "SshCredentials(<redacted>)"
}

class SshBackendException(
    val tunnelError: TunnelError,
    cause: Throwable? = null,
) : IllegalStateException("SSH backend failed: ${tunnelError.code}", cause)

interface SshBackend {
    suspend fun discoverHostKey(config: TunnelConfig): DiscoveredHostKey
    suspend fun start(config: TunnelConfig, credentials: SshCredentials): BackendHandle
}

class SshTunnelBackend internal constructor(
    private val sessionFactory: SshSessionFactory,
    private val workPolicy: dev.portweave.tunnel.engine.RuntimeWorkPolicy = dev.portweave.tunnel.engine.RuntimeWorkPolicy(),
) : SshBackend {
    constructor(workPolicy: dev.portweave.tunnel.engine.RuntimeWorkPolicy = dev.portweave.tunnel.engine.RuntimeWorkPolicy()) : this(JschSessionFactory, workPolicy)

    override suspend fun discoverHostKey(config: TunnelConfig): DiscoveredHostKey = withContext(Dispatchers.IO) {
        val ssh = requireSsh(config)
        val endpoint = requireRemoteEndpoint(ssh)
        validateDiscovery(ssh, endpoint)
        val repository = DiscoveringSshHostKeyRepository()
        val session = sessionFactory.create(ssh.username, endpoint.host, endpoint.port)
        session.setWorkPolicy(workPolicy)
        try {
            configureIdentityVerification(session, repository)
            val failure = try {
                session.connectCancellable(secondsToMillis(ssh.connectTimeoutSeconds))
                null
            } catch (exception: Exception) {
                exception
            }
            if (failure is CancellationException) throw failure
            repository.result() ?: throw SshBackendException(
                TunnelError(TunnelErrorCode.SSH_FORWARD_FAILED, true, "ssh_host_key_discovery_failed"),
                failure,
            )
        } finally {
            session.disconnectSafely()
        }
    }

    override suspend fun start(config: TunnelConfig, credentials: SshCredentials): SshBackendHandle =
        withContext(Dispatchers.IO) {
            val ssh = requireSsh(config)
            val endpoint = requireRemoteEndpoint(ssh)
            validateStart(config, ssh, endpoint, credentials)
            val fingerprint = credentials.hostFingerprint.toString(Charsets.UTF_8)
            val repository = try {
                PinnedSshHostKeyRepository(fingerprint)
            } finally {
                // The String is short-lived and never logged; source bytes are wiped by the caller.
            }
            val session = sessionFactory.create(ssh.username, endpoint.host, endpoint.port)
        session.setWorkPolicy(workPolicy)
            val startedAtNanos = System.nanoTime()
            try {
                configureIdentityVerification(session, repository)
                session.setConfig("PreferredAuthentications", ssh.authMethod.preferredAuthentications())
                session.setServerAliveInterval(secondsToMillis(ssh.keepAliveSeconds))
                when (ssh.authMethod) {
                    SshAuthMethod.PASSWORD -> session.setPassword(requireNotNull(credentials.password))
                    SshAuthMethod.PRIVATE_KEY -> session.addIdentity(
                        "portweave-${config.id}",
                        requireNotNull(credentials.privateKey),
                        credentials.passphrase,
                    )
                }
                session.connectCancellable(secondsToMillis(ssh.connectTimeoutSeconds))
                if (repository.mismatchObserved || repository.matchedFingerprint == null) {
                    throw SshBackendException(
                        TunnelError(TunnelErrorCode.SSH_HOST_KEY_MISMATCH, false, "ssh_host_key_mismatch"),
                    )
                }
                val boundPort = session.setPortForwardingL(
                    "127.0.0.1",
                    config.localPort,
                    ssh.remoteTargetHost,
                    ssh.remoteTargetPort,
                )
                session.setHandshakeLatency((System.nanoTime() - startedAtNanos) / 1_000_000L)
                SshBackendHandle(session, boundPort, workPolicy)
            } catch (cancellation: CancellationException) {
                session.disconnectSafely()
                throw cancellation
            } catch (failure: SshBackendException) {
                session.disconnectSafely()
                throw failure
            } catch (failure: Exception) {
                session.disconnectSafely()
                if (repository.mismatchObserved) {
                    throw SshBackendException(
                        TunnelError(TunnelErrorCode.SSH_HOST_KEY_MISMATCH, false, "ssh_host_key_mismatch"),
                    )
                }
                throw mapFailure(failure)
            }
        }

    private fun configureIdentityVerification(session: SshSessionHandle, repository: HostKeyRepository) {
        session.setHostKeyRepository(repository)
        session.setConfig("StrictHostKeyChecking", "yes")
        session.setConfig("kex", AllowedKexAlgorithms)
        session.setConfig("server_host_key", AllowedHostKeyAlgorithms)
    }

    private fun validateDiscovery(ssh: TunnelSpec.Ssh, endpoint: EndpointSource.Remote) {
        require(endpoint.host.isNotBlank() && endpoint.port in 1..65535)
        require(ssh.username.isNotBlank())
        require(ssh.connectTimeoutSeconds in 1..120)
    }

    private fun validateStart(
        config: TunnelConfig,
        ssh: TunnelSpec.Ssh,
        endpoint: EndpointSource.Remote,
        credentials: SshCredentials,
    ) {
        try {
            validateDiscovery(ssh, endpoint)
            require(config.localPort in 1024..65535)
            require(ssh.identity.logicalHost.isNotBlank() && ssh.identity.logicalPort in 1..65535)
            require(ssh.remoteTargetHost.isNotBlank() && ssh.remoteTargetPort in 1..65535)
            require(ssh.keepAliveSeconds in 1..300)
            require(credentials.hostFingerprint.isNotEmpty())
            when (ssh.authMethod) {
                SshAuthMethod.PASSWORD -> require(credentials.password != null && credentials.password.isNotEmpty())
                SshAuthMethod.PRIVATE_KEY ->
                    require(
                        credentials.privateKey != null && credentials.privateKey.isNotEmpty() &&
                            credentials.privateKey.size <= MaxPrivateKeyBytes,
                    )
            }
        } catch (failure: IllegalArgumentException) {
            throw SshBackendException(
                TunnelError(TunnelErrorCode.CONFIG_INVALID, false, "ssh_config_invalid"),
                failure,
            )
        }
    }

    private fun requireSsh(config: TunnelConfig): TunnelSpec.Ssh = config.spec as? TunnelSpec.Ssh
        ?: throw SshBackendException(TunnelError(TunnelErrorCode.CONFIG_INVALID, false, "not_ssh_config"))

    private fun requireRemoteEndpoint(ssh: TunnelSpec.Ssh): EndpointSource.Remote =
        ssh.endpoint as? EndpointSource.Remote ?: throw SshBackendException(
            TunnelError(TunnelErrorCode.CONFIG_INVALID, false, "ssh_dependency_not_resolved"),
        )

    private fun mapFailure(failure: Exception): SshBackendException {
        val causes = generateSequence<Throwable>(failure) { it.cause }.toList()
        val error = when {
            causes.any { it is BindException } -> TunnelError(TunnelErrorCode.PORT_IN_USE, false, "local_port_in_use")
            causes.any { it is UnknownHostException } -> TunnelError(TunnelErrorCode.DNS_FAILED, true, "dns_failed")
            causes.any { it is SocketTimeoutException } -> TunnelError(TunnelErrorCode.CONNECT_TIMEOUT, true, "connect_timeout")
            causes.any { it is ConnectException } -> TunnelError(TunnelErrorCode.CONNECTION_REFUSED, true, "connection_refused")
            failure is JSchException && listOf("Auth fail", "invalid privatekey", "decrypt", "passphrase")
                .any { marker -> failure.message.orEmpty().contains(marker, true) } ->
                TunnelError(TunnelErrorCode.SSH_AUTH_FAILED, false, "ssh_auth_failed")
            failure.message.orEmpty().contains("PortForwardingL", true) ->
                TunnelError(TunnelErrorCode.PORT_IN_USE, false, "local_port_in_use")
            else -> TunnelError(TunnelErrorCode.SSH_FORWARD_FAILED, true, "ssh_forward_failed")
        }
        return SshBackendException(error, failure)
    }

    private companion object {
        const val AllowedKexAlgorithms =
            "curve25519-sha256,curve25519-sha256@libssh.org," +
                "ecdh-sha2-nistp256,ecdh-sha2-nistp384,ecdh-sha2-nistp521," +
                "diffie-hellman-group16-sha512,diffie-hellman-group14-sha256"
        const val AllowedHostKeyAlgorithms =
            "ssh-ed25519,ecdsa-sha2-nistp521,ecdsa-sha2-nistp384,ecdsa-sha2-nistp256," +
                "rsa-sha2-512,rsa-sha2-256"
        const val MaxPrivateKeyBytes = 256 * 1024
    }
}

class SshBackendHandle internal constructor(
    private val session: SshSessionHandle,
    port: Int,
    private val workPolicy: dev.portweave.tunnel.engine.RuntimeWorkPolicy = dev.portweave.tunnel.engine.RuntimeWorkPolicy(),
) : BackendHandle {
    override val kind = TunnelKind.SSH
    override val localEndpoint = LocalEndpoint("127.0.0.1", port)
    override val isConnected: Boolean get() = session.isConnected
    override val capabilities: BackendCapabilities get() = session.capabilities.copy(handshakeLatency = true)
    override val metrics: StateFlow<BackendMetrics> get() = session.metrics

    override suspend fun awaitConnectionLost() {
        while (session.isConnected) {
            kotlinx.coroutines.currentCoroutineContext().ensureActive()
            workPolicy.counters.increment("sshStateChecks")
            workPolicy.awaitNextCheck(metrics)
        }
    }

    override suspend fun stop() = withContext(NonCancellable + Dispatchers.IO) { session.disconnectSafely() }
}

internal interface SshSessionFactory {
    fun create(username: String, host: String, port: Int): SshSessionHandle
}

internal interface SshSessionHandle {
    val isConnected: Boolean
    val capabilities: BackendCapabilities get() = BackendCapabilities()
    val metrics: StateFlow<BackendMetrics> get() = kotlinx.coroutines.flow.MutableStateFlow(BackendMetrics())
    fun setHostKeyRepository(repository: HostKeyRepository)
    fun setConfig(key: String, value: String)
    fun setServerAliveInterval(intervalMillis: Int)
    fun setWorkPolicy(policy: dev.portweave.tunnel.engine.RuntimeWorkPolicy) = Unit
    fun setHandshakeLatency(millis: Long) = Unit
    fun addIdentity(name: String, privateKey: ByteArray, passphrase: ByteArray?)
    fun setPassword(password: ByteArray)
    fun connect(timeoutMillis: Int)
    fun setPortForwardingL(bindAddress: String, localPort: Int, remoteHost: String, remotePort: Int): Int
    fun disconnect()
}

private object JschSessionFactory : SshSessionFactory {
    override fun create(username: String, host: String, port: Int): SshSessionHandle {
        ensureBouncyCastle()
        val jsch = JSch()
        return JschSessionHandle(jsch, jsch.getSession(username, host, port))
    }
}

private class JschSessionHandle(private val jsch: JSch, private val session: Session) : SshSessionHandle {
    private val forwarders = ConcurrentHashMap.newKeySet<LoopbackForwarder>()
    private val metricsTracker = SshMetricsTracker()
    override val isConnected: Boolean get() = session.isConnected
    override val capabilities = BackendCapabilities(byteCounters = true, activeConnectionCount = true)
    override val metrics: StateFlow<BackendMetrics> = metricsTracker.metrics
    override fun setHostKeyRepository(repository: HostKeyRepository) {
        jsch.setHostKeyRepository(repository)
        session.setHostKeyRepository(repository)
    }
    override fun setConfig(key: String, value: String) = session.setConfig(key, value)
    override fun setServerAliveInterval(intervalMillis: Int) { session.serverAliveInterval = intervalMillis }
    override fun setWorkPolicy(policy: dev.portweave.tunnel.engine.RuntimeWorkPolicy) { metricsTracker.policy = policy }
    override fun setHandshakeLatency(millis: Long) = metricsTracker.handshakeLatency(millis)
    override fun addIdentity(name: String, privateKey: ByteArray, passphrase: ByteArray?) =
        jsch.addIdentity(name, privateKey, null, passphrase)
    override fun setPassword(password: ByteArray) = session.setPassword(password)
    override fun connect(timeoutMillis: Int) = session.connect(timeoutMillis)
    override fun setPortForwardingL(
        bindAddress: String,
        localPort: Int,
        remoteHost: String,
        remotePort: Int,
    ): Int {
        require(bindAddress == "127.0.0.1") { "Only IPv4 loopback forwarding is supported" }
        val forwarder = LoopbackForwarder(
            session,
            localPort,
            remoteHost,
            remotePort,
            metricsTracker,
        ) { forwarders.remove(it) }
        val boundPort = forwarder.start()
        forwarders += forwarder
        return boundPort
    }
    override fun disconnect() {
        forwarders.toList().forEach(LoopbackForwarder::close)
        forwarders.clear()
        session.disconnect()
        metricsTracker.close()
    }
}

private class LoopbackForwarder(
    private val session: Session,
    private val requestedPort: Int,
    private val remoteHost: String,
    private val remotePort: Int,
    private val metrics: SshMetricsTracker,
    private val onClosed: (LoopbackForwarder) -> Unit,
) : AutoCloseable {
    private val running = AtomicBoolean(false)
    private val serverSocket = ServerSocket()
    private val sockets = ConcurrentHashMap.newKeySet<Socket>()
    private val channels = ConcurrentHashMap.newKeySet<Channel>()
    private val executor = Executors.newCachedThreadPool { task ->
        Thread(task, "portweave-ssh-forward").apply { isDaemon = true }
    }

    fun start(): Int {
        // Reconnects must be able to bind while old client connections are in TIME_WAIT.
        serverSocket.reuseAddress = true
        serverSocket.bind(
            InetSocketAddress(InetAddress.getByName("127.0.0.1"), requestedPort),
            ListenBacklog,
        )
        running.set(true)
        executor.execute(::acceptLoop)
        return serverSocket.localPort
    }

    private fun acceptLoop() {
        try {
            while (running.get() && session.isConnected) {
                val socket = try {
                    serverSocket.accept()
                } catch (_: Exception) {
                    break
                }
                if (!running.get()) {
                    socket.close()
                    break
                }
                sockets += socket
                metrics.connectionOpened()
                executor.execute { forward(socket) }
            }
        } finally {
            if (running.get()) close()
        }
    }

    private fun forward(socket: Socket) {
        var channel: ChannelDirectTCPIP? = null
        try {
            socket.tcpNoDelay = true
            channel = session.openChannel("direct-tcpip") as ChannelDirectTCPIP
            channels += channel
            channel.setHost(remoteHost)
            channel.setPort(remotePort)
            channel.setOrgIPAddress("127.0.0.1")
            channel.setOrgPort(socket.port)
            val fromRemote = channel.inputStream
            val toRemote = channel.outputStream
            channel.connect(ChannelConnectTimeoutMillis)

            val upload = executor.submit {
                try {
                    socket.getInputStream().copyBoundedTo(toRemote, metrics::uploaded)
                } finally {
                    runCatching { toRemote.close() }
                }
            }
            try {
                fromRemote.copyBoundedTo(socket.getOutputStream(), metrics::downloaded)
                runCatching { socket.shutdownOutput() }
            } finally {
                runCatching { upload.get(StopWaitSeconds, TimeUnit.SECONDS) }
            }
        } catch (_: Exception) {
            // Per-client forwarding errors close only that client; the SSH session remains reusable.
        } finally {
            channel?.disconnect()
            channel?.let(channels::remove)
            runCatching { socket.close() }
            sockets.remove(socket)
            metrics.connectionClosed()
        }
    }

    override fun close() {
        if (!running.compareAndSet(true, false)) return
        runCatching { serverSocket.close() }
        sockets.toList().forEach { runCatching { it.close() } }
        channels.toList().forEach { runCatching { it.disconnect() } }
        executor.shutdownNow()
        runCatching { executor.awaitTermination(StopWaitSeconds, TimeUnit.SECONDS) }
        onClosed(this)
    }

    private companion object {
        const val ChannelConnectTimeoutMillis = 10_000
        const val ListenBacklog = 128
        const val StopWaitSeconds = 2L
    }
}

private fun java.io.InputStream.copyBoundedTo(
    output: java.io.OutputStream,
    onBytes: (Long) -> Unit,
) {
    val buffer = ByteArray(16 * 1024)
    try {
        while (true) {
            val read = read(buffer)
            if (read < 0) break
            if (read == 0) continue
            output.write(buffer, 0, read)
            output.flush()
            onBytes(read.toLong())
        }
    } finally {
        buffer.fill(0)
    }
}

private suspend fun SshSessionHandle.connectCancellable(timeoutMillis: Int) = coroutineScope {
    val connecting = AtomicBoolean(true)
    val closer = launch(Dispatchers.IO, start = CoroutineStart.UNDISPATCHED) {
        try { awaitCancellation() } finally { if (connecting.get()) disconnectSafely() }
    }
    try {
        connect(timeoutMillis)
        kotlinx.coroutines.currentCoroutineContext().ensureActive()
    } finally {
        connecting.set(false)
        closer.cancel()
    }
}

private fun SshSessionHandle.disconnectSafely() {
    try {
        disconnect()
    } catch (_: Exception) {
        // Best-effort cleanup must not replace the primary result.
    }
}

private fun SshAuthMethod.preferredAuthentications(): String = when (this) {
    SshAuthMethod.PASSWORD -> "password"
    SshAuthMethod.PRIVATE_KEY -> "publickey"
}

private fun secondsToMillis(seconds: Int): Int {
    require(seconds in 1..Int.MAX_VALUE / 1_000)
    return seconds * 1_000
}

private val ProviderLock = Any()

private fun ensureBouncyCastle() = synchronized(ProviderLock) {
    val current = Security.getProvider(BouncyCastleProvider.PROVIDER_NAME)
    if (current?.javaClass?.name == BouncyCastleProvider::class.java.name) return@synchronized
    if (current != null) Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME)
    Security.addProvider(BouncyCastleProvider())
}
