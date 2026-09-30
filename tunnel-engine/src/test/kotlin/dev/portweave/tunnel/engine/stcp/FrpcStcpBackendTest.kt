package dev.portweave.tunnel.engine.stcp

import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.FrpsServerIdentity
import dev.portweave.core.model.StcpWireProtocol
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.core.security.CredentialRef
import dev.portweave.tunnel.engine.TunnelErrorCode
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcEnsureVisitorResult
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcSessionConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcStcpVisitorClient
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcStcpVisitorConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcStcpVisitorState
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcStopSessionResult
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcVisitorReadyResult
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcVisitorRuntimeState
import io.github.ycfeng.ocdeck.frpcstcpvisitor.KotlinFrpcStcpVisitorException
import io.github.ycfeng.ocdeck.frpcstcpvisitor.KotlinFrpcStcpVisitorFailure
import java.util.UUID
import javax.net.ssl.SSLHandshakeException
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.assertFalse
import org.junit.Test
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.flow.MutableStateFlow
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcStcpVisitorStateObservable
import dev.portweave.tunnel.engine.RecoveringBackendHandle

class FrpcStcpBackendTest {
    @OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
    @Test fun observableBackendWaitsForInternalRecoveryWithoutPollingAndTerminatesOnFailure() = runTest {
        val client = ObservableClient()
        val handle = FrpcStcpBackend({ client }).start(config(), credentials())
        client.snapshot.value = client.initialState()
        val lost = CompletableDeferred<Unit>()
        val watcher = launch { handle.awaitConnectionLost(); lost.complete(Unit) }
        runCurrent()
        advanceTimeBy(60_000)
        runCurrent()
        assertEquals(0, client.reads)
        val ready = client.snapshot.value
        client.snapshot.value = ready.copy(phase = "reconnecting",
            visitors = ready.visitors.mapValues { it.value.copy(phase = "pending", listenerBound = false) })
        runCurrent()
        assertFalse(lost.isCompleted)
        assertFalse((handle as RecoveringBackendHandle).availability.value.ready)
        client.snapshot.value = ready.copy(controlEpoch = 2,
            visitors = ready.visitors.mapValues { it.value.copy(boundControlEpoch = 2) })
        runCurrent()
        assertEquals(2L, handle.availability.value.epoch)
        assertFalse(lost.isCompleted)
        client.snapshot.value = ready.copy(phase = "failed", lastError = "login rejected")
        runCurrent()
        assertTrue(lost.isCompleted)
        assertEquals(TunnelErrorCode.STCP_LOGIN_FAILED, handle.failure?.code)
        assertEquals(false, handle.failure?.retryable)
        watcher.join()
        handle.stop()
    }

    @OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
    @Test fun alreadyClosedObservableCompletesWithoutWaitingForAnotherEvent() = runTest {
        val client = ObservableClient()
        val handle = FrpcStcpBackend({ client }).start(config(), credentials())
        handle.awaitConnectionLost()
        assertFalse(handle.isConnected)
        handle.stop()
    }

    @Test
    fun mapsConfigWaitsForReadinessAndStopsSession() = runTest {
        val client = RecordingClient()
        val backend = FrpcStcpBackend({ client }, statePollMillis = 1)
        val token = "token-value".toByteArray()
        val secret = "secret-value".toByteArray()
        val credentials = StcpCredentials(token, secret)

        val handle = backend.start(config(), credentials)

        assertEquals("relay.example", client.sessionConfig?.serverAddr)
        assertEquals("frps.example", client.sessionConfig?.tlsServerName)
        assertEquals("v2", client.sessionConfig?.wireProtocol)
        assertEquals("127.0.0.1", client.visitorConfig?.bindAddr)
        assertEquals(15432, client.visitorConfig?.bindPort)
        assertEquals(true, client.visitorConfig?.useEncryption)
        assertEquals(true, client.visitorConfig?.useCompression)
        assertEquals("server-user", client.visitorConfig?.serverUser)
        assertEquals(15432, handle.localEndpoint.port)

        credentials.close()
        assertArrayEquals(ByteArray(token.size), token)
        assertArrayEquals(ByteArray(secret.size), secret)
        handle.stop()
        assertEquals(listOf("visitor", "session"), client.stops)
        assertEquals(true, client.closed)
        assertFalse(handle.isConnected)
    }

    @Test
    fun bindConflictMapsToTypedPortError() = runTest {
        val client = RecordingClient().apply {
            startFailure = KotlinFrpcStcpVisitorException(KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT)
        }
        val failure = try {
            FrpcStcpBackend({ client }).start(config(), credentials())
            error("expected STCP backend failure")
        } catch (expected: StcpBackendException) {
            expected
        }
        assertEquals(TunnelErrorCode.PORT_IN_USE, failure.tunnelError.code)
        assertFalse(failure.tunnelError.retryable)
    }

    @Test
    fun invalidTlsPinIsRejectedBeforeStartingClient() = runTest {
        val client = RecordingClient()
        val invalid = config().copy(
            spec = (config().spec as TunnelSpec.Stcp).copy(tlsLeafPinSha256 = "bad-pin"),
        )
        val failure = try {
            FrpcStcpBackend({ client }).start(invalid, credentials())
            error("expected STCP backend failure")
        } catch (expected: StcpBackendException) {
            expected
        }
        assertEquals(TunnelErrorCode.CONFIG_INVALID, failure.tunnelError.code)
        assertEquals(null, client.sessionConfig)
    }

    @Test
    fun tlsHandshakeFailureMapsToNonRetryableIdentityError() {
        val failure = KotlinFrpcStcpVisitorException(
            KotlinFrpcStcpVisitorFailure.CONTROL_FAILED,
            SSLHandshakeException("certificate pin did not match"),
        )
        val error = failure.toTunnelError()
        assertEquals(TunnelErrorCode.TLS_PIN_MISMATCH, error.code)
        assertFalse(error.retryable)
    }

    @Test
    fun asynchronousControlDiagnosticsDistinguishPinAndAuthenticationFailures() {
        val generic = KotlinFrpcStcpVisitorException(KotlinFrpcStcpVisitorFailure.CONTROL_FAILED)
        val pinError = generic.toTunnelError("transport tls handshake failed", tlsPinConfigured = true)
        val loginError = generic.toTunnelError("login rejected")
        assertEquals(TunnelErrorCode.TLS_PIN_MISMATCH, pinError.code)
        assertFalse(pinError.retryable)
        assertEquals(TunnelErrorCode.STCP_LOGIN_FAILED, loginError.code)
        assertFalse(loginError.retryable)
    }

    private fun config(): TunnelConfig = TunnelConfig.empty(TunnelKind.STCP, "database", 15432).copy(
        spec = TunnelSpec.Stcp(
            endpoint = EndpointSource.Remote("relay.example", 7000),
            identity = FrpsServerIdentity("frps.example", 7000),
            tokenRef = CredentialRef(UUID.randomUUID()),
            user = "visitor-user",
            serverUser = "server-user",
            serverName = "database",
            secretKeyRef = CredentialRef(UUID.randomUUID()),
            wireProtocol = StcpWireProtocol.V2,
            useEncryption = true,
            useCompression = true,
            tlsLeafPinSha256 = "ab".repeat(32),
        ),
    )

    private fun credentials() = StcpCredentials("token".toByteArray(), "secret".toByteArray())
}

private open class RecordingClient : FrpcStcpVisitorClient, AutoCloseable {
    var sessionConfig: FrpcSessionConfig? = null
    var visitorConfig: FrpcStcpVisitorConfig? = null
    var startFailure: Exception? = null
    val stops = mutableListOf<String>()
    var closed = false

    override suspend fun startSession(config: FrpcSessionConfig): String {
        startFailure?.let { throw it }
        sessionConfig = config
        return "session"
    }

    override suspend fun ensureVisitor(
        sessionId: String,
        visitor: FrpcStcpVisitorConfig,
    ): FrpcEnsureVisitorResult {
        visitorConfig = visitor
        return FrpcEnsureVisitorResult(visitor.bindPort, 1)
    }

    override suspend fun waitVisitorReady(
        sessionId: String,
        visitorName: String,
        desiredRevision: Long,
        timeoutMillis: Long,
    ) = FrpcVisitorReadyResult(visitorName, desiredRevision, "ready", true, 1)

    override suspend fun stopVisitor(sessionId: String, visitorName: String) {
        stops += "visitor"
    }

    override suspend fun stopSession(sessionId: String, timeoutMillis: Long): FrpcStopSessionResult {
        stops += "session"
        return FrpcStopSessionResult(sessionId, "closed")
    }

    override suspend fun getState(sessionId: String): FrpcStcpVisitorState {
        val visitor = requireNotNull(visitorConfig)
        return FrpcStcpVisitorState(
            sessionId = sessionId,
            phase = "open",
            controlEpoch = 1,
            visitors = mapOf(
                visitor.name to FrpcVisitorRuntimeState(1, "ready", true, 1),
            ),
        )
    }

    override fun close() {
        closed = true
    }
}

private class ObservableClient : RecordingClient(), FrpcStcpVisitorStateObservable {
    val snapshot = MutableStateFlow(FrpcStcpVisitorState("session", "closed"))
    var reads = 0
    suspend fun initialState() = super.getState("session")
    override suspend fun getState(sessionId: String): FrpcStcpVisitorState { reads++; return snapshot.value }
    override suspend fun observeState(sessionId: String) = snapshot
}
