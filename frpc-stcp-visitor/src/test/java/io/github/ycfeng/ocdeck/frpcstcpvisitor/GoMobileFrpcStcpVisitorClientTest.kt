package io.github.ycfeng.ocdeck.frpcstcpvisitor

import java.net.BindException
import java.util.concurrent.CancellationException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class GoMobileFrpcStcpVisitorClientTest {
    private val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
    }

    @Test
    fun startSessionEncodesSessionConfig() = runTest {
        val bridge = FakeBridge()
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        val sessionId = client.startSession(
            FrpcSessionConfig(
                serverAddr = "frps.example.com",
                serverPort = 7001,
                authToken = "token-value",
                user = "mobile",
                transportProtocol = "tcp",
                wireProtocol = "v2",
            ),
        )

        assertEquals("session-1", sessionId)
        assertEquals(
            FrpcSessionConfig(
                serverAddr = "frps.example.com",
                serverPort = 7001,
                authToken = "token-value",
                user = "mobile",
                transportProtocol = "tcp",
                wireProtocol = "v2",
            ),
            json.decodeFromString<FrpcSessionConfig>(bridge.startedConfigJson!!),
        )
    }

    @Test
    fun startSessionEncodesDefaultServerPort() = runTest {
        val bridge = FakeBridge()
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        client.startSession(
            FrpcSessionConfig(
                serverAddr = "frps.example.com",
                authToken = "",
            ),
        )

        assertTrue(bridge.startedConfigJson!!.contains("\"serverPort\":7000"))
    }

    @Test
    fun startSessionEncodesTlsCertificatePinForBridgeEnforcement() = runTest {
        val bridge = FakeBridge()
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)
        val pin = "0".repeat(64)

        client.startSession(
            FrpcSessionConfig(
                serverAddr = "frps.example.com",
                authToken = "token-value",
                tlsServerCertificateSha256 = pin,
            ),
        )

        assertEquals(
            pin,
            json.decodeFromString<FrpcSessionConfig>(bridge.startedConfigJson!!).tlsServerCertificateSha256,
        )
    }

    @Test
    fun ensureVisitorEncodesVisitorConfigAndReturnsRevision() = runTest {
        val bridge = FakeBridge(
            ensureJson = """{"bindPort":5096,"desiredRevision":7}""",
        )
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        val result = client.ensureVisitor(
            sessionId = "session-1",
            visitor = FrpcStcpVisitorConfig(
                name = "opencode_android_remote",
                serverName = "opencode",
                serverUser = "server-user",
                secretKey = "secret-value",
                bindAddr = "127.0.0.1",
                bindPort = 5096,
                useEncryption = true,
                useCompression = true,
            ),
        )

        assertEquals(FrpcEnsureVisitorResult(bindPort = 5096, desiredRevision = 7), result)
        assertEquals("session-1", bridge.ensuredSessionId)
        assertEquals(
            FrpcStcpVisitorConfig(
                name = "opencode_android_remote",
                serverName = "opencode",
                serverUser = "server-user",
                secretKey = "secret-value",
                bindAddr = "127.0.0.1",
                bindPort = 5096,
                useEncryption = true,
                useCompression = true,
            ),
            json.decodeFromString<FrpcStcpVisitorConfig>(bridge.ensuredVisitorJson!!),
        )
    }

    @Test
    fun waitVisitorReadyDelegatesRevisionAndTimeout() = runTest {
        val bridge = FakeBridge(
            readyJson = """{
                "name":"visitor-1",
                "desiredRevision":7,
                "phase":"ready",
                "listenerBound":true,
                "boundControlEpoch":3
            }""".trimIndent(),
        )
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        val result = client.waitVisitorReady(
            sessionId = "session-1",
            visitorName = "visitor-1",
            desiredRevision = 7,
            timeoutMillis = 8_000,
        )

        assertEquals("visitor-1", result.name)
        assertEquals(7, result.desiredRevision)
        assertEquals(3, result.boundControlEpoch)
        assertTrue(result.listenerBound)
        assertEquals(ReadyCall("session-1", "visitor-1", 7, 8_000), bridge.readyCalls.single())
    }

    @Test
    fun ensureVisitorEncodesDefaultBindPort() = runTest {
        val bridge = FakeBridge()
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        client.ensureVisitor(
            sessionId = "session-1",
            visitor = FrpcStcpVisitorConfig(
                name = "opencode_android_remote",
                serverName = "opencode",
                secretKey = "secret-value",
            ),
        )

        assertTrue(bridge.ensuredVisitorJson!!.contains("\"bindPort\":4096"))
    }

    @Test
    fun ensureAndWaitSanitizeKnownLegacyBindFailures() = runTest {
        val ensureMarker = "ensure-bind-detail-leak-marker"
        val waitMarker = "wait-bind-detail-leak-marker"
        val bridge = FakeBridge().apply {
            ensureFailure = Exception("listen failed: address already in use: $ensureMarker")
            readyFailure = Exception("listener failed: EADDRINUSE: $waitMarker")
        }
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        val ensureFailure = suspendFailureOf {
            client.ensureVisitor(
                sessionId = "session-1",
                visitor = FrpcStcpVisitorConfig(
                    name = "visitor-1",
                    serverName = "opencode",
                    secretKey = "secret-value",
                ),
            )
        }
        val waitFailure = suspendFailureOf {
            client.waitVisitorReady("session-1", "visitor-1", 1, 1_000)
        }

        assertSanitizedBindFailure(ensureFailure, ensureMarker)
        assertSanitizedBindFailure(waitFailure, waitMarker)
    }

    @Test
    fun ensureAndWaitPreserveNonBindFailureIdentity() = runTest {
        val ensureFailure = IdentityBridgeException("ordinary ensure failure", Unit)
        val waitFailure = IdentityBridgeException("ordinary wait failure", Unit)
        val bridge = FakeBridge().apply {
            this.ensureFailure = ensureFailure
            readyFailure = waitFailure
        }
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        assertSame(
            ensureFailure,
            suspendFailureOf {
                client.ensureVisitor(
                    sessionId = "session-1",
                    visitor = FrpcStcpVisitorConfig(
                        name = "visitor-1",
                        serverName = "opencode",
                        secretKey = "secret-value",
                    ),
                )
            },
        )
        assertSame(
            waitFailure,
            suspendFailureOf {
                client.waitVisitorReady("session-1", "visitor-1", 1, 1_000)
            },
        )
    }

    @Test
    fun nestedCancellationAndJvmErrorPrecedeLegacyBindTranslation() = runTest {
        val cancellation = CancellationException("nested cancellation")
        val error = BridgeTestJvmError(Unit)
        val bridge = FakeBridge().apply {
            ensureFailure = IdentityBridgeException("address already in use", Unit).apply {
                initCause(cancellation)
            }
            readyFailure = IdentityBridgeException("EADDRINUSE", Unit).apply {
                initCause(error)
            }
        }
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        assertSame(
            cancellation,
            suspendFailureOf {
                client.ensureVisitor(
                    sessionId = "session-1",
                    visitor = FrpcStcpVisitorConfig(
                        name = "visitor-1",
                        serverName = "opencode",
                        secretKey = "secret-value",
                    ),
                )
            },
        )
        assertSame(
            error,
            suspendFailureOf {
                client.waitVisitorReady("session-1", "visitor-1", 1, 1_000)
            },
        )
    }

    @Test
    fun cyclicOrdinaryCauseChainTerminatesBeforeLegacyBindTranslation() = runTest {
        val marker = "cyclic-bind-detail-leak-marker"
        val first = IdentityBridgeException("address already in use: $marker", Unit)
        val second = IdentityBridgeException("ordinary nested failure", Unit)
        first.initCause(second)
        second.initCause(first)
        val bridge = FakeBridge().apply { ensureFailure = first }
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        val failure = suspendFailureOf {
            client.ensureVisitor(
                sessionId = "session-1",
                visitor = FrpcStcpVisitorConfig(
                    name = "visitor-1",
                    serverName = "opencode",
                    secretKey = "secret-value",
                ),
            )
        }

        assertSanitizedBindFailure(failure, marker)
    }

    @Test
    fun ensureAndWaitPreserveCancellationAndJvmErrorIdentity() = runTest {
        val ensureCancellation = BridgeTestCancellationException("ensure cancellation", Unit)
        val ensureError = BridgeTestJvmError(Unit)
        val waitCancellation = BridgeTestCancellationException("wait cancellation", Unit)
        val waitError = BridgeTestJvmError(Unit)
        val bridge = FakeBridge()
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        bridge.ensureFailure = ensureCancellation
        assertSame(
            ensureCancellation,
            suspendFailureOf {
                client.ensureVisitor(
                    sessionId = "session-1",
                    visitor = FrpcStcpVisitorConfig(
                        name = "visitor-1",
                        serverName = "opencode",
                        secretKey = "secret-value",
                    ),
                )
            },
        )
        bridge.ensureFailure = ensureError
        assertSame(
            ensureError,
            suspendFailureOf {
                client.ensureVisitor(
                    sessionId = "session-1",
                    visitor = FrpcStcpVisitorConfig(
                        name = "visitor-1",
                        serverName = "opencode",
                        secretKey = "secret-value",
                    ),
                )
            },
        )
        bridge.readyFailure = waitCancellation
        assertSame(
            waitCancellation,
            suspendFailureOf {
                client.waitVisitorReady("session-1", "visitor-1", 1, 1_000)
            },
        )
        bridge.readyFailure = waitError
        assertSame(
            waitError,
            suspendFailureOf {
                client.waitVisitorReady("session-1", "visitor-1", 1, 1_000)
            },
        )
    }

    @Test
    fun callerCancellationDoesNotHideConcurrentBridgeJvmError() = runBlocking {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val fatal = BridgeTestJvmError(Unit)
        val bridge = FakeBridge().apply {
            beforeEnsure = {
                entered.countDown()
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw AssertionError("Timed out waiting to release bridge failure")
                }
            }
            ensureFailure = fatal
        }
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        supervisorScope {
            val call = async(start = CoroutineStart.UNDISPATCHED) {
                client.ensureVisitor(
                    sessionId = "session-1",
                    visitor = FrpcStcpVisitorConfig(
                        name = "visitor-1",
                        serverName = "opencode",
                        secretKey = "secret-value",
                    ),
                )
            }
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS))
                call.cancel(CancellationException("caller cancellation"))
            } finally {
                release.countDown()
            }

            assertSame(fatal, suspendFailureOf { call.await() })
        }
    }

    @Test
    fun callerCancellationPrecedesConcurrentOrdinaryAndLegacyBindFailures() = runBlocking {
        listOf(
            IdentityBridgeException("ordinary late failure", Unit),
            IdentityBridgeException("address already in use", Unit),
        ).forEach { bridgeFailure ->
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val cancellation = BridgeTestCancellationException("caller cancellation", Unit)
            val bridge = FakeBridge().apply {
                beforeEnsure = {
                    entered.countDown()
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw AssertionError("Timed out waiting to release bridge failure")
                    }
                }
                ensureFailure = bridgeFailure
            }
            val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

            supervisorScope {
                val call = async(start = CoroutineStart.UNDISPATCHED) {
                    client.ensureVisitor(
                        sessionId = "session-1",
                        visitor = FrpcStcpVisitorConfig(
                            name = "visitor-1",
                            serverName = "opencode",
                            secretKey = "secret-value",
                        ),
                    )
                }
                try {
                    assertTrue(entered.await(5, TimeUnit.SECONDS))
                    call.cancel(cancellation)
                } finally {
                    release.countDown()
                }

                assertSame(cancellation, suspendFailureOf { call.await() })
            }
        }
    }

    @Test
    fun nestedJvmErrorPrecedesEarlierCancellationInBridgeCauseChain() = runTest {
        val fatal = BridgeTestJvmError(Unit)
        val cancellation = BridgeTestCancellationException("nested cancellation", Unit).apply {
            initCause(fatal)
        }
        val bridge = FakeBridge().apply {
            ensureFailure = IdentityBridgeException("address already in use", Unit).apply {
                initCause(cancellation)
            }
        }
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        assertSame(
            fatal,
            suspendFailureOf {
                client.ensureVisitor(
                    sessionId = "session-1",
                    visitor = FrpcStcpVisitorConfig(
                        name = "visitor-1",
                        serverName = "opencode",
                        secretKey = "secret-value",
                    ),
                )
            },
        )
    }

    @Test
    fun sensitiveValueObjectsUseSafeSummaryToString() {
        val endpoint = "endpoint-leak-marker.invalid"
        val bindEndpoint = "bind-endpoint-leak-marker.invalid"
        val user = "user-leak-marker"
        val token = "token-leak-marker"
        val visitorName = "visitor-name-leak-marker"
        val serverName = "server-name-leak-marker"
        val serverUser = "server-user-leak-marker"
        val secret = "secret-leak-marker"
        val sessionId = "session-leak-marker"
        val lastError = "last-error-leak-marker"
        val visitorMapKey = "visitor-map-key-leak-marker"
        val session = FrpcSessionConfig(
            serverAddr = endpoint,
            serverPort = 7001,
            authToken = token,
            user = user,
        )
        val visitor = FrpcStcpVisitorConfig(
            name = visitorName,
            serverName = serverName,
            serverUser = serverUser,
            secretKey = secret,
            bindAddr = bindEndpoint,
            bindPort = 5096,
            useEncryption = true,
            useCompression = true,
        )
        val ready = FrpcVisitorReadyResult(
            name = visitorName,
            desiredRevision = 7,
            phase = "ready",
            listenerBound = true,
            boundControlEpoch = 3,
            lastError = lastError,
        )
        val runtime = FrpcVisitorRuntimeState(
            desiredRevision = 7,
            phase = "ready",
            listenerBound = true,
            boundControlEpoch = 3,
            lastError = lastError,
        )
        val state = FrpcStcpVisitorState(
            sessionId = sessionId,
            phase = "open",
            configRevision = 7,
            controlEpoch = 3,
            lastError = lastError,
            visitors = mapOf(visitorMapKey to runtime),
        )
        val stopped = FrpcStopSessionResult(sessionId = sessionId, phase = "closed")

        val summaries = listOf(session, visitor, ready, runtime, state, stopped).map(Any::toString)
        summaries.forEach { summary ->
            assertOmits(
                summary,
                endpoint,
                bindEndpoint,
                user,
                token,
                visitorName,
                serverName,
                serverUser,
                secret,
                sessionId,
                lastError,
                visitorMapKey,
            )
        }
        assertTrue(session.toString().contains("serverPort=7001"))
        assertTrue(session.toString().contains("authToken=<redacted>"))
        assertTrue(session.toString().contains("userPresent=true"))
        assertTrue(visitor.toString().contains("secretKey=<redacted>"))
        assertTrue(visitor.toString().contains("bindPort=5096"))
        assertTrue(ready.toString().contains("lastErrorPresent=true"))
        assertTrue(runtime.toString().contains("lastErrorPresent=true"))
        assertTrue(state.toString().contains("visitorCount=1"))
        assertTrue(stopped.toString().contains("phase=closed"))
    }

    @Test
    fun unavailableClientUsesTypedSafeFailureAndState() = runTest {
        val unsafeReason = "reason-leak-marker endpoint-leak-marker token-leak-marker"
        val sessionId = "session-leak-marker"
        val visitorName = "visitor-leak-marker"
        val client = UnavailableFrpcStcpVisitorClient(unsafeReason)

        val failures = listOf(
            suspendFailureOf {
                client.startSession(
                    FrpcSessionConfig(
                        serverAddr = "endpoint-leak-marker.invalid",
                        authToken = "token-leak-marker",
                    ),
                )
            },
            suspendFailureOf {
                client.ensureVisitor(
                    sessionId = sessionId,
                    visitor = FrpcStcpVisitorConfig(
                        name = visitorName,
                        serverName = "server-name-leak-marker",
                        secretKey = "secret-leak-marker",
                    ),
                )
            },
            suspendFailureOf {
                client.waitVisitorReady(sessionId, visitorName, 1, 1_000)
            },
        )

        failures.forEach { failure ->
            assertTrue(failure is GoMobileBridgeUnavailableException)
            assertEquals(GO_MOBILE_BRIDGE_UNAVAILABLE_MESSAGE, failure.message)
            assertNull(failure.cause)
            assertOmits(failure.toString(), unsafeReason, sessionId, visitorName, "token-leak-marker")
        }

        val state = client.getState(sessionId)
        assertEquals("unavailable", state.phase)
        assertNull(state.lastError)
        assertOmits(state.toString(), unsafeReason, sessionId)
    }

    @Test
    fun missingBridgeClassThrowsTypedSafeUnavailableFailure() {
        val missingClassName =
            "io.github.ycfeng.ocdeck.frpcstcpvisitor.missing.EndpointCredentialLeakMarker"
        val bridge = ReflectiveGoMobileFrpcStcpVisitorBridge(missingClassName)

        val failure = failureOf {
            bridge.startSession("{\"token\":\"json-token-leak-marker\"}")
        }

        assertTrue(failure is GoMobileBridgeUnavailableException)
        assertEquals(GO_MOBILE_BRIDGE_UNAVAILABLE_MESSAGE, failure.message)
        assertNull(failure.cause)
        assertOmits(failure.toString(), missingClassName, "json-token-leak-marker")
    }

    @Test
    fun missingBridgeMethodThrowsTypedSafeApiMismatchFailure() {
        val bridgeClassName = MissingMethodsBridge::class.java.name
        val bridge = ReflectiveGoMobileFrpcStcpVisitorBridge(bridgeClassName)
        val sensitiveJson = "{\"endpoint\":\"endpoint-leak-marker\",\"token\":\"token-leak-marker\"}"

        val failure = failureOf { bridge.startSession(sensitiveJson) }

        assertTrue(failure is GoMobileBridgeApiMismatchException)
        assertEquals(GO_MOBILE_BRIDGE_API_MISMATCH_MESSAGE, failure.message)
        assertNull(failure.cause)
        assertOmits(failure.toString(), bridgeClassName, sensitiveJson, "endpoint-leak-marker", "token-leak-marker")
    }

    @Test
    fun malformedBridgeJsonThrowsTypedSafeApiMismatchFailure() = runTest {
        val malformedJson =
            "{\"bindPort\":\"endpoint-leak-marker\",\"credential\":\"credential-leak-marker\""
        val bridge = FakeBridge(ensureJson = malformedJson)
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        val failure = suspendFailureOf {
            client.ensureVisitor(
                sessionId = "session-leak-marker",
                visitor = FrpcStcpVisitorConfig(
                    name = "visitor-leak-marker",
                    serverName = "server-leak-marker",
                    secretKey = "secret-leak-marker",
                ),
            )
        }

        assertTrue(failure is GoMobileBridgeApiMismatchException)
        assertEquals(GO_MOBILE_BRIDGE_API_MISMATCH_MESSAGE, failure.message)
        generateSequence(failure) { it.cause }.forEach { reportedFailure ->
            assertTrue(reportedFailure is GoMobileBridgeApiMismatchException)
            assertEquals(GO_MOBILE_BRIDGE_API_MISMATCH_MESSAGE, reportedFailure.message)
            assertOmits(
                reportedFailure.toString(),
                malformedJson,
                "endpoint-leak-marker",
                "credential-leak-marker",
                "session-leak-marker",
                "visitor-leak-marker",
                "server-leak-marker",
                "secret-leak-marker",
            )
        }
    }

    @Test
    fun reflectiveBridgePreservesTargetExceptionsCancellationAndJvmErrors() {
        val bridge = ReflectiveGoMobileFrpcStcpVisitorBridge(ThrowingBridge::class.java.name)
        val targetFailures = listOf(
            IllegalArgumentException("target-message-leak-marker"),
            CancellationException("cancellation-message-leak-marker"),
            AssertionError("jvm-error-message-leak-marker"),
        )

        targetFailures.forEach { expected ->
            ThrowingBridge.failure = expected
            val actual = failureOf { bridge.startSession("json-leak-marker") }
            assertSame(expected, actual)
        }
    }

    @Test
    fun getStateDecodesUnknownFields() = runTest {
        val bridge = FakeBridge(
            stateJson = """{
                "sessionId":"session-1",
                "phase":"open",
                "configRevision":7,
                "controlEpoch":3,
                "visitors":{
                    "visitor-1":{
                        "desiredRevision":7,
                        "phase":"ready",
                        "listenerBound":true,
                        "boundControlEpoch":3
                    }
                },
                "extra":"ignored"
            }""".trimIndent(),
        )
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        val state = client.getState("session-1")

        assertEquals("open", state.phase)
        assertEquals(7, state.configRevision)
        assertEquals(3, state.controlEpoch)
        assertTrue(state.visitors.getValue("visitor-1").listenerBound)
    }

    @Test
    fun stopMethodsDelegateToBridge() = runTest {
        val bridge = FakeBridge()
        val client = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

        client.stopVisitor("session-1", "visitor-1")
        val stopped = client.stopSession("session-1", timeoutMillis = 12_000)

        assertTrue(bridge.stoppedVisitors.contains("session-1" to "visitor-1"))
        assertTrue(bridge.stoppedSessions.contains("session-1" to 12_000L))
        assertEquals(FrpcStopSessionResult("session-1", "closed"), stopped)
    }

    @Test
    fun runtimeDiagnosticsDecodeAggregateCounts() = runTest {
        val client = GoMobileFrpcStcpVisitorClient(json, FakeBridge(), Unit)

        assertEquals(
            FrpcRuntimeDiagnostics(
                goroutineCount = 7,
                sessionCount = 2,
                ownerCounts = mapOf("client.controls" to 1L, "visitor.handlers" to 0L),
                runtimeCounts = mapOf("/sched/goroutines:goroutines" to 7L),
            ),
            client.captureRuntimeDiagnostics(),
        )
    }

    private class FakeBridge(
        private val ensureJson: String = """{"bindPort":4096,"desiredRevision":1}""",
        private val readyJson: String = """{
            "name":"visitor-1",
            "desiredRevision":1,
            "phase":"ready",
            "listenerBound":true,
            "boundControlEpoch":1
        }""".trimIndent(),
        private val stateJson: String = """{"sessionId":"session-1","phase":"closed"}""",
    ) : GoMobileFrpcStcpVisitorBridge {
        var startedConfigJson: String? = null
        var ensuredSessionId: String? = null
        var ensuredVisitorJson: String? = null
        val readyCalls = mutableListOf<ReadyCall>()
        val stoppedVisitors = mutableListOf<Pair<String, String>>()
        val stoppedSessions = mutableListOf<Pair<String, Long>>()
        var ensureFailure: Throwable? = null
        var readyFailure: Throwable? = null
        var beforeEnsure: (() -> Unit)? = null

        override fun startSession(configJson: String): String {
            startedConfigJson = configJson
            return "session-1"
        }

        override fun ensureVisitor(sessionId: String, visitorJson: String): String {
            beforeEnsure?.invoke()
            ensureFailure?.let { throw it }
            ensuredSessionId = sessionId
            ensuredVisitorJson = visitorJson
            return ensureJson
        }

        override fun waitVisitorReady(
            sessionId: String,
            visitorName: String,
            desiredRevision: Long,
            timeoutMillis: Long,
        ): String {
            readyFailure?.let { throw it }
            readyCalls += ReadyCall(sessionId, visitorName, desiredRevision, timeoutMillis)
            return readyJson
        }

        override fun stopVisitor(sessionId: String, visitorName: String) {
            stoppedVisitors += sessionId to visitorName
        }

        override fun stopSession(sessionId: String, timeoutMillis: Long): String {
            stoppedSessions += sessionId to timeoutMillis
            return """{"sessionId":"$sessionId","phase":"closed"}"""
        }

        override fun getState(sessionId: String): String = stateJson

        override fun getRuntimeDiagnostics(): String =
            """{"goroutineCount":7,"sessionCount":2,"ownerCounts":{"client.controls":1,"visitor.handlers":0},"runtimeCounts":{"/sched/goroutines:goroutines":7}}"""
    }

    private data class ReadyCall(
        val sessionId: String,
        val visitorName: String,
        val desiredRevision: Long,
        val timeoutMillis: Long,
    )

    private class MissingMethodsBridge

    private class ThrowingBridge {
        companion object {
            lateinit var failure: Throwable

            @JvmStatic
            fun startSession(@Suppress("UNUSED_PARAMETER") configJson: String): String = throw failure
        }
    }

    private class IdentityBridgeException(
        message: String,
        @Suppress("UNUSED_PARAMETER") identity: Unit,
    ) : Exception(message)

    private class BridgeTestCancellationException(
        message: String,
        @Suppress("UNUSED_PARAMETER") identity: Unit,
    ) : CancellationException(message)

    private class BridgeTestJvmError(
        @Suppress("UNUSED_PARAMETER") identity: Unit,
    ) : Error()

    private fun assertOmits(summary: String, vararg sensitiveValues: String) {
        sensitiveValues.forEach { sensitiveValue ->
            assertFalse("Summary leaked sensitive data", summary.contains(sensitiveValue))
        }
    }

    private fun assertSanitizedBindFailure(failure: Throwable, sensitiveValue: String) {
        assertTrue(failure is BindException)
        assertNull(failure.message)
        assertNull(failure.cause)
        assertOmits(failure.toString(), sensitiveValue, "address already in use", "eaddrinuse")
    }

    private fun failureOf(block: () -> Unit): Throwable {
        var failure: Throwable? = null
        try {
            block()
        } catch (caught: Throwable) {
            failure = caught
        }
        return failure ?: throw AssertionError("Expected failure")
    }

    private suspend fun suspendFailureOf(block: suspend () -> Unit): Throwable {
        var failure: Throwable? = null
        try {
            block()
        } catch (caught: Throwable) {
            failure = caught
        }
        return failure ?: throw AssertionError("Expected failure")
    }
}
