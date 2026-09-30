package io.github.ycfeng.ocdeck.frpcstcpvisitor

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.jsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class FrpcStcpVisitorSerializationContractTest {
    private val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
        encodeDefaults = false
    }

    @Test
    fun suspendApiContractCanBeImplementedAndCalledByRecordingFake() = runTest {
        val fake = RecordingFake()
        val client: FrpcStcpVisitorClient = fake
        val sessionConfig = FrpcSessionConfig(
            serverAddr = "frps-api-fixture.invalid",
            serverPort = 7100,
            authToken = "api-token-fixture",
            user = "api-user-fixture",
            transportProtocol = "kcp",
            wireProtocol = "v2",
        )
        val visitorConfig = FrpcStcpVisitorConfig(
            name = "api-visitor-fixture",
            serverName = "api-server-fixture",
            serverUser = "api-server-user-fixture",
            secretKey = "api-secret-fixture",
            bindAddr = "127.0.0.2",
            bindPort = 4100,
            useEncryption = true,
            useCompression = true,
        )

        val sessionId = client.startSession(config = sessionConfig)
        val ensured = client.ensureVisitor(
            sessionId = sessionId,
            visitor = visitorConfig,
        )
        val ready = client.waitVisitorReady(
            sessionId = sessionId,
            visitorName = visitorConfig.name,
            desiredRevision = ensured.desiredRevision,
            timeoutMillis = 8_000L,
        )
        client.stopVisitor(
            sessionId = sessionId,
            visitorName = visitorConfig.name,
        )
        val stopped = client.stopSession(
            sessionId = sessionId,
            timeoutMillis = 12_000L,
        )
        val state = client.getState(sessionId = sessionId)

        assertEquals(fake.returnedSessionId, sessionId)
        assertEquals(FrpcEnsureVisitorResult(bindPort = 4100, desiredRevision = 41L), ensured)
        assertEquals(
            FrpcVisitorReadyResult(
                name = visitorConfig.name,
                desiredRevision = 41L,
                phase = "ready",
                listenerBound = true,
                boundControlEpoch = 9L,
            ),
            ready,
        )
        assertEquals(FrpcStopSessionResult(sessionId = sessionId, phase = "closed"), stopped)
        assertEquals(
            FrpcStcpVisitorState(
                sessionId = sessionId,
                phase = "open",
                configRevision = 41L,
                controlEpoch = 9L,
            ),
            state,
        )
        assertEquals(sessionConfig, fake.startedConfig)
        assertEquals(EnsureCall(sessionId, visitorConfig), fake.ensureCall)
        assertEquals(ReadyCall(sessionId, visitorConfig.name, 41L, 8_000L), fake.readyCall)
        assertEquals(sessionId to visitorConfig.name, fake.stopVisitorCall)
        assertEquals(sessionId to 12_000L, fake.stopSessionCall)
        assertEquals(sessionId, fake.getStateSessionId)
        assertEquals(
            listOf(
                "startSession",
                "ensureVisitor",
                "waitVisitorReady",
                "stopVisitor",
                "stopSession",
                "getState",
            ),
            fake.calledMethods,
        )
    }

    @Test
    fun defaultStopTimeoutIsTenSecondsAndUsedByTheDefaultArgument() = runTest {
        val fake = RecordingFake()
        val client: FrpcStcpVisitorClient = fake

        client.stopSession(sessionId = "default-timeout-session-fixture")

        assertEquals(10_000L, FrpcStcpVisitorClient.DEFAULT_STOP_TIMEOUT_MILLIS)
        assertEquals(
            "default-timeout-session-fixture" to 10_000L,
            fake.stopSessionCall,
        )
    }

    @Test
    fun sessionAndVisitorDefaultsAreStableAndPortsAreExplicitlyEncoded() {
        val session = FrpcSessionConfig(
            serverAddr = "frps-default-fixture.invalid",
            authToken = "default-token-fixture",
        )
        val visitor = FrpcStcpVisitorConfig(
            name = "default-visitor-fixture",
            serverName = "default-server-fixture",
            secretKey = "default-secret-fixture",
        )

        assertEquals(7000, session.serverPort)
        assertNull(session.user)
        assertEquals("tcp", session.transportProtocol)
        assertEquals("v1", session.wireProtocol)
        assertEquals("127.0.0.1", visitor.bindAddr)
        assertEquals(4096, visitor.bindPort)
        assertNull(visitor.serverUser)
        assertFalse(visitor.useEncryption)
        assertFalse(visitor.useCompression)

        val encodedSession = encodeObject(session)
        val encodedVisitor = encodeObject(visitor)
        assertEquals(JsonPrimitive(7000), encodedSession["serverPort"])
        assertEquals(JsonPrimitive(4096), encodedVisitor["bindPort"])
        assertEquals(session, json.decodeFromString<FrpcSessionConfig>(encodedSession.toString()))
        assertEquals(visitor, json.decodeFromString<FrpcStcpVisitorConfig>(encodedVisitor.toString()))
    }

    @Test
    fun dtoFieldNamesAndRoundTripsAreStable() {
        val runtime = FrpcVisitorRuntimeState(
            desiredRevision = 4_294_967_297L,
            phase = "ready",
            listenerBound = true,
            boundControlEpoch = 8_589_934_593L,
            lastError = "runtime-error-fixture",
        )

        assertFieldsAndRoundTrip(
            value = FrpcSessionConfig(
                serverAddr = "frps-fields-fixture.invalid",
                serverPort = 7100,
                authToken = "fields-token-fixture",
                user = "fields-user-fixture",
                transportProtocol = "quic",
                wireProtocol = "v2",
            ),
            expectedFields = setOf(
                "serverAddr",
                "serverPort",
                "authToken",
                "user",
                "transportProtocol",
                "wireProtocol",
            ),
        )
        assertFieldsAndRoundTrip(
            value = FrpcStcpVisitorConfig(
                name = "fields-visitor-fixture",
                serverName = "fields-server-fixture",
                serverUser = "fields-server-user-fixture",
                secretKey = "fields-secret-fixture",
                bindAddr = "127.0.0.2",
                bindPort = 4100,
                useEncryption = true,
                useCompression = true,
            ),
            expectedFields = setOf(
                "name",
                "serverName",
                "serverUser",
                "secretKey",
                "bindAddr",
                "bindPort",
                "useEncryption",
                "useCompression",
            ),
        )
        assertFieldsAndRoundTrip(
            value = FrpcEnsureVisitorResult(
                bindPort = 4100,
                desiredRevision = 4_294_967_297L,
            ),
            expectedFields = setOf("bindPort", "desiredRevision"),
        )
        assertFieldsAndRoundTrip(
            value = FrpcVisitorReadyResult(
                name = "fields-visitor-fixture",
                desiredRevision = 4_294_967_297L,
                phase = "ready",
                listenerBound = true,
                boundControlEpoch = 8_589_934_593L,
                lastError = "ready-error-fixture",
            ),
            expectedFields = setOf(
                "name",
                "desiredRevision",
                "phase",
                "listenerBound",
                "boundControlEpoch",
                "lastError",
            ),
        )
        assertFieldsAndRoundTrip(
            value = runtime,
            expectedFields = setOf(
                "desiredRevision",
                "phase",
                "listenerBound",
                "boundControlEpoch",
                "lastError",
            ),
        )
        assertFieldsAndRoundTrip(
            value = FrpcStcpVisitorState(
                sessionId = "fields-session-fixture",
                phase = "open",
                configRevision = 4_294_967_299L,
                controlEpoch = 8_589_934_593L,
                lastError = "state-error-fixture",
                visitors = mapOf("fields-visitor-fixture" to runtime),
            ),
            expectedFields = setOf(
                "sessionId",
                "phase",
                "configRevision",
                "controlEpoch",
                "lastError",
                "visitors",
            ),
        )
        assertFieldsAndRoundTrip(
            value = FrpcStopSessionResult(
                sessionId = "fields-session-fixture",
                phase = "closed",
            ),
            expectedFields = setOf("sessionId", "phase"),
        )
    }

    @Test
    fun canonicalBridgeDtoJsonIsConsumedByKotlinTypes() {
        val contract = json.parseToJsonElement(readBridgeDtoContract()).jsonObject
        assertEquals(
            setOf(
                "sessionConfig",
                "sessionConfigDefaults",
                "visitorConfig",
                "visitorConfigDefaults",
                "ensureVisitorResult",
                "visitorReadyResult",
                "visitorReadyResultWithoutError",
                "visitorRuntimeState",
                "visitorRuntimeStateWithoutError",
                "state",
                "stateWithoutOptionalFields",
                "stopSessionResult",
            ),
            contract.keys,
        )

        val session = json.decodeFromJsonElement<FrpcSessionConfig>(contract.getValue("sessionConfig"))
        val defaultSession = json.decodeFromJsonElement<FrpcSessionConfig>(
            contract.getValue("sessionConfigDefaults"),
        )
        val visitor = json.decodeFromJsonElement<FrpcStcpVisitorConfig>(contract.getValue("visitorConfig"))
        val defaultVisitor = json.decodeFromJsonElement<FrpcStcpVisitorConfig>(
            contract.getValue("visitorConfigDefaults"),
        )
        val ensured = json.decodeFromJsonElement<FrpcEnsureVisitorResult>(
            contract.getValue("ensureVisitorResult"),
        )
        val ready = json.decodeFromJsonElement<FrpcVisitorReadyResult>(
            contract.getValue("visitorReadyResult"),
        )
        val readyWithoutError = json.decodeFromJsonElement<FrpcVisitorReadyResult>(
            contract.getValue("visitorReadyResultWithoutError"),
        )
        val runtime = json.decodeFromJsonElement<FrpcVisitorRuntimeState>(
            contract.getValue("visitorRuntimeState"),
        )
        val runtimeWithoutError = json.decodeFromJsonElement<FrpcVisitorRuntimeState>(
            contract.getValue("visitorRuntimeStateWithoutError"),
        )
        val state = json.decodeFromJsonElement<FrpcStcpVisitorState>(contract.getValue("state"))
        val stateWithoutOptionalFields = json.decodeFromJsonElement<FrpcStcpVisitorState>(
            contract.getValue("stateWithoutOptionalFields"),
        )
        val stopped = json.decodeFromJsonElement<FrpcStopSessionResult>(
            contract.getValue("stopSessionResult"),
        )

        val expectedRuntime = FrpcVisitorRuntimeState(
            desiredRevision = 4_294_967_297L,
            phase = "ready",
            listenerBound = true,
            boundControlEpoch = 8_589_934_593L,
            lastError = "<redacted>",
        )
        assertEquals(
            FrpcSessionConfig(
                serverAddr = "bridge-frps.invalid",
                serverPort = 7000,
                authToken = "<redacted>",
                user = "bridge-user",
                transportProtocol = "tcp",
                wireProtocol = "v2",
            ),
            session,
        )
        assertEquals(
            FrpcSessionConfig(
                serverAddr = "bridge-defaults-frps.invalid",
                serverPort = 7000,
                authToken = "<redacted>",
                user = null,
                transportProtocol = "tcp",
                wireProtocol = "v1",
            ),
            defaultSession,
        )
        assertEquals(
            FrpcStcpVisitorConfig(
                name = "bridge-visitor",
                serverName = "bridge-server",
                serverUser = "bridge-server-user",
                secretKey = "<redacted>",
                bindAddr = "127.0.0.1",
                bindPort = 4096,
                useEncryption = false,
                useCompression = false,
            ),
            visitor,
        )
        assertEquals(
            FrpcStcpVisitorConfig(
                name = "bridge-defaults-visitor",
                serverName = "bridge-defaults-server",
                serverUser = null,
                secretKey = "<redacted>",
                bindAddr = "127.0.0.1",
                bindPort = 4096,
                useEncryption = false,
                useCompression = false,
            ),
            defaultVisitor,
        )
        assertEquals(
            FrpcEnsureVisitorResult(bindPort = 4096, desiredRevision = 4_294_967_297L),
            ensured,
        )
        assertEquals(
            FrpcVisitorReadyResult(
                name = "bridge-visitor",
                desiredRevision = 4_294_967_297L,
                phase = "ready",
                listenerBound = true,
                boundControlEpoch = 8_589_934_593L,
                lastError = "<redacted>",
            ),
            ready,
        )
        assertEquals(
            FrpcVisitorReadyResult(
                name = "bridge-defaults-visitor",
                desiredRevision = 4_294_967_297L,
                phase = "ready",
                listenerBound = true,
                boundControlEpoch = 8_589_934_593L,
                lastError = null,
            ),
            readyWithoutError,
        )
        assertEquals(expectedRuntime, runtime)
        assertEquals(
            FrpcVisitorRuntimeState(
                desiredRevision = 4_294_967_297L,
                phase = "pending",
                listenerBound = false,
                boundControlEpoch = 0L,
                lastError = null,
            ),
            runtimeWithoutError,
        )
        assertEquals(
            FrpcStcpVisitorState(
                sessionId = "bridge-session",
                phase = "open",
                configRevision = 4_294_967_299L,
                controlEpoch = 8_589_934_593L,
                lastError = "<redacted>",
                visitors = mapOf("bridge-visitor" to expectedRuntime),
            ),
            state,
        )
        assertEquals(
            FrpcStcpVisitorState(
                sessionId = "bridge-defaults-session",
                phase = "closed",
                configRevision = 0L,
                controlEpoch = 0L,
                lastError = null,
                visitors = emptyMap(),
            ),
            stateWithoutOptionalFields,
        )
        assertEquals(
            FrpcStopSessionResult(sessionId = "bridge-session", phase = "closed"),
            stopped,
        )
    }

    @Test
    fun nullableAndCollectionDefaultsAreStable() {
        val ready = json.decodeFromString<FrpcVisitorReadyResult>(
            """{
                "name":"nullable-visitor-fixture",
                "desiredRevision":3,
                "phase":"ready",
                "listenerBound":true,
                "boundControlEpoch":2
            }""".trimIndent(),
        )
        val runtime = json.decodeFromString<FrpcVisitorRuntimeState>(
            """{
                "desiredRevision":3,
                "phase":"pending",
                "listenerBound":false,
                "boundControlEpoch":0
            }""".trimIndent(),
        )
        val state = json.decodeFromString<FrpcStcpVisitorState>(
            """{"sessionId":"nullable-session-fixture","phase":"closed"}""",
        )

        assertNull(ready.lastError)
        assertNull(runtime.lastError)
        assertEquals(0L, state.configRevision)
        assertEquals(0L, state.controlEpoch)
        assertNull(state.lastError)
        assertTrue(state.visitors.isEmpty())

        assertFalse("lastError" in encodeObject(ready))
        assertFalse("lastError" in encodeObject(runtime))
        val encodedState = encodeObject(state)
        assertFalse("lastError" in encodedState)
        assertFalse("visitors" in encodedState)
    }

    @Test
    fun sessionAndVisitorPhaseStringSamplesRoundTrip() {
        val sessionPhases = listOf(
            "connecting",
            "open",
            "reconnecting",
            "stopping",
            "closed",
            "failed",
        )
        val visitorPhases = listOf(
            "pending",
            "starting",
            "ready",
            "failed",
            "stopped",
        )

        sessionPhases.forEach { phase ->
            val state = FrpcStcpVisitorState(
                sessionId = "phase-session-fixture",
                phase = phase,
            )
            assertEquals(phase, roundTrip(state).phase)
        }
        visitorPhases.forEach { phase ->
            val state = FrpcVisitorRuntimeState(
                desiredRevision = 1L,
                phase = phase,
                listenerBound = phase == "ready",
                boundControlEpoch = if (phase == "ready") 1L else 0L,
            )
            assertEquals(phase, roundTrip(state).phase)
        }
    }

    @Test
    fun revisionAndControlEpochValuesRoundTripAsLongs() {
        val desiredRevision = 4_294_967_297L
        val configRevision = 4_294_967_299L
        val controlEpoch = 8_589_934_593L
        val ensure = roundTrip(
            FrpcEnsureVisitorResult(
                bindPort = 4096,
                desiredRevision = desiredRevision,
            ),
        )
        val ready = roundTrip(
            FrpcVisitorReadyResult(
                name = "revision-visitor-fixture",
                desiredRevision = desiredRevision,
                phase = "ready",
                listenerBound = true,
                boundControlEpoch = controlEpoch,
            ),
        )
        val state = roundTrip(
            FrpcStcpVisitorState(
                sessionId = "revision-session-fixture",
                phase = "open",
                configRevision = configRevision,
                controlEpoch = controlEpoch,
                visitors = mapOf(
                    "revision-visitor-fixture" to FrpcVisitorRuntimeState(
                        desiredRevision = desiredRevision,
                        phase = "ready",
                        listenerBound = true,
                        boundControlEpoch = controlEpoch,
                    ),
                ),
            ),
        )

        assertEquals(desiredRevision, ensure.desiredRevision)
        assertEquals(desiredRevision, ready.desiredRevision)
        assertEquals(controlEpoch, ready.boundControlEpoch)
        assertEquals(configRevision, state.configRevision)
        assertEquals(controlEpoch, state.controlEpoch)
        assertEquals(
            desiredRevision,
            state.visitors.getValue("revision-visitor-fixture").desiredRevision,
        )
        assertEquals(
            controlEpoch,
            state.visitors.getValue("revision-visitor-fixture").boundControlEpoch,
        )
    }

    @Test
    fun tolerantJsonIgnoresUnknownTopLevelAndNestedFields() {
        val state = json.decodeFromString<FrpcStcpVisitorState>(
            """{
                "sessionId":"unknown-field-session-fixture",
                "phase":"open",
                "configRevision":7,
                "controlEpoch":3,
                "futureSessionField":{"enabled":true},
                "visitors":{
                    "unknown-field-visitor-fixture":{
                        "desiredRevision":7,
                        "phase":"ready",
                        "listenerBound":true,
                        "boundControlEpoch":3,
                        "futureVisitorField":[1,2,3]
                    }
                }
            }""".trimIndent(),
        )
        val session = json.decodeFromString<FrpcSessionConfig>(
            """{
                "serverAddr":"frps-unknown-field-fixture.invalid",
                "serverPort":7000,
                "authToken":"unknown-field-token-fixture",
                "futureConfigField":"ignored"
            }""".trimIndent(),
        )

        assertEquals("open", state.phase)
        assertTrue(state.visitors.getValue("unknown-field-visitor-fixture").listenerBound)
        assertEquals("tcp", session.transportProtocol)
        assertEquals("v1", session.wireProtocol)
    }

    @Test
    fun structuredToStringsOmitSensitiveAndRuntimeValues() {
        val endpoint = "endpoint-leak-marker.invalid"
        val bindEndpoint = "bind-endpoint-leak-marker.invalid"
        val token = "token-leak-marker"
        val secret = "secret-leak-marker"
        val sessionUser = "session-user-leak-marker"
        val serverName = "server-name-leak-marker"
        val serverUser = "server-user-leak-marker"
        val sessionId = "session-id-leak-marker"
        val visitorName = "visitor-name-leak-marker"
        val runtimeError = "runtime-error-leak-marker"
        val session = FrpcSessionConfig(
            serverAddr = endpoint,
            serverPort = 7100,
            authToken = token,
            user = sessionUser,
        )
        val visitor = FrpcStcpVisitorConfig(
            name = visitorName,
            serverName = serverName,
            serverUser = serverUser,
            secretKey = secret,
            bindAddr = bindEndpoint,
            bindPort = 4100,
            useEncryption = true,
            useCompression = true,
        )
        val ready = FrpcVisitorReadyResult(
            name = visitorName,
            desiredRevision = 7L,
            phase = "ready",
            listenerBound = true,
            boundControlEpoch = 3L,
            lastError = runtimeError,
        )
        val runtime = FrpcVisitorRuntimeState(
            desiredRevision = 7L,
            phase = "ready",
            listenerBound = true,
            boundControlEpoch = 3L,
            lastError = runtimeError,
        )
        val state = FrpcStcpVisitorState(
            sessionId = sessionId,
            phase = "open",
            configRevision = 7L,
            controlEpoch = 3L,
            lastError = runtimeError,
            visitors = mapOf(visitorName to runtime),
        )
        val stopped = FrpcStopSessionResult(sessionId = sessionId, phase = "closed")
        val ensure = FrpcEnsureVisitorResult(bindPort = 4100, desiredRevision = 7L)

        val summaries = listOf(session, visitor, ensure, ready, runtime, state, stopped).map(Any::toString)
        summaries.forEach { summary ->
            assertOmits(
                summary,
                endpoint,
                bindEndpoint,
                token,
                secret,
                sessionUser,
                serverName,
                serverUser,
                sessionId,
                visitorName,
                runtimeError,
            )
        }
        assertTrue(session.toString().contains("authToken=<redacted>"))
        assertTrue(visitor.toString().contains("secretKey=<redacted>"))
        assertTrue(ready.toString().contains("lastErrorPresent=true"))
        assertTrue(runtime.toString().contains("lastErrorPresent=true"))
        assertTrue(state.toString().contains("lastErrorPresent=true"))
        assertTrue(state.toString().contains("visitorCount=1"))
    }

    private inline fun <reified T> encodeObject(value: T): JsonObject =
        json.parseToJsonElement(json.encodeToString(value)).jsonObject

    private inline fun <reified T> assertFieldsAndRoundTrip(
        value: T,
        expectedFields: Set<String>,
    ) {
        val encoded = json.encodeToString(value)
        assertEquals(expectedFields, json.parseToJsonElement(encoded).jsonObject.keys)
        assertEquals(value, json.decodeFromString<T>(encoded))
    }

    private inline fun <reified T> roundTrip(value: T): T =
        json.decodeFromString(json.encodeToString(value))

    private fun readBridgeDtoContract(): String {
        val resource = "io/github/ycfeng/ocdeck/frpcstcpvisitor/contract/v1/bridge/dto-contract.json"
        val stream = javaClass.classLoader?.getResourceAsStream(resource)
            ?: throw AssertionError("Missing bridge DTO contract resource")
        val bytes = stream.use { it.readNBytes(MAX_BRIDGE_DTO_CONTRACT_BYTES + 1) }
        assertTrue("Bridge DTO contract resource is too large", bytes.size <= MAX_BRIDGE_DTO_CONTRACT_BYTES)
        return bytes.toString(Charsets.UTF_8)
    }

    private fun assertOmits(summary: String, vararg sensitiveValues: String) {
        sensitiveValues.forEach { sensitiveValue ->
            assertFalse("Summary leaked sensitive data", summary.contains(sensitiveValue))
        }
    }

    private class RecordingFake : FrpcStcpVisitorClient {
        val returnedSessionId = "recording-session-fixture"
        val calledMethods = mutableListOf<String>()
        var startedConfig: FrpcSessionConfig? = null
        var ensureCall: EnsureCall? = null
        var readyCall: ReadyCall? = null
        var stopVisitorCall: Pair<String, String>? = null
        var stopSessionCall: Pair<String, Long>? = null
        var getStateSessionId: String? = null

        override suspend fun startSession(config: FrpcSessionConfig): String {
            calledMethods += "startSession"
            startedConfig = config
            return returnedSessionId
        }

        override suspend fun ensureVisitor(
            sessionId: String,
            visitor: FrpcStcpVisitorConfig,
        ): FrpcEnsureVisitorResult {
            calledMethods += "ensureVisitor"
            ensureCall = EnsureCall(sessionId, visitor)
            return FrpcEnsureVisitorResult(bindPort = visitor.bindPort, desiredRevision = 41L)
        }

        override suspend fun waitVisitorReady(
            sessionId: String,
            visitorName: String,
            desiredRevision: Long,
            timeoutMillis: Long,
        ): FrpcVisitorReadyResult {
            calledMethods += "waitVisitorReady"
            readyCall = ReadyCall(sessionId, visitorName, desiredRevision, timeoutMillis)
            return FrpcVisitorReadyResult(
                name = visitorName,
                desiredRevision = desiredRevision,
                phase = "ready",
                listenerBound = true,
                boundControlEpoch = 9L,
            )
        }

        override suspend fun stopVisitor(sessionId: String, visitorName: String) {
            calledMethods += "stopVisitor"
            stopVisitorCall = sessionId to visitorName
        }

        override suspend fun stopSession(
            sessionId: String,
            timeoutMillis: Long,
        ): FrpcStopSessionResult {
            calledMethods += "stopSession"
            stopSessionCall = sessionId to timeoutMillis
            return FrpcStopSessionResult(sessionId = sessionId, phase = "closed")
        }

        override suspend fun getState(sessionId: String): FrpcStcpVisitorState {
            calledMethods += "getState"
            getStateSessionId = sessionId
            return FrpcStcpVisitorState(
                sessionId = sessionId,
                phase = "open",
                configRevision = 41L,
                controlEpoch = 9L,
            )
        }
    }

    private data class EnsureCall(
        val sessionId: String,
        val visitor: FrpcStcpVisitorConfig,
    )

    private data class ReadyCall(
        val sessionId: String,
        val visitorName: String,
        val desiredRevision: Long,
        val timeoutMillis: Long,
    )

    private companion object {
        const val MAX_BRIDGE_DTO_CONTRACT_BYTES = 64 * 1024
    }
}
