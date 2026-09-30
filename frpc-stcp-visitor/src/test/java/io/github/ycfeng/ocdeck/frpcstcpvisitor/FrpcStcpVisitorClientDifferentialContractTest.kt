package io.github.ycfeng.ocdeck.frpcstcpvisitor

import java.net.BindException
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.junit.Assert.assertEquals
import org.junit.Test

class FrpcStcpVisitorClientDifferentialContractTest {
    @Test
    fun lifecycleRevisionReadinessAndMissingOperationsMatch() = runBlocking {
        assertDifferential(
            expected = expectedLifecycleObservation(),
            scenario = ::observeLifecycle,
        )
    }

    @Test
    fun bindConflictIsNormalizedAndDiagnosticsRemainSecretFree() = runBlocking {
        assertDifferential(
            expected = expectedBindConflictObservation(),
            scenario = ::observeBindConflict,
        )
    }

    @Test
    fun callerCancellationRemainsCancellationWithoutMutatingRuntimeState() = runBlocking {
        assertDifferential(
            expected = expectedCancellationObservation(),
            scenario = ::observeCancellation,
        )
    }

    private suspend fun observeLifecycle(
        fixture: FrpcStcpVisitorClientContractFixture,
    ): LifecycleObservation {
        val client = fixture.client
        val missingSessionId = "k4-missing-session"
        val missingState = client.getState(missingSessionId).normalized()
        client.stopVisitor(missingSessionId, "k4-missing-visitor")
        val missingStop = client.stopSession(missingSessionId, timeoutMillis = 1_000L).phase

        val sessionId = client.startSession(k4SessionConfig())
        val started = client.getState(sessionId).normalized()
        val firstConfig = k4VisitorConfig(
            name = K4_PRIMARY_VISITOR,
            secret = K4_PRIMARY_SECRET,
            bindPort = K4_PRIMARY_BIND_PORT,
        )
        val first = client.ensureVisitor(sessionId, firstConfig)
        val firstPending = client.getState(sessionId).normalized()

        val same = client.ensureVisitor(
            sessionId,
            firstConfig.copy(
                name = K4_PRIMARY_VISITOR,
                serverName = K4_SERVER_NAME,
                serverUser = K4_SERVER_USER,
                secretKey = K4_PRIMARY_SECRET,
                bindAddr = "192.0.2.10",
            ),
        )
        val samePending = client.getState(sessionId).normalized()

        fixture.openControl(controlEpoch = K4_CONTROL_EPOCH)
        val firstBind = fixture.awaitBindAttempt(K4_PRIMARY_BIND_PORT)
        val firstReady = client.waitVisitorReady(
            sessionId = sessionId,
            visitorName = "  $K4_PRIMARY_VISITOR  ",
            desiredRevision = first.desiredRevision,
            timeoutMillis = 5_000L,
        ).normalized()
        val firstOpen = client.getState(sessionId).normalized()

        val replacement = client.ensureVisitor(
            sessionId,
            firstConfig.copy(
                name = K4_PRIMARY_VISITOR,
                serverName = K4_SERVER_NAME,
                serverUser = K4_SERVER_USER,
                secretKey = K4_REPLACEMENT_SECRET,
                bindAddr = "198.51.100.20",
                useEncryption = true,
                useCompression = true,
            ),
        )
        val replacementBind = fixture.awaitBindAttempt(K4_PRIMARY_BIND_PORT)
        val replacementReady = client.waitVisitorReady(
            sessionId = sessionId,
            visitorName = K4_PRIMARY_VISITOR,
            desiredRevision = replacement.desiredRevision,
            timeoutMillis = 5_000L,
        ).normalized()
        val replacementOpen = client.getState(sessionId).normalized()

        client.stopVisitor(sessionId, "  $K4_PRIMARY_VISITOR  ")
        val afterStopVisitor = client.getState(sessionId).normalized()
        client.stopVisitor(sessionId, "k4-missing-visitor")
        val afterMissingVisitorStop = client.getState(sessionId).normalized()
        val stopped = client.stopSession(sessionId, timeoutMillis = 5_000L).phase
        val stoppedAgain = client.stopSession(sessionId, timeoutMillis = 5_000L).phase
        val closedState = client.getState(sessionId).normalized()

        return LifecycleObservation(
            missingState = missingState,
            missingStopPhase = missingStop,
            started = started,
            firstEnsure = first.normalized(),
            firstPending = firstPending,
            sameEnsure = same.normalized(),
            samePending = samePending,
            firstBind = firstBind,
            firstReady = firstReady,
            firstOpen = firstOpen,
            replacementEnsure = replacement.normalized(),
            replacementBind = replacementBind,
            replacementReady = replacementReady,
            replacementOpen = replacementOpen,
            afterStopVisitor = afterStopVisitor,
            afterMissingVisitorStop = afterMissingVisitorStop,
            stoppedPhase = stopped,
            stoppedAgainPhase = stoppedAgain,
            closedState = closedState,
        )
    }

    private suspend fun observeBindConflict(
        fixture: FrpcStcpVisitorClientContractFixture,
    ): BindConflictObservation {
        val client = fixture.client
        val sessionConfig = k4SessionConfig()
        val visitorConfig = k4VisitorConfig(
            name = K4_CONFLICT_VISITOR,
            secret = K4_CONFLICT_SECRET,
            bindPort = K4_CONFLICT_BIND_PORT,
        )
        val sessionId = client.startSession(sessionConfig)
        fixture.openControl(K4_FAILURE_CONTROL_EPOCH)
        fixture.failNextBindWithConflict()

        val ensured = client.ensureVisitor(sessionId, visitorConfig)
        val bind = fixture.awaitBindAttempt(K4_CONFLICT_BIND_PORT)
        val failure = captureFailure {
            client.waitVisitorReady(
                sessionId = sessionId,
                visitorName = K4_CONFLICT_VISITOR,
                desiredRevision = ensured.desiredRevision,
                timeoutMillis = 5_000L,
            )
        }
        val state = client.getState(sessionId)
        val runtime = state.visitors.getValue(K4_CONFLICT_VISITOR)
        val summaries = listOf(
            sessionConfig.toString(),
            visitorConfig.toString(),
            ensured.toString(),
            runtime.toString(),
            state.toString(),
            failure.toString(),
            client.toString(),
        )
        val sensitiveValues = listOf(
            K4_SESSION_ENDPOINT,
            K4_SESSION_TOKEN,
            K4_SESSION_USER,
            K4_CONFLICT_VISITOR,
            K4_SERVER_NAME,
            K4_SERVER_USER,
            K4_CONFLICT_SECRET,
        )
        val rawErrors = buildList {
            state.lastError?.let(::add)
            state.visitors.values.mapNotNullTo(this) { it.lastError }
        }

        client.stopSession(sessionId, timeoutMillis = 5_000L)
        return BindConflictObservation(
            ensured = ensured.normalized(),
            bind = bind,
            failureKind = failure.normalizedFailureKind(),
            failureCauseAbsent = failure.cause == null,
            state = state.normalized(),
            structuredDiagnostics =
                sessionConfig.toString().contains("authToken=<redacted>") &&
                    visitorConfig.toString().contains("secretKey=<redacted>") &&
                    runtime.toString().contains("lastErrorPresent=true") &&
                    state.toString().contains("visitorCount=1"),
            summariesSecretFree = summaries.none { summary ->
                sensitiveValues.any(summary::contains)
            },
            errorPayloadSecretFree = rawErrors.none { error ->
                listOf(K4_SESSION_TOKEN, K4_CONFLICT_SECRET).any(error::contains)
            },
        )
    }

    private suspend fun observeCancellation(
        fixture: FrpcStcpVisitorClientContractFixture,
    ): CancellationObservation {
        val client = fixture.client
        val sessionId = client.startSession(k4SessionConfig())
        val ensured = client.ensureVisitor(
            sessionId,
            k4VisitorConfig(
                name = K4_PRIMARY_VISITOR,
                secret = K4_PRIMARY_SECRET,
                bindPort = K4_CANCELLATION_BIND_PORT,
            ),
        )
        fixture.armFutureReadinessWait()
        val cancellation = CancellationException(K4_CANCELLATION_MARKER)
        val actual = supervisorScope {
            val waiter = async(start = CoroutineStart.UNDISPATCHED) {
                client.waitVisitorReady(
                    sessionId = sessionId,
                    visitorName = K4_PRIMARY_VISITOR,
                    desiredRevision = ensured.desiredRevision + 1L,
                    timeoutMillis = 120_000L,
                )
            }
            try {
                fixture.awaitFutureReadinessWait()
                waiter.cancel(cancellation)
                fixture.releaseFutureReadinessWait()
                try {
                    waiter.await()
                    throw AssertionError("Cancelled readiness wait unexpectedly completed")
                } catch (failure: CancellationException) {
                    failure
                }
            } finally {
                fixture.releaseFutureReadinessWait()
            }
        }
        val state = client.getState(sessionId).normalized()
        client.stopSession(sessionId, timeoutMillis = 5_000L)

        return CancellationObservation(
            failureKind = actual.normalizedFailureKind(),
            cancellationIdentityPreserved = actual.containsCauseIdentity(cancellation),
            diagnosticSecretFree = listOf(K4_SESSION_TOKEN, K4_PRIMARY_SECRET).none(actual.toString()::contains),
            state = state,
        )
    }

    private suspend fun <T> assertDifferential(
        expected: T,
        scenario: suspend (FrpcStcpVisitorClientContractFixture) -> T,
    ) {
        val goAdapter = observeFixture(GoAdapterContractFixture(), scenario)
        val kotlinRuntime = observeFixture(KotlinRuntimeContractFixture(), scenario)

        assertEquals("Go adapter seam contract changed", expected, goAdapter)
        assertEquals("Kotlin runtime contract changed", expected, kotlinRuntime)
        assertEquals("Adapter/runtime public semantics diverged", goAdapter, kotlinRuntime)
    }

    private suspend fun <T> observeFixture(
        fixture: FrpcStcpVisitorClientContractFixture,
        scenario: suspend (FrpcStcpVisitorClientContractFixture) -> T,
    ): T = try {
        scenario(fixture)
    } finally {
        fixture.shutdown()
    }

    private fun expectedLifecycleObservation(): LifecycleObservation {
        val closed = NormalizedState("closed", 0L, 0L, false, emptyMap())
        val pendingRevisionOne = normalizedVisitor(1L, "pending", listenerBound = false, controlEpoch = 0L)
        val readyRevisionOne = normalizedVisitor(
            1L,
            "ready",
            listenerBound = true,
            controlEpoch = K4_CONTROL_EPOCH,
        )
        val readyRevisionThree = normalizedVisitor(
            3L,
            "ready",
            listenerBound = true,
            controlEpoch = K4_CONTROL_EPOCH,
        )
        val successfulBind = K4BindObservation(
            loopback = true,
            requestedPort = K4_PRIMARY_BIND_PORT,
            listenerPort = K4_PRIMARY_BIND_PORT,
            listenerCreated = true,
        )
        return LifecycleObservation(
            missingState = closed,
            missingStopPhase = "closed",
            started = NormalizedState("connecting", 0L, 0L, false, emptyMap()),
            firstEnsure = NormalizedEnsure(K4_PRIMARY_BIND_PORT, 1L),
            firstPending = NormalizedState(
                "connecting",
                1L,
                0L,
                false,
                mapOf(K4_PRIMARY_VISITOR to pendingRevisionOne),
            ),
            sameEnsure = NormalizedEnsure(K4_PRIMARY_BIND_PORT, 1L),
            samePending = NormalizedState(
                "connecting",
                2L,
                0L,
                false,
                mapOf(K4_PRIMARY_VISITOR to pendingRevisionOne),
            ),
            firstBind = successfulBind,
            firstReady = NormalizedReady(
                K4_PRIMARY_VISITOR,
                1L,
                "ready",
                listenerBound = true,
                boundControlEpoch = K4_CONTROL_EPOCH,
                lastErrorPresent = false,
            ),
            firstOpen = NormalizedState(
                "open",
                2L,
                K4_CONTROL_EPOCH,
                false,
                mapOf(K4_PRIMARY_VISITOR to readyRevisionOne),
            ),
            replacementEnsure = NormalizedEnsure(K4_PRIMARY_BIND_PORT, 3L),
            replacementBind = successfulBind,
            replacementReady = NormalizedReady(
                K4_PRIMARY_VISITOR,
                3L,
                "ready",
                listenerBound = true,
                boundControlEpoch = K4_CONTROL_EPOCH,
                lastErrorPresent = false,
            ),
            replacementOpen = NormalizedState(
                "open",
                3L,
                K4_CONTROL_EPOCH,
                false,
                mapOf(K4_PRIMARY_VISITOR to readyRevisionThree),
            ),
            afterStopVisitor = NormalizedState("open", 4L, K4_CONTROL_EPOCH, false, emptyMap()),
            afterMissingVisitorStop = NormalizedState("open", 4L, K4_CONTROL_EPOCH, false, emptyMap()),
            stoppedPhase = "closed",
            stoppedAgainPhase = "closed",
            closedState = closed,
        )
    }

    private fun expectedBindConflictObservation(): BindConflictObservation = BindConflictObservation(
        ensured = NormalizedEnsure(K4_CONFLICT_BIND_PORT, 1L),
        bind = K4BindObservation(
            loopback = true,
            requestedPort = K4_CONFLICT_BIND_PORT,
            listenerPort = K4_CONFLICT_BIND_PORT,
            listenerCreated = false,
        ),
        failureKind = NormalizedFailureKind.BIND_CONFLICT,
        failureCauseAbsent = true,
        state = NormalizedState(
            phase = "open",
            configRevision = 1L,
            controlEpoch = K4_FAILURE_CONTROL_EPOCH,
            lastErrorPresent = false,
            visitors = mapOf(
                K4_CONFLICT_VISITOR to normalizedVisitor(
                    desiredRevision = 1L,
                    phase = "failed",
                    listenerBound = false,
                    controlEpoch = 0L,
                    lastErrorPresent = true,
                ),
            ),
        ),
        structuredDiagnostics = true,
        summariesSecretFree = true,
        errorPayloadSecretFree = true,
    )

    private fun expectedCancellationObservation(): CancellationObservation = CancellationObservation(
        failureKind = NormalizedFailureKind.CANCELLED,
        cancellationIdentityPreserved = true,
        diagnosticSecretFree = true,
        state = NormalizedState(
            phase = "connecting",
            configRevision = 1L,
            controlEpoch = 0L,
            lastErrorPresent = false,
            visitors = mapOf(
                K4_PRIMARY_VISITOR to normalizedVisitor(
                    desiredRevision = 1L,
                    phase = "pending",
                    listenerBound = false,
                    controlEpoch = 0L,
                ),
            ),
        ),
    )

    private fun normalizedVisitor(
        desiredRevision: Long,
        phase: String,
        listenerBound: Boolean,
        controlEpoch: Long,
        lastErrorPresent: Boolean = false,
    ): NormalizedVisitor = NormalizedVisitor(
        desiredRevision = desiredRevision,
        phase = phase,
        listenerBound = listenerBound,
        boundControlEpoch = controlEpoch,
        lastErrorPresent = lastErrorPresent,
    )

    private fun FrpcEnsureVisitorResult.normalized(): NormalizedEnsure =
        NormalizedEnsure(bindPort, desiredRevision)

    private fun FrpcVisitorReadyResult.normalized(): NormalizedReady = NormalizedReady(
        name = name,
        desiredRevision = desiredRevision,
        phase = phase,
        listenerBound = listenerBound,
        boundControlEpoch = boundControlEpoch,
        lastErrorPresent = lastError != null,
    )

    private fun FrpcStcpVisitorState.normalized(): NormalizedState = NormalizedState(
        phase = phase,
        configRevision = configRevision,
        controlEpoch = controlEpoch,
        lastErrorPresent = lastError != null,
        visitors = visitors.toSortedMap().mapValues { (_, visitor) ->
            NormalizedVisitor(
                desiredRevision = visitor.desiredRevision,
                phase = visitor.phase,
                listenerBound = visitor.listenerBound,
                boundControlEpoch = visitor.boundControlEpoch,
                lastErrorPresent = visitor.lastError != null,
            )
        },
    )

    private fun Throwable.normalizedFailureKind(): NormalizedFailureKind = when (this) {
        is BindException -> NormalizedFailureKind.BIND_CONFLICT
        is KotlinFrpcStcpVisitorException -> when (failure) {
            KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT -> NormalizedFailureKind.BIND_CONFLICT
            else -> NormalizedFailureKind.OTHER
        }
        is CancellationException -> NormalizedFailureKind.CANCELLED
        else -> NormalizedFailureKind.OTHER
    }

    private fun Throwable.containsCauseIdentity(expected: Throwable): Boolean {
        val seen = Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())
        var current: Throwable? = this
        while (current != null && seen.add(current)) {
            if (current === expected) return true
            current = current.cause
        }
        return false
    }

    private suspend fun captureFailure(block: suspend () -> Unit): Throwable = try {
        block()
        throw AssertionError("Operation unexpectedly succeeded")
    } catch (failure: AssertionError) {
        throw failure
    } catch (failure: Throwable) {
        failure
    }

    private data class LifecycleObservation(
        val missingState: NormalizedState,
        val missingStopPhase: String,
        val started: NormalizedState,
        val firstEnsure: NormalizedEnsure,
        val firstPending: NormalizedState,
        val sameEnsure: NormalizedEnsure,
        val samePending: NormalizedState,
        val firstBind: K4BindObservation,
        val firstReady: NormalizedReady,
        val firstOpen: NormalizedState,
        val replacementEnsure: NormalizedEnsure,
        val replacementBind: K4BindObservation,
        val replacementReady: NormalizedReady,
        val replacementOpen: NormalizedState,
        val afterStopVisitor: NormalizedState,
        val afterMissingVisitorStop: NormalizedState,
        val stoppedPhase: String,
        val stoppedAgainPhase: String,
        val closedState: NormalizedState,
    )

    private data class BindConflictObservation(
        val ensured: NormalizedEnsure,
        val bind: K4BindObservation,
        val failureKind: NormalizedFailureKind,
        val failureCauseAbsent: Boolean,
        val state: NormalizedState,
        val structuredDiagnostics: Boolean,
        val summariesSecretFree: Boolean,
        val errorPayloadSecretFree: Boolean,
    )

    private data class CancellationObservation(
        val failureKind: NormalizedFailureKind,
        val cancellationIdentityPreserved: Boolean,
        val diagnosticSecretFree: Boolean,
        val state: NormalizedState,
    )

    private data class NormalizedEnsure(
        val bindPort: Int,
        val desiredRevision: Long,
    )

    private data class NormalizedReady(
        val name: String,
        val desiredRevision: Long,
        val phase: String,
        val listenerBound: Boolean,
        val boundControlEpoch: Long,
        val lastErrorPresent: Boolean,
    )

    private data class NormalizedState(
        val phase: String,
        val configRevision: Long,
        val controlEpoch: Long,
        val lastErrorPresent: Boolean,
        val visitors: Map<String, NormalizedVisitor>,
    )

    private data class NormalizedVisitor(
        val desiredRevision: Long,
        val phase: String,
        val listenerBound: Boolean,
        val boundControlEpoch: Long,
        val lastErrorPresent: Boolean,
    )

    private enum class NormalizedFailureKind {
        BIND_CONFLICT,
        CANCELLED,
        OTHER,
    }

    private companion object {
        const val K4_PRIMARY_BIND_PORT = 51_100
        const val K4_CONFLICT_BIND_PORT = 51_101
        const val K4_CANCELLATION_BIND_PORT = 51_102
        const val K4_CONTROL_EPOCH = 7L
        const val K4_FAILURE_CONTROL_EPOCH = 9L
        const val K4_CANCELLATION_MARKER = "k4-caller-cancellation"
    }
}
