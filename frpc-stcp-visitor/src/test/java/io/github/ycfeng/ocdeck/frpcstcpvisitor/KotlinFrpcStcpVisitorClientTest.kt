package io.github.ycfeng.ocdeck.frpcstcpvisitor

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.compression.FrpSnappyFramedInputStream
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.compression.FrpSnappyFramedOutputStream
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpClientStreamLease
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlIdentity
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlReconnectDiagnostics
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlState
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpUnixTimeSource
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpWireProtocol
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpTimestampAuth
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpV1Cfb
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpV1CfbOutputStream
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV1
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV2
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.NewVisitorConn
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.NewVisitorConnResp
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpLocalConnection
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpLocalListener
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpLocalListenerFactory
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpSessionControl
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpSessionControlFactory
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpRelayVisitorConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpVisitorRelay
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.productionFrpControlConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpMuxStream
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpTransportException
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpTransportFailure
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.BindException
import java.net.InetAddress
import java.util.Collections
import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

class KotlinFrpcStcpVisitorClientTest {
    @Test
    fun publicStateSubscriptionDeliversReadinessRecoveryAndStopWithoutPolling() = runBlocking {
        val controls = FakeControlFactory()
        val listeners = FakeListenerFactory()
        val parent = newParentScope()
        val client = newClient(parentScope = parent, controlFactory = controls, listenerFactory = listeners)
        val id = client.startSession(sessionConfig())
        val control = controls.single()
        val snapshots = Channel<FrpcStcpVisitorState>(Channel.UNLIMITED)
        val collector = launch { client.observeState(id).collect { snapshots.send(it) } }
        suspend fun nextMatching(test: (FrpcStcpVisitorState) -> Boolean): FrpcStcpVisitorState = withTimeout(5_000) {
            var state = snapshots.receive()
            while (!test(state)) state = snapshots.receive()
            state
        }
        try {
            control.open(epoch = 1, transport = 1)
            client.ensureVisitor(id, visitorConfig(name = "visitor", bindPort = 50_071))
            listeners.takeBinding()
            nextMatching { it.visitors["visitor"]?.phase == "ready" }
            control.retry(attempt = 1)
            nextMatching { it.phase == "reconnecting" }
            control.open(epoch = 2, transport = 2)
            listeners.takeBinding()
            nextMatching { it.controlEpoch == 2L && it.visitors["visitor"]?.phase == "ready" }
            control.releaseStop()
            client.stopSession(id, 5_000)
            nextMatching { it.phase == "closed" }
            assertEquals("closed", client.observeState(id).first().phase)
        } finally {
            collector.cancel()
            control.releaseStop()
            client.stopSession(id, 5_000)
            client.close()
            parent.cancel()
        }
    }

    @Test
    fun revisionsValidationFixedBindAndLoopbackOwnershipAreLinearized() = runBlocking {
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val controlStates = Channel<FrpControlState>(Channel.UNLIMITED)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope = parentScope,
            controlFactory = controlFactory,
            listenerFactory = listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                afterControlState = { controlStates.send(it) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            val identity = control.open(epoch = 1L, transport = 1L)
            awaitControlState(controlStates) { it is FrpControlState.Open }

            val invalidPort = captureRuntimeFailure {
                client.ensureVisitor(sessionId, visitorConfig(name = "invalid", bindPort = 0))
            }
            assertEquals(KotlinFrpcStcpVisitorFailure.INVALID_CONFIGURATION, invalidPort.failure)
            assertEquals(0L, client.getState(sessionId).configRevision)

            val firstConfig = visitorConfig(
                name = "first",
                bindAddr = "0.0.0.0",
                bindPort = 50_000,
            )
            val first = client.ensureVisitor(sessionId, firstConfig)
            assertEquals(50_000, first.bindPort)
            assertEquals(1L, first.desiredRevision)
            val firstBinding = listenerFactory.takeBinding()
            assertEquals("127.0.0.1", firstBinding.address.hostAddress)
            assertEquals(50_000, firstBinding.requestedPort)
            assertEquals(50_000, firstBinding.listener.bindPort)
            val invalidWaitTimeout = captureRuntimeFailure {
                client.waitVisitorReady(sessionId, "first", first.desiredRevision, 0L)
            }
            assertEquals(KotlinFrpcStcpVisitorFailure.INVALID_CONFIGURATION, invalidWaitTimeout.failure)
            assertEquals(
                FrpcVisitorReadyResult("first", 1L, "ready", true, identity.controlEpoch),
                client.waitVisitorReady(sessionId, "first", 1L, 1_000L),
            )

            val same = client.ensureVisitor(
                sessionId,
                firstConfig.copy(bindAddr = "192.0.2.1"),
            )
            assertEquals(50_000, same.bindPort)
            assertEquals(1L, same.desiredRevision)
            assertEquals(1, listenerFactory.bindCount.get())
            assertEquals(2L, client.getState(sessionId).configRevision)

            val wrapped = client.ensureVisitor(
                sessionId,
                firstConfig.copy(useEncryption = true, useCompression = true),
            )
            assertEquals(3L, wrapped.desiredRevision)
            val wrappedBinding = listenerFactory.takeBinding()
            assertEquals(1, firstBinding.listener.closeCount.get())
            assertEquals(3L, client.getState(sessionId).configRevision)

            val second = client.ensureVisitor(
                sessionId,
                visitorConfig(name = "second", bindPort = 50_001),
            )
            assertEquals(4L, second.desiredRevision)
            listenerFactory.takeBinding()
            val duplicate = captureRuntimeFailure {
                client.ensureVisitor(
                    sessionId,
                    visitorConfig(name = "duplicate", bindPort = 50_001),
                )
            }
            assertEquals(KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT, duplicate.failure)
            assertEquals(4L, client.getState(sessionId).configRevision)

            client.stopVisitor(sessionId, "missing")
            assertEquals(4L, client.getState(sessionId).configRevision)
            client.stopVisitor(sessionId, "first")
            assertEquals(5L, client.getState(sessionId).configRevision)
            assertEquals(1, wrappedBinding.listener.closeCount.get())
            assertFalse(client.getState(sessionId).visitors.containsKey("first"))
        } finally {
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun readinessRequiresExactRevisionAndRebindsOnTheCurrentControlEpoch() = runBlocking {
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val controlStates = Channel<FrpControlState>(Channel.UNLIMITED)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                afterControlState = { controlStates.send(it) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            awaitControlState(controlStates) { it is FrpControlState.Open }
            val initial = client.ensureVisitor(sessionId, visitorConfig(secret = "secret-one", bindPort = 50_010))
            val firstListener = listenerFactory.takeBinding().listener
            assertEquals(1L, initial.desiredRevision)
            assertEquals(1L, client.waitVisitorReady(sessionId, "visitor", 1L, 1_000L).boundControlEpoch)

            val changed = client.ensureVisitor(sessionId, visitorConfig(secret = "secret-two", bindPort = 50_010))
            val secondListener = listenerFactory.takeBinding().listener
            assertEquals(2L, changed.desiredRevision)
            assertEquals(1, firstListener.closeCount.get())
            val stale = captureRuntimeFailure {
                client.waitVisitorReady(sessionId, "visitor", 1L, 1_000L)
            }
            assertEquals(KotlinFrpcStcpVisitorFailure.VISITOR_SUPERSEDED, stale.failure)

            val futureWait = async {
                captureRuntimeFailure {
                    client.waitVisitorReady(sessionId, "visitor", 3L, 100L)
                }
            }
            val same = client.ensureVisitor(sessionId, visitorConfig(secret = "secret-two", bindPort = 50_010))
            assertEquals(2L, same.desiredRevision)
            assertEquals(KotlinFrpcStcpVisitorFailure.WAIT_TIMEOUT, futureWait.await().failure)

            control.retry(attempt = 1)
            awaitControlState(controlStates) { it is FrpControlState.Retrying }
            assertEquals(1, secondListener.closeCount.get())
            val disconnected = client.getState(sessionId)
            assertEquals("reconnecting", disconnected.phase)
            assertFalse(disconnected.visitors.getValue("visitor").listenerBound)

            val secondIdentity = control.open(epoch = 2L, transport = 2L)
            awaitControlState(controlStates) {
                it is FrpControlState.Open && it.identity == secondIdentity
            }
            listenerFactory.takeBinding()
            val ready = client.waitVisitorReady(sessionId, "visitor", 2L, 1_000L)
            assertEquals(2L, ready.boundControlEpoch)
            assertEquals(3L, client.getState(sessionId).configRevision)
        } finally {
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun liveControlStatePreventsStaleReadinessBeforeActorReconciliation() = runBlocking {
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val blockNextControl = AtomicBoolean(false)
        val blockedControl = CompletableDeferred<Unit>()
        val releaseControl = CompletableDeferred<Unit>()
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                beforeRuntimeCommand = { kind ->
                    if (kind == "ControlChangedCommand" && blockNextControl.compareAndSet(true, false)) {
                        blockedControl.complete(Unit)
                        releaseControl.await()
                    }
                },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_011))
            control.open(epoch = 1L, transport = 1L)
            listenerFactory.takeBinding()
            assertEquals(
                1L,
                client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L).boundControlEpoch,
            )

            blockNextControl.set(true)
            control.retry(attempt = 1)
            withTimeout(5_000L) { blockedControl.await() }
            val disconnected = client.getState(sessionId)
            assertEquals("reconnecting", disconnected.phase)
            assertFalse(disconnected.visitors.getValue("visitor").listenerBound)
            val stale = captureRuntimeFailure {
                client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 25L)
            }
            assertEquals(KotlinFrpcStcpVisitorFailure.WAIT_TIMEOUT, stale.failure)

            control.fail()
            assertEquals("failed", client.getState(sessionId).phase)
            val failed = captureRuntimeFailure {
                client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)
            }
            assertEquals(KotlinFrpcStcpVisitorFailure.CONTROL_FAILED, failed.failure)
        } finally {
            releaseControl.complete(Unit)
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun v1AndV2VisitorPayloadModesPreserveCoalescedDataAndRelayHalfCloses() = runBlocking {
        listOf(FrpWireProtocol.V1, FrpWireProtocol.V2).forEach { protocol ->
            listOf(false, true).forEach { useEncryption ->
                listOf(false, true).forEach { useCompression ->
                    val controlFactory = FakeControlFactory(protocol = protocol)
                    val listenerFactory = FakeListenerFactory()
                    val controlStates = Channel<FrpControlState>(Channel.UNLIMITED)
                    val parentScope = newParentScope()
                    val client = newClient(
                        parentScope,
                        controlFactory,
                        listenerFactory,
                        unixTimeSource = FrpUnixTimeSource { FIXED_TIMESTAMP },
                        hooks = KotlinFrpcRuntimeLifecycleHooks(
                            afterControlState = { controlStates.send(it) },
                        ),
                    )
                    val sessionId = client.startSession(
                        sessionConfig(user = "session-user", wireProtocol = protocol.name.lowercase()),
                    )
                    val control = controlFactory.single()
                    try {
                        control.open(epoch = 1L, transport = 1L)
                        awaitControlState(controlStates) { it is FrpControlState.Open }
                        val ensured = client.ensureVisitor(
                            sessionId,
                            visitorConfig(
                                serverName = "remote-service",
                                serverUser = "server-user",
                                secret = SYNTHETIC_SECRET,
                                bindPort = 50_020,
                                useEncryption = useEncryption,
                                useCompression = useCompression,
                            ),
                        )
                        val listener = listenerFactory.takeBinding().listener
                        client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

                        val proxyName = "server-user.remote-service"
                        val mode = "${protocol.name}-$useEncryption-$useCompression"
                        val serverPayload = "server-payload-$mode".repeat(32).encodeToByteArray()
                        val response = when (protocol) {
                            FrpWireProtocol.V1 -> FrpWireV1.encodeMessage(
                                NewVisitorConnResp(proxyName = "mismatched-response-name"),
                            )
                            FrpWireProtocol.V2 -> FrpWireV2.encodeMessage(
                                NewVisitorConnResp(proxyName = "mismatched-response-name"),
                            )
                        }
                        val wrappedServerPayload = encodeVisitorPayload(
                            serverPayload,
                            useEncryption,
                            useCompression,
                        )
                        val stream = ScriptedMuxStream(response + wrappedServerPayload)
                        control.enqueueStream(stream)
                        val localPayload = "local-payload-$mode".repeat(32).encodeToByteArray()
                        val local = FakeLocalConnection(localPayload)
                        listener.offer(local)
                        withTimeout(5_000L) { stream.resetStarted.await() }

                        val written = ByteArrayInputStream(stream.writtenBytes())
                        if (protocol == FrpWireProtocol.V2) FrpWireV2.readMagic(written)
                        val request = when (protocol) {
                            FrpWireProtocol.V1 -> FrpWireV1.readMessage(written)
                            FrpWireProtocol.V2 -> FrpWireV2.readMessage(written)
                        } as NewVisitorConn
                        assertEquals("run-fixture", request.runId)
                        assertEquals(proxyName, request.proxyName)
                        assertEquals(FIXED_TIMESTAMP, request.timestamp)
                        assertEquals(FrpTimestampAuth.key(SYNTHETIC_SECRET, FIXED_TIMESTAMP), request.signKey)
                        assertEquals(useEncryption, request.useEncryption)
                        assertEquals(useCompression, request.useCompression)
                        assertTrue(
                            localPayload.contentEquals(
                                decodeVisitorPayload(written, useEncryption, useCompression),
                            ),
                        )
                        assertTrue(serverPayload.contentEquals(local.writtenBytes()))
                        assertEquals(1, stream.closeWriteCount.get())
                        assertEquals(1, local.shutdownOutputCount.get())
                        assertEquals(1, stream.resetCount.get())
                        assertEquals("ready", client.getState(sessionId).visitors.getValue("visitor").phase)
                    } finally {
                        control.releaseStop()
                        client.stopSession(sessionId, 5_000L)
                        client.close()
                        parentScope.cancel()
                    }
                }
            }
        }
    }

    @Test
    fun rejectedVisitorHandshakeIsRelayLocalAndNeverExposesServerText() = runBlocking {
        val marker = "unsafe-server-handshake-marker"
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val controlStates = Channel<FrpControlState>(Channel.UNLIMITED)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                afterControlState = { controlStates.send(it) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            awaitControlState(controlStates) { it is FrpControlState.Open }
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_030))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)
            val response = FrpWireV1.encodeMessage(
                NewVisitorConnResp(proxyName = "remote-service", error = marker),
            )
            val stream = ScriptedMuxStream(response)
            control.enqueueStream(stream)
            val local = FakeLocalConnection(ByteArray(0))
            listener.offer(local)
            withTimeout(5_000L) { stream.resetStarted.await() }

            val state = client.getState(sessionId)
            assertEquals("open", state.phase)
            assertEquals("ready", state.visitors.getValue("visitor").phase)
            assertFalse(state.toString().contains(marker))
            assertFalse(client.toString().contains(marker))
            assertEquals(1, local.closeCount.get())
        } finally {
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun visitorHandshakeTimeoutWinsBeforeRelayCommit() = runBlocking {
        val commitEntered = CompletableDeferred<Unit>()
        val releaseCommit = CompletableDeferred<Unit>()
        val relayCompleted = CompletableDeferred<Unit>()
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                beforeVisitorHandshakeCommit = {
                    commitEntered.complete(Unit)
                    releaseCommit.await()
                },
                visitorHandshakeTimeoutMillis = 50L,
                afterRelayCompletion = { relayCompleted.complete(Unit) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_039))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)
            val serverPayload = "must-not-be-relayed".encodeToByteArray()
            val response = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))
            val stream = ScriptedMuxStream(response + serverPayload)
            control.enqueueStream(stream)
            val local = FakeLocalConnection(ByteArray(0))
            listener.offer(local)

            commitEntered.await()
            withTimeout(5_000L) { stream.resetStarted.await() }
            releaseCommit.complete(Unit)
            withTimeout(5_000L) { relayCompleted.await() }

            assertTrue(local.writtenBytes().isEmpty())
            assertEquals(1, local.closeCount.get())
            assertEquals(1, stream.resetCount.get())
            assertEquals("ready", client.getState(sessionId).visitors.getValue("visitor").phase)
        } finally {
            releaseCommit.complete(Unit)
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun completedFailedRelayCannotRejectTheNextAcceptedConnection() = runBlocking {
        val blockNextEnsure = AtomicBoolean(false)
        val actorBlocked = CompletableDeferred<Unit>()
        val releaseActor = CompletableDeferred<Unit>()
        val releaseFirstHandshake = CompletableDeferred<Unit>()
        val firstRelayCompleted = CompletableDeferred<Unit>()
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                beforeRuntimeCommand = { kind ->
                    if (kind == "EnsureVisitorCommand" && blockNextEnsure.compareAndSet(true, false)) {
                        actorBlocked.complete(Unit)
                        releaseActor.await()
                    }
                },
                afterRelayCompletion = { firstRelayCompleted.complete(Unit) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_031))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            val failedResponse = FrpWireV1.encodeMessage(
                NewVisitorConnResp(proxyName = "ignored", error = "synthetic rejection"),
            )
            val firstStream = ScriptedMuxStream(failedResponse, firstReadRelease = releaseFirstHandshake)
            control.enqueueStream(firstStream)
            val firstLocal = FakeLocalConnection(ByteArray(0))
            listener.offer(firstLocal)
            firstStream.readStarted.await()

            blockNextEnsure.set(true)
            val sameEnsure = async { client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_031)) }
            actorBlocked.await()
            val secondResponse = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))
            val secondStream = ScriptedMuxStream(secondResponse)
            control.enqueueStream(secondStream)
            val secondLocal = FakeLocalConnection(byteArrayOf(1, 2, 3))
            listener.offer(secondLocal)
            waitUntil { listener.acceptCount.get() == 2 }

            releaseFirstHandshake.complete(Unit)
            firstRelayCompleted.await()
            assertEquals(1, firstLocal.closeCount.get())
            releaseActor.complete(Unit)
            sameEnsure.await()

            withTimeout(5_000L) { secondStream.resetStarted.await() }
            assertEquals(1, secondLocal.closeCount.get())
            assertTrue(secondStream.writtenBytes().isNotEmpty())
            assertTrue(control.state.value is FrpControlState.Open)
        } finally {
            releaseFirstHandshake.complete(Unit)
            releaseActor.complete(Unit)
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun bindPortConflictIsTypedSafeAndCommitsOnlyTheAcceptedRevision() = runBlocking {
        val marker = "unsafe-bind-endpoint-marker"
        val controlFactory = FakeControlFactory()
        val delayer = ManualRuntimeDelayer()
        val listenerFactory = FakeListenerFactory().apply {
            nextFailure = BindException(marker)
        }
        val controlStates = Channel<FrpControlState>(Channel.UNLIMITED)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            runtimeDelayer = delayer,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                afterControlState = { controlStates.send(it) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            awaitControlState(controlStates) { it is FrpControlState.Open }

            val accepted = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_040))
            assertEquals(1L, accepted.desiredRevision)
            val retry = delayer.takeCall(10_000L)
            val state = client.getState(sessionId)
            assertEquals(1L, state.configRevision)
            assertEquals("failed", state.visitors.getValue("visitor").phase)
            assertEquals(
                KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT.description,
                state.visitors.getValue("visitor").lastError,
            )
            assertFalse(state.toString().contains(marker))
            val exactFailure = captureRuntimeFailure {
                client.waitVisitorReady(sessionId, "visitor", accepted.desiredRevision, 120_000L)
            }
            assertEquals(KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT, exactFailure.failure)
            assertFalse(exactFailure.toString().contains(marker))
            assertEquals(null, exactFailure.cause)

            val same = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_040))
            assertEquals(1L, same.desiredRevision)
            assertEquals(2L, client.getState(sessionId).configRevision)
            assertEquals(1, listenerFactory.bindCount.get())
            retry.release.complete(Unit)
            listenerFactory.takeBinding()
            assertEquals(
                "ready",
                client.waitVisitorReady(sessionId, "visitor", accepted.desiredRevision, 1_000L).phase,
            )
        } finally {
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun repeatedBindFailuresBackOffExponentiallyFromTheFirstTenSeconds() = runBlocking {
        val controlFactory = FakeControlFactory()
        val delayer = ManualRuntimeDelayer()
        val listenerFactory = FakeListenerFactory().apply { nextFailure = java.net.BindException("busy") }
        val controlStates = Channel<FrpControlState>(Channel.UNLIMITED)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            runtimeDelayer = delayer,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                afterControlState = { controlStates.send(it) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            awaitControlState(controlStates) { it is FrpControlState.Open }
            client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_070))
            var retry = delayer.takeCall(10_000L)
            for (expected in listOf(20_000L, 40_000L)) {
                listenerFactory.nextFailure = java.net.BindException("busy")
                retry.release.complete(Unit)
                retry = delayer.takeCall(expected)
            }
            retry.release.complete(Unit)
            listenerFactory.takeBinding()
            assertEquals(4, listenerFactory.bindCount.get())
        } finally {
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun ordinaryListenerBindFailureRemainsListenerBindFailed() = runBlocking {
        val marker = "ordinary-listener-failure-marker"
        val controlFactory = FakeControlFactory()
        val delayer = ManualRuntimeDelayer()
        val listenerFactory = FakeListenerFactory().apply {
            nextFailure = IllegalStateException(marker)
        }
        val controlStates = Channel<FrpControlState>(Channel.UNLIMITED)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            runtimeDelayer = delayer,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                afterControlState = { controlStates.send(it) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            awaitControlState(controlStates) { it is FrpControlState.Open }

            val accepted = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_039))
            delayer.takeCall(10_000L)
            val state = client.getState(sessionId)
            assertEquals(
                KotlinFrpcStcpVisitorFailure.LISTENER_BIND_FAILED.description,
                state.visitors.getValue("visitor").lastError,
            )
            assertFalse(state.toString().contains(marker))
            val exactFailure = captureRuntimeFailure {
                client.waitVisitorReady(sessionId, "visitor", accepted.desiredRevision, 120_000L)
            }
            assertEquals(KotlinFrpcStcpVisitorFailure.LISTENER_BIND_FAILED, exactFailure.failure)
            assertEquals(null, exactFailure.cause)
            assertFalse(exactFailure.toString().contains(marker))
        } finally {
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun stopTimeoutDoesNotCancelTheSharedCleanupFlightOrRemoveTheSessionEarly() = runBlocking {
        val stopRelease = CompletableDeferred<Unit>()
        val cleanupRelease = CompletableDeferred<Unit>()
        val controlFactory = FakeControlFactory(stopRelease = stopRelease)
        val listenerFactory = FakeListenerFactory()
        val controlStates = Channel<FrpControlState>(Channel.UNLIMITED)
        val stopStarted = CompletableDeferred<Unit>()
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                afterControlState = { controlStates.send(it) },
                beforeStopCleanup = {
                    stopStarted.complete(Unit)
                    cleanupRelease.await()
                },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            awaitControlState(controlStates) { it is FrpControlState.Open }
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_041))
            listenerFactory.takeBinding()
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            val firstStop = async {
                captureRuntimeFailure { client.stopSession(sessionId, 50L) }
            }
            withTimeout(5_000L) { stopStarted.await() }
            assertEquals("stopping", client.getState(sessionId).phase)
            assertEquals(
                KotlinFrpcStcpVisitorFailure.SESSION_CLOSED,
                captureRuntimeFailure {
                    client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 5_000L)
                }.failure,
            )
            assertEquals(KotlinFrpcStcpVisitorFailure.STOP_TIMEOUT, firstStop.await().failure)
            assertEquals(0, control.closeCount.get())
            assertFalse(control.closeCalled.isCompleted)

            val secondStop = async { client.stopSession(sessionId, 5_000L) }
            assertFalse(secondStop.isCompleted)
            cleanupRelease.complete(Unit)
            withTimeout(5_000L) { control.closeCalled.await() }
            assertEquals(1, control.closeCount.get())
            assertFalse(secondStop.isCompleted)
            stopRelease.complete(Unit)
            assertEquals("closed", secondStop.await().phase)
            assertEquals(1, control.closeCount.get())
            assertEquals("closed", client.getState(sessionId).phase)

            val replacementSessionId = client.startSession(sessionConfig())
            assertEquals(sessionId, replacementSessionId)
            assertEquals("closed", client.stopSession(replacementSessionId, 5_000L).phase)
        } finally {
            cleanupRelease.complete(Unit)
            stopRelease.complete(Unit)
            control.releaseStop()
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun fatalCleanupStillClosesControlAndCompletesTheSharedFlight() = runBlocking {
        val hookFailure = AssertionError("synthetic stop hook failure")
        val listenerFailure = AssertionError("synthetic listener close failure")
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val controlStates = Channel<FrpControlState>(Channel.UNLIMITED)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                afterControlState = { controlStates.send(it) },
                beforeStopCleanup = { throw hookFailure },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            awaitControlState(controlStates) { it is FrpControlState.Open }
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_042))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)
            listener.closeFailure = listenerFailure

            var actual: Error? = null
            try {
                client.stopSession(sessionId, 5_000L)
            } catch (failure: Error) {
                actual = failure
            }
            val propagated = actual?.cause ?: actual
            assertSame(hookFailure, propagated)
            assertTrue(propagated?.suppressed?.any { (it.cause ?: it) === listenerFailure } == true)
            withTimeout(5_000L) { control.closeCalled.await() }
            assertEquals(1, control.closeCount.get())
            assertEquals(1, listener.closeCount.get())
        } finally {
            control.releaseStop()
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun callerCancellationKeepsItsIdentityWhileReadinessHasNoActorWaiter() = runBlocking {
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(parentScope, controlFactory, listenerFactory)
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_043))
            val waiter = async {
                client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision + 1L, 120_000L)
            }
            val cancellation = CancellationException("caller cancellation identity")
            waiter.cancel(cancellation)
            val actual = try {
                waiter.await()
                throw AssertionError("cancelled waiter unexpectedly completed")
            } catch (failure: CancellationException) {
                failure
            }
            assertSame(cancellation, actual.cause ?: actual)
            assertEquals(1L, client.getState(sessionId).configRevision)
        } finally {
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun missingVisitorFailsImmediatelyWithoutWaitingForTimeout() = runBlocking {
        val controlFactory = FakeControlFactory()
        val parentScope = newParentScope()
        val client = newClient(parentScope, controlFactory, FakeListenerFactory())
        val sessionId = client.startSession(sessionConfig())
        try {
            val failure = captureRuntimeFailure {
                client.waitVisitorReady(sessionId, "missing", 1L, 120_000L)
            }
            assertEquals(KotlinFrpcStcpVisitorFailure.VISITOR_NOT_FOUND, failure.failure)
        } finally {
            controlFactory.single().releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun visitorRevisionAndExactFailurePrecedeControlTerminalFailure() = runBlocking {
        val controlStates = Channel<FrpControlState>(Channel.UNLIMITED)
        val delayer = ManualRuntimeDelayer()
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            runtimeDelayer = delayer,
            hooks = KotlinFrpcRuntimeLifecycleHooks(afterControlState = { controlStates.send(it) }),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            awaitControlState(controlStates) { it is FrpControlState.Open }
            val original = client.ensureVisitor(
                sessionId,
                visitorConfig(name = "superseded", secret = "first-secret", bindPort = 50_062),
            )
            listenerFactory.takeBinding()
            client.waitVisitorReady(sessionId, "superseded", original.desiredRevision, 1_000L)
            val replacement = client.ensureVisitor(
                sessionId,
                visitorConfig(name = "superseded", secret = "second-secret", bindPort = 50_062),
            )
            listenerFactory.takeBinding()
            client.waitVisitorReady(sessionId, "superseded", replacement.desiredRevision, 1_000L)

            listenerFactory.nextFailure = BindException("synthetic bind failure")
            val failed = client.ensureVisitor(
                sessionId,
                visitorConfig(name = "failed", bindPort = 50_063),
            )
            delayer.takeCall(10_000L)

            control.fail()
            awaitControlState(controlStates) { it is FrpControlState.Failed }

            assertEquals(
                KotlinFrpcStcpVisitorFailure.VISITOR_NOT_FOUND,
                captureRuntimeFailure {
                    client.waitVisitorReady(sessionId, "missing", 1L, 120_000L)
                }.failure,
            )
            assertEquals(
                KotlinFrpcStcpVisitorFailure.VISITOR_SUPERSEDED,
                captureRuntimeFailure {
                    client.waitVisitorReady(sessionId, "superseded", original.desiredRevision, 120_000L)
                }.failure,
            )
            assertEquals(
                KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT,
                captureRuntimeFailure {
                    client.waitVisitorReady(sessionId, "failed", failed.desiredRevision, 120_000L)
                }.failure,
            )
            assertEquals(
                KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT.description,
                client.getState(sessionId).visitors.getValue("failed").lastError,
            )
        } finally {
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun unrelatedVisitorRevisionDoesNotSupersedeTargetVisitor() = runBlocking {
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(parentScope, controlFactory, listenerFactory)
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val target = client.ensureVisitor(
                sessionId,
                visitorConfig(name = "target", bindPort = 50_060),
            )
            listenerFactory.takeBinding()
            client.waitVisitorReady(sessionId, "target", target.desiredRevision, 1_000L)
            val unrelated = client.ensureVisitor(
                sessionId,
                visitorConfig(name = "unrelated", bindPort = 50_061),
            )
            listenerFactory.takeBinding()
            client.waitVisitorReady(sessionId, "unrelated", unrelated.desiredRevision, 1_000L)

            assertEquals(
                target.desiredRevision,
                client.waitVisitorReady(sessionId, "target", target.desiredRevision, 1_000L).desiredRevision,
            )
            val future = captureRuntimeFailure {
                client.waitVisitorReady(sessionId, "target", target.desiredRevision + 1L, 50L)
            }
            assertEquals(KotlinFrpcStcpVisitorFailure.WAIT_TIMEOUT, future.failure)
        } finally {
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun nonPositiveStopTimeoutUsesTheDefaultBudget() = runBlocking {
        val controlFactory = FakeControlFactory()
        val sessionSequence = AtomicInteger(0)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            FakeListenerFactory(),
            sessionIdSource = FrpSessionIdSource { "stop-default-${sessionSequence.incrementAndGet()}" },
        )
        try {
            val zero = client.startSession(sessionConfig())
            assertEquals("closed", client.stopSession(zero, 0L).phase)
            val negative = client.startSession(sessionConfig())
            assertEquals("closed", client.stopSession(negative, -1L).phase)
        } finally {
            controlFactory.snapshot().forEach(FakeSessionControl::releaseStop)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun phaseContractIncludesStartingReconnectingStoppingAndStopped() = runBlocking {
        val bindEntered = CompletableDeferred<Unit>()
        val releaseBind = CompletableDeferred<Unit>()
        val stopRelease = CompletableDeferred<Unit>()
        val controlFactory = FakeControlFactory(stopRelease = stopRelease)
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                beforeVisitorBind = { _, _ ->
                    bindEntered.complete(Unit)
                    releaseBind.await()
                },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_044))
            assertEquals("connecting", client.getState(sessionId).phase)
            assertEquals("pending", client.getState(sessionId).visitors.getValue("visitor").phase)

            control.open(epoch = 1L, transport = 1L)
            bindEntered.await()
            assertEquals("open", client.getState(sessionId).phase)
            assertEquals("starting", client.getState(sessionId).visitors.getValue("visitor").phase)
            releaseBind.complete(Unit)
            listenerFactory.takeBinding()
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            control.retry(attempt = 1)
            waitUntil { client.getState(sessionId).phase == "reconnecting" }
            val reconnecting = client.getState(sessionId)
            assertEquals("transport failed", reconnecting.lastError)
            assertEquals("pending", reconnecting.visitors.getValue("visitor").phase)

            control.establishmentFailure = FrpControlFailure.RECONNECT_WAITING_LOGIN
            control.retry(attempt = 1)
            assertEquals("control reconnect waiting login", client.getState(sessionId).lastError)
            control.reconnectDiagnostics = FrpControlReconnectDiagnostics(
                establishAttemptCount = 1L,
                loginTimerScheduledCount = 1L,
                currentAttempt = 1L,
                loginTimerScheduledAttempt = 1L,
            )
            assertEquals("control reconnect login timer scheduled", client.getState(sessionId).lastError)
            control.reconnectDiagnostics = control.reconnectDiagnostics.copy(
                loginTimerFiredCount = 1L,
                loginTimerFiredAttempt = 1L,
            )
            assertEquals("control reconnect login timer fired", client.getState(sessionId).lastError)
            control.retry(attempt = 2, failure = FrpControlFailure.YAMUX_TRANSPORT_FAILED)
            assertEquals("yamux transport failed", client.getState(sessionId).lastError)
            control.establishmentFailure = FrpControlFailure.RECONNECT_WAITING_LOGIN_AFTER_TIMEOUT
            assertEquals(
                "control reconnect waiting login after timeout",
                client.getState(sessionId).lastError,
            )
            control.establishmentFailure = null

            control.open(epoch = 2L, transport = 2L)
            listenerFactory.takeBinding()
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            val stopping = async { client.stopSession(sessionId, 5_000L) }
            control.closeCalled.await()
            val stoppingState = client.getState(sessionId)
            assertEquals("stopping", stoppingState.phase)
            assertEquals("stopped", stoppingState.visitors.getValue("visitor").phase)
            stopRelease.complete(Unit)
            assertEquals("closed", stopping.await().phase)
        } finally {
            releaseBind.complete(Unit)
            stopRelease.complete(Unit)
            control.releaseStop()
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun defaultNetworkChangeInvalidatesEveryOpenControl() = runBlocking {
        val controlFactory = FakeControlFactory()
        val sessionSequence = AtomicInteger(0)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            FakeListenerFactory(),
            sessionIdSource = FrpSessionIdSource { "network-change-${sessionSequence.incrementAndGet()}" },
        )
        val firstSession = client.startSession(sessionConfig())
        val secondSession = client.startSession(sessionConfig())
        val controls = controlFactory.snapshot()
        try {
            controls[0].open(epoch = 1L, transport = 1L)
            controls[1].open(epoch = 1L, transport = 1L)

            client.reconnectAfterDefaultNetworkChange()

            assertEquals(listOf(1, 1), controls.map { it.invalidateCount.get() })
            assertEquals("reconnecting", client.getState(firstSession).phase)
            assertEquals("reconnecting", client.getState(secondSession).phase)
        } finally {
            controls.forEach(FakeSessionControl::releaseStop)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun updateInvalidatesBlockedBindAndClosesLateListenerBeforeReplacement() = runBlocking {
        val controlFactory = FakeControlFactory()
        val listenerFactory = LateListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(parentScope, controlFactory, listenerFactory)
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val first = client.ensureVisitor(sessionId, visitorConfig(secret = "first-secret", bindPort = 50_045))
            val firstBind = listenerFactory.takeRequest()
            assertEquals("starting", client.getState(sessionId).visitors.getValue("visitor").phase)

            val update = async {
                client.ensureVisitor(sessionId, visitorConfig(secret = "second-secret", bindPort = 50_045))
            }
            assertFalse(update.isCompleted)
            firstBind.release.complete(Unit)
            val second = update.await()
            assertEquals(first.desiredRevision + 1L, second.desiredRevision)
            assertEquals(1, firstBind.listener.closeCount.get())
            val secondBind = listenerFactory.takeRequest()
            secondBind.release.complete(Unit)
            client.waitVisitorReady(sessionId, "visitor", second.desiredRevision, 1_000L)
        } finally {
            listenerFactory.releaseAll()
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
        Unit
    }

    @Test
    fun reconnectInvalidatesBlockedBindAndClosesLateListener() = runBlocking {
        val controlFactory = FakeControlFactory()
        val listenerFactory = LateListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(parentScope, controlFactory, listenerFactory)
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_046))
            val firstBind = listenerFactory.takeRequest()

            control.retry(attempt = 1)
            assertEquals("reconnecting", client.getState(sessionId).phase)
            firstBind.release.complete(Unit)
            waitUntil { firstBind.listener.closeCount.get() == 1 }

            control.open(epoch = 2L, transport = 2L)
            val secondBind = listenerFactory.takeRequest()
            secondBind.release.complete(Unit)
            assertEquals(
                2L,
                client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L).boundControlEpoch,
            )
        } finally {
            listenerFactory.releaseAll()
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun stopInvalidatesBlockedBindAndClosesLateListener() = runBlocking {
        val controlFactory = FakeControlFactory()
        val listenerFactory = LateListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(parentScope, controlFactory, listenerFactory)
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_047))
            val pendingBind = listenerFactory.takeRequest()

            val stop = async { client.stopSession(sessionId, 5_000L) }
            waitUntil { client.getState(sessionId).phase == "stopping" }
            pendingBind.release.complete(Unit)
            assertEquals("closed", stop.await().phase)
            assertEquals(1, pendingBind.listener.closeCount.get())
        } finally {
            listenerFactory.releaseAll()
            control.releaseStop()
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun mailboxFullClosesAcceptedSocketsAndReliablyFailsThenRetriesVisitor() = runBlocking {
        val blockNextEnsure = AtomicBoolean(false)
        val actorBlocked = CompletableDeferred<Unit>()
        val releaseActor = CompletableDeferred<Unit>()
        val delayer = ManualRuntimeDelayer()
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            runtimeDelayer = delayer,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                beforeRuntimeCommand = { kind ->
                    if (kind == "EnsureVisitorCommand" && blockNextEnsure.compareAndSet(true, false)) {
                        actorBlocked.complete(Unit)
                        releaseActor.await()
                    }
                },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_048))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            blockNextEnsure.set(true)
            val sameEnsure = async { client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_048)) }
            actorBlocked.await()
            val connections = List(65) { FakeLocalConnection(ByteArray(0)) }
            connections.forEach(listener::offer)
            waitUntil { connections.last().closeCount.get() == 1 }
            assertEquals(1, listener.closeCount.get())

            releaseActor.complete(Unit)
            sameEnsure.await()
            delayer.takeCall(10_000L)
            waitUntil { connections.all { it.closeCount.get() == 1 } }
            assertEquals("failed", client.getState(sessionId).visitors.getValue("visitor").phase)
        } finally {
            releaseActor.complete(Unit)
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
        Unit
    }

    @Test
    fun acceptFailureRevokesReadinessWhileActorIsBlocked() = runBlocking {
        val blockNextEnsure = AtomicBoolean(false)
        val actorBlocked = CompletableDeferred<Unit>()
        val releaseActor = CompletableDeferred<Unit>()
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                beforeRuntimeCommand = { kind ->
                    if (kind == "EnsureVisitorCommand" && blockNextEnsure.compareAndSet(true, false)) {
                        actorBlocked.complete(Unit)
                        releaseActor.await()
                    }
                },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_051))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            blockNextEnsure.set(true)
            val sameEnsure = async { client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_051)) }
            actorBlocked.await()
            listener.failAccept(BindException("synthetic accept failure"))
            waitUntil { listener.closeCount.get() == 1 }

            val state = client.getState(sessionId).visitors.getValue("visitor")
            assertFalse(state.listenerBound)
            assertEquals("failed", state.phase)
            val readiness = captureRuntimeFailure {
                client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)
            }
            assertEquals(KotlinFrpcStcpVisitorFailure.VISITOR_FAILED, readiness.failure)

            releaseActor.complete(Unit)
            sameEnsure.await()
        } finally {
            releaseActor.complete(Unit)
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
        Unit
    }

    @Test
    fun parentCancellationDuringStopDrainsMailboxAndRemovesRegistryAfterAllResourcesStop() = runBlocking {
        val blockNextEnsure = AtomicBoolean(false)
        val actorBlocked = CompletableDeferred<Unit>()
        val releaseActor = CompletableDeferred<Unit>()
        val cleanupObserved = CompletableDeferred<KotlinFrpcSessionCleanupDiagnostics>()
        val stopRelease = CompletableDeferred<Unit>()
        val controlFactory = FakeControlFactory(stopRelease = stopRelease)
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                beforeRuntimeCommand = { kind ->
                    if (kind == "EnsureVisitorCommand" && blockNextEnsure.compareAndSet(true, false)) {
                        actorBlocked.complete(Unit)
                        releaseActor.await()
                    }
                },
                beforeSessionRegistryRemoval = { cleanupObserved.complete(it) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_049))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            blockNextEnsure.set(true)
            val blockedEnsure = async { captureRuntimeFailure { client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_049)) } }
            actorBlocked.await()
            val pendingSockets = List(8) { FakeLocalConnection(ByteArray(0)) }
            pendingSockets.forEach(listener::offer)
            waitUntil { listener.acceptCount.get() == pendingSockets.size }

            val stop = async {
                try {
                    client.stopSession(sessionId, 5_000L)
                } catch (_: CancellationException) {
                    null
                }
            }
            parentScope.cancel()
            stopRelease.complete(Unit)
            releaseActor.complete(Unit)
            stop.await()
            val cleanup = cleanupObserved.await()
            assertEquals(0, cleanup.activeOwnedJobs)
            assertEquals(0, cleanup.activeListeners)
            assertEquals(0, cleanup.activeRelays)
            assertTrue(cleanup.actorCompleted)
            assertTrue(cleanup.controlCollectorCompleted)
            assertTrue(pendingSockets.all { it.closeCount.get() == 1 })
            assertEquals(0, client.diagnostics().activeSessions)
            assertTrue(blockedEnsure.isCompleted || blockedEnsure.await().failure == KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
        } finally {
            releaseActor.complete(Unit)
            stopRelease.complete(Unit)
            control.releaseStop()
            client.close()
        }
    }

    @Test
    fun oneHundredConcurrentRelaysAreSharedAcrossSessionsAndTheNextIsRejected() = runBlocking {
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val sessionSequence = AtomicInteger(0)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            sessionIdSource = FrpSessionIdSource { "relay-session-${sessionSequence.incrementAndGet()}" },
            ioDispatcher = Dispatchers.IO.limitedParallelism(256, "KotlinFrpcRelayBudgetTest"),
        )
        val sessions = mutableListOf<String>()
        val localConnections = mutableListOf<BlockingLocalConnection>()
        try {
            repeat(2) { index ->
                val sessionId = client.startSession(sessionConfig())
                sessions += sessionId
                val control = controlFactory.snapshot()[index]
                control.open(epoch = 1L, transport = 1L)
                val ensured = client.ensureVisitor(
                    sessionId,
                    visitorConfig(name = "visitor-$index", bindPort = 50_050 + index),
                )
                val listener = listenerFactory.takeBinding().listener
                client.waitVisitorReady(sessionId, "visitor-$index", ensured.desiredRevision, 1_000L)
                repeat(50) { relayIndex ->
                    val response = FrpWireV1.encodeMessage(
                        NewVisitorConnResp(proxyName = "ignored-response-$index-$relayIndex"),
                    )
                    control.enqueueStream(ScriptedMuxStream(response + byteArrayOf(relayIndex.toByte())))
                    BlockingLocalConnection().also { local ->
                        localConnections += local
                        listener.offer(local)
                    }
                }
            }
            waitUntil {
                localConnections.size == 100 &&
                    localConnections.all { it.readStarted.isCompleted && it.shutdownOutputCount.get() == 1 }
            }
            assertEquals(100, client.diagnostics().activeRelays)

            val rejected = FakeLocalConnection(ByteArray(0))
            listenerFactory.bindingSnapshot().first().listener.offer(rejected)
            waitUntil { rejected.closeCount.get() == 1 }
            assertEquals(100, client.diagnostics().activeRelays)
            assertEquals(1, client.diagnostics().rejectedRelays)

            localConnections.forEach(BlockingLocalConnection::releaseRead)
            waitUntil { client.diagnostics().activeRelays == 0 }
        } finally {
            localConnections.forEach(BlockingLocalConnection::releaseRead)
            controlFactory.snapshot().forEach(FakeSessionControl::releaseStop)
            sessions.forEach { client.stopSession(it, 5_000L) }
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun stopVisitorDoesNotWaitForBestEffortRelayReset() = runBlocking {
        val resetRelease = CompletableDeferred<Unit>()
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(parentScope, controlFactory, listenerFactory)
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_052))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            val response = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))
            val stream = ScriptedMuxStream(response, resetRelease = resetRelease)
            val local = BlockingLocalConnection()
            control.enqueueStream(stream)
            listener.offer(local)
            local.readStarted.await()

            val stop = async { client.stopVisitor(sessionId, "visitor") }
            withTimeout(5_000L) { stream.resetStarted.await() }
            withTimeout(1_000L) { stop.await() }

            assertFalse(resetRelease.isCompleted)
            assertFalse(client.getState(sessionId).visitors.containsKey("visitor"))
            assertEquals(1, listener.closeCount.get())
            assertEquals(1, local.closeCount.get())
            assertEquals(1, client.diagnostics().activeRelays)

            resetRelease.complete(Unit)
            waitUntil { client.diagnostics().activeRelays == 0 }
        } finally {
            resetRelease.complete(Unit)
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun stopVisitorDoesNotWaitForBlockedRelayCloseWrite() = runBlocking {
        val closeWriteRelease = CompletableDeferred<Unit>()
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(parentScope, controlFactory, listenerFactory)
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_053))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            val response = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))
            val stream = ScriptedMuxStream(response, closeWriteRelease = closeWriteRelease)
            val local = FakeLocalConnection(ByteArray(0))
            control.enqueueStream(stream)
            listener.offer(local)
            withTimeout(5_000L) { stream.closeWriteStarted.await() }

            withTimeout(1_000L) { client.stopVisitor(sessionId, "visitor") }

            assertFalse(closeWriteRelease.isCompleted)
            assertFalse(client.getState(sessionId).visitors.containsKey("visitor"))
            assertEquals(1, listener.closeCount.get())
            assertEquals(1, local.closeCount.get())
            assertEquals(1, client.diagnostics().activeRelays)

            closeWriteRelease.complete(Unit)
            waitUntil { client.diagnostics().activeRelays == 0 }
        } finally {
            closeWriteRelease.complete(Unit)
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun relayCancellationAfterConsumedPumpNotificationDoesNotLoseCompletion() = runBlocking {
        val notificationReceived = CompletableDeferred<Unit>()
        val blockCoordinator = CompletableDeferred<Unit>()
        val closeWriteRelease = CompletableDeferred<Unit>()
        val parentScope = newParentScope()
        val control = FakeSessionControl(
            generation = 1L,
            protocol = FrpWireProtocol.V1,
            parentScope = parentScope,
            stopRelease = null,
        )
        val identity = control.open(epoch = 1L, transport = 1L)
        val response = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))
        val stream = ScriptedMuxStream(
            inputBytes = response,
            closeWriteRelease = closeWriteRelease,
            closeWriteFailure = FrpTransportException(FrpTransportFailure.CLOSED),
        )
        control.enqueueStream(stream)
        val relay = FrpVisitorRelay(
            visitorRevision = 1L,
            listenerAttempt = 1L,
            control = control,
            identity = identity,
            visitor = FrpRelayVisitorConfig(
                serverName = "remote-service",
                serverUser = "",
                sessionUser = "",
                secretKey = SYNTHETIC_SECRET,
            ),
            local = FakeLocalConnection(ByteArray(0)),
            parentScope = parentScope,
            unixTimeSource = FrpUnixTimeSource { FIXED_TIMESTAMP },
            ioDispatcher = Dispatchers.IO,
            afterPumpOutcomeReceived = {
                notificationReceived.complete(Unit)
                blockCoordinator.await()
            },
        )
        try {
            relay.start()
            withTimeout(5_000L) { notificationReceived.await() }
            withTimeout(5_000L) { stream.closeWriteStarted.await() }

            relay.close()
            closeWriteRelease.complete(Unit)

            withTimeout(1_000L) { relay.awaitStopped() }
            assertEquals(1, stream.resetCount.get())
        } finally {
            blockCoordinator.complete(Unit)
            closeWriteRelease.complete(Unit)
            relay.close()
            control.releaseStop()
            parentScope.cancel()
        }
    }

    @Test
    fun relayCloseAfterOrdinaryFailureClassificationStillReportsRelayFailure() = runBlocking {
        val failureClassified = CompletableDeferred<Unit>()
        val releaseClassification = CompletableDeferred<Unit>()
        val remoteReadRelease = CompletableDeferred<Unit>()
        val parentScope = newParentScope()
        val control = FakeSessionControl(
            generation = 1L,
            protocol = FrpWireProtocol.V1,
            parentScope = parentScope,
            stopRelease = null,
        )
        val identity = control.open(epoch = 1L, transport = 1L)
        val response = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))
        val stream = ScriptedMuxStream(response, afterInputRelease = remoteReadRelease)
        control.enqueueStream(stream)
        val relay = FrpVisitorRelay(
            visitorRevision = 1L,
            listenerAttempt = 1L,
            control = control,
            identity = identity,
            visitor = FrpRelayVisitorConfig(
                serverName = "remote-service",
                serverUser = "",
                sessionUser = "",
                secretKey = SYNTHETIC_SECRET,
            ),
            local = ZeroReadLocalConnection(),
            parentScope = parentScope,
            unixTimeSource = FrpUnixTimeSource { FIXED_TIMESTAMP },
            ioDispatcher = Dispatchers.IO,
            afterOrdinaryPumpFailureClassified = {
                failureClassified.complete(Unit)
                releaseClassification.await()
            },
        )
        try {
            relay.start()
            withTimeout(5_000L) { failureClassified.await() }

            relay.close()
            releaseClassification.complete(Unit)

            val failure = captureRuntimeFailure { relay.awaitStopped() }
            assertEquals(KotlinFrpcStcpVisitorFailure.RELAY_FAILED, failure.failure)
        } finally {
            releaseClassification.complete(Unit)
            remoteReadRelease.complete(Unit)
            relay.close()
            control.releaseStop()
            parentScope.cancel()
        }
    }

    @Test
    fun relayCloseRacingConsumedPumpCancellationPreservesTheExternalInstance() = runBlocking {
        val notificationReceived = CompletableDeferred<Unit>()
        val blockCoordinator = CompletableDeferred<Unit>()
        val externalCancellation = CancellationException("independent relay pump cancellation")
        val parentScope = newParentScope()
        val control = FakeSessionControl(
            generation = 1L,
            protocol = FrpWireProtocol.V1,
            parentScope = parentScope,
            stopRelease = null,
        )
        val identity = control.open(epoch = 1L, transport = 1L)
        val response = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))
        val stream = ScriptedMuxStream(response, afterInputFailure = externalCancellation)
        control.enqueueStream(stream)
        val relay = FrpVisitorRelay(
            visitorRevision = 1L,
            listenerAttempt = 1L,
            control = control,
            identity = identity,
            visitor = FrpRelayVisitorConfig(
                serverName = "remote-service",
                serverUser = "",
                sessionUser = "",
                secretKey = SYNTHETIC_SECRET,
            ),
            local = BlockingLocalConnection(),
            parentScope = parentScope,
            unixTimeSource = FrpUnixTimeSource { FIXED_TIMESTAMP },
            ioDispatcher = Dispatchers.IO,
            afterPumpOutcomeReceived = {
                notificationReceived.complete(Unit)
                blockCoordinator.await()
            },
        )
        try {
            relay.start()
            withTimeout(5_000L) { notificationReceived.await() }

            relay.close()

            val actual = try {
                withTimeout(1_000L) { relay.awaitStopped() }
                throw AssertionError("cancelled relay unexpectedly completed")
            } catch (failure: CancellationException) {
                failure
            }
            assertTrue(actual === externalCancellation || actual.cause === externalCancellation)
        } finally {
            blockCoordinator.complete(Unit)
            relay.close()
            control.releaseStop()
            parentScope.cancel()
        }
    }

    @Test
    fun relayFailurePromotionDoesNotCreateCyclicSuppressedGraph() = runBlocking {
        val notificationReceived = CompletableDeferred<Unit>()
        val blockCoordinator = CompletableDeferred<Unit>()
        val reportedFailure = CompletableDeferred<Throwable>()
        val promotedError = AssertionError("promoted relay failure")
        val externalCancellation = CancellationException("relay cancellation with a fatal failure").apply {
            addSuppressed(promotedError)
        }
        val parentScope = CoroutineScope(
            SupervisorJob() + Dispatchers.Default + CoroutineExceptionHandler { _, failure ->
                reportedFailure.complete(failure)
            },
        )
        val control = FakeSessionControl(
            generation = 1L,
            protocol = FrpWireProtocol.V1,
            parentScope = parentScope,
            stopRelease = null,
        )
        val identity = control.open(epoch = 1L, transport = 1L)
        val response = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))
        val stream = ScriptedMuxStream(response, afterInputFailure = externalCancellation)
        control.enqueueStream(stream)
        val relay = FrpVisitorRelay(
            visitorRevision = 1L,
            listenerAttempt = 1L,
            control = control,
            identity = identity,
            visitor = FrpRelayVisitorConfig(
                serverName = "remote-service",
                serverUser = "",
                sessionUser = "",
                secretKey = SYNTHETIC_SECRET,
            ),
            local = BlockingLocalConnection(),
            parentScope = parentScope,
            unixTimeSource = FrpUnixTimeSource { FIXED_TIMESTAMP },
            ioDispatcher = Dispatchers.IO,
            afterPumpOutcomeReceived = {
                notificationReceived.complete(Unit)
                blockCoordinator.await()
            },
        )
        try {
            relay.start()
            withTimeout(5_000L) { notificationReceived.await() }

            relay.close()

            val actual = try {
                withTimeout(1_000L) { relay.awaitStopped() }
                throw AssertionError("relay with a fatal failure unexpectedly completed")
            } catch (failure: AssertionError) {
                failure
            }
            assertTrue(actual === promotedError || actual.cause === promotedError)
            assertFalse(promotedError.suppressed.any { it === externalCancellation })
            assertTrue(externalCancellation.suppressed.any { it === promotedError })
            val reported = withTimeout(5_000L) { reportedFailure.await() }
            assertTrue(reported === promotedError || reported.cause === promotedError)
        } finally {
            blockCoordinator.complete(Unit)
            relay.close()
            control.releaseStop()
            parentScope.cancel()
        }
    }

    @Test
    fun relayWatcherOwnsCleanupBeforeSessionCancellation() = runBlocking {
        val parentJob = SupervisorJob()
        val parentScope = CoroutineScope(
            parentJob + Dispatchers.Default.limitedParallelism(1, "KotlinFrpcRelayWatcherStartTest"),
        )
        val watcherScheduled = CompletableDeferred<Unit>()
        val cleanupDiagnostics = CompletableDeferred<KotlinFrpcSessionCleanupDiagnostics>()
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                afterRelayWatcherScheduled = {
                    watcherScheduled.complete(Unit)
                    parentJob.cancel()
                },
                beforeSessionRegistryRemoval = { cleanupDiagnostics.complete(it) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_053))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            val response = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))
            control.enqueueStream(ScriptedMuxStream(response))
            listener.offer(BlockingLocalConnection())
            withTimeout(5_000L) { watcherScheduled.await() }
            control.releaseStop()

            assertEquals("closed", client.stopSession(sessionId, 5_000L).phase)
            val diagnostics = withTimeout(5_000L) { cleanupDiagnostics.await() }
            assertEquals(0, diagnostics.activeOwnedJobs)
            assertEquals(0, diagnostics.activeRelays)
            assertEquals(0, client.diagnostics().activeRelays)
        } finally {
            control.releaseStop()
            client.close()
            parentJob.cancel()
        }
    }

    @Test
    fun stopSessionPropagatesLateRelayResetCancellation() = runBlocking {
        val resetRelease = CompletableDeferred<Unit>()
        val resetCancellation = CancellationException("late relay reset cancellation")
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(parentScope, controlFactory, listenerFactory)
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_054))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            val response = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))
            val stream = ScriptedMuxStream(
                response,
                resetRelease = resetRelease,
                resetFailure = resetCancellation,
            )
            val local = BlockingLocalConnection()
            control.enqueueStream(stream)
            listener.offer(local)
            local.readStarted.await()

            client.stopVisitor(sessionId, "visitor")
            withTimeout(5_000L) { stream.resetStarted.await() }
            control.releaseStop()
            val stopping = async {
                try {
                    client.stopSession(sessionId, 5_000L)
                    null
                } catch (failure: Throwable) {
                    failure
                }
            }
            delay(50L)
            assertFalse(stopping.isCompleted)
            resetRelease.complete(Unit)

            val actual = withTimeout(5_000L) { stopping.await() }
            assertSame(resetCancellation, actual?.cause ?: actual)
            assertEquals(0, client.diagnostics().activeRelays)
        } finally {
            resetRelease.complete(Unit)
            control.releaseStop()
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun stopSessionPropagatesLateRelayResetError() = runBlocking {
        val resetRelease = CompletableDeferred<Unit>()
        val reportedFailure = CompletableDeferred<Throwable>()
        val resetError = AssertionError("late relay reset error")
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = CoroutineScope(
            SupervisorJob() + Dispatchers.Default + CoroutineExceptionHandler { _, failure ->
                reportedFailure.complete(failure)
            },
        )
        val client = newClient(parentScope, controlFactory, listenerFactory)
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort = 50_055))
            val listener = listenerFactory.takeBinding().listener
            client.waitVisitorReady(sessionId, "visitor", ensured.desiredRevision, 1_000L)

            val response = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))
            val stream = ScriptedMuxStream(
                response,
                resetRelease = resetRelease,
                resetFailure = resetError,
            )
            val local = BlockingLocalConnection()
            control.enqueueStream(stream)
            listener.offer(local)
            local.readStarted.await()

            client.stopVisitor(sessionId, "visitor")
            withTimeout(5_000L) { stream.resetStarted.await() }
            control.releaseStop()
            val stopping = async {
                try {
                    client.stopSession(sessionId, 5_000L)
                    null
                } catch (failure: Throwable) {
                    failure
                }
            }
            delay(50L)
            assertFalse(stopping.isCompleted)
            resetRelease.complete(Unit)

            val actual = withTimeout(5_000L) { stopping.await() }
            assertTrue(actual === resetError || actual?.cause === resetError)
            val reported = withTimeout(5_000L) { reportedFailure.await() }
            assertTrue(reported === resetError || reported.cause === resetError)
            assertEquals(0, client.diagnostics().activeRelays)
        } finally {
            resetRelease.complete(Unit)
            control.releaseStop()
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun relayZeroLengthReadFailsTypedWithoutFailingControl() = runBlocking {
        val parentScope = newParentScope()
        val control = FakeSessionControl(
            generation = 1L,
            protocol = FrpWireProtocol.V1,
            parentScope = parentScope,
            stopRelease = null,
        )
        val identity = control.open(epoch = 1L, transport = 1L)
        control.enqueueStream(
            ScriptedMuxStream(FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored"))),
        )
        val relay = FrpVisitorRelay(
            visitorRevision = 1L,
            listenerAttempt = 1L,
            control = control,
            identity = identity,
            visitor = FrpRelayVisitorConfig(
                serverName = "remote-service",
                serverUser = "",
                sessionUser = "",
                secretKey = SYNTHETIC_SECRET,
            ),
            local = ZeroReadLocalConnection(),
            parentScope = parentScope,
            unixTimeSource = FrpUnixTimeSource { FIXED_TIMESTAMP },
            ioDispatcher = Dispatchers.IO,
        )
        try {
            relay.start()
            val failure = captureRuntimeFailure { relay.awaitStopped() }
            assertEquals(KotlinFrpcStcpVisitorFailure.RELAY_FAILED, failure.failure)
            assertTrue(control.state.value is FrpControlState.Open)
        } finally {
            relay.close()
            control.close()
            control.releaseStop()
            control.awaitStopped()
            parentScope.cancel()
        }
    }

    @Test
    fun plainRelayAvoidsPerChunkLocalIoDispatchForImmediateStreams() = runBlocking {
        val parentScope = newParentScope()
        val control = FakeSessionControl(
            generation = 1L,
            protocol = FrpWireProtocol.V1,
            parentScope = parentScope,
            stopRelease = null,
        )
        val identity = control.open(epoch = 1L, transport = 1L)
        val localPayload = ByteArray(40 * 1024) { index -> index.toByte() }
        val remotePayload = ByteArray(40 * 1024) { index -> (index xor 0x5a).toByte() }
        val stream = ScriptedMuxStream(
            FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored")) + remotePayload,
        )
        control.enqueueStream(stream)
        val local = FakeLocalConnection(localPayload)
        val dispatcher = CountingDispatcher(Dispatchers.IO)
        val relay = FrpVisitorRelay(
            visitorRevision = 1L,
            listenerAttempt = 1L,
            control = control,
            identity = identity,
            visitor = FrpRelayVisitorConfig(
                serverName = "remote-service",
                serverUser = "",
                sessionUser = "",
                secretKey = SYNTHETIC_SECRET,
            ),
            local = local,
            parentScope = parentScope,
            unixTimeSource = FrpUnixTimeSource { FIXED_TIMESTAMP },
            ioDispatcher = dispatcher,
        )
        try {
            relay.start()
            relay.awaitStopped()

            assertTrue(localPayload.contentEquals(stream.writtenBytes().copyOfRange(
                stream.writtenBytes().size - localPayload.size,
                stream.writtenBytes().size,
            )))
            assertTrue(remotePayload.contentEquals(local.writtenBytes()))
            assertEquals(1, stream.closeWriteCount.get())
            assertEquals(1, local.shutdownOutputCount.get())
            // Each pump enters once; the three chunks in each direction add no local-I/O dispatches.
            assertEquals(2, dispatcher.dispatchCount.get())
        } finally {
            relay.close()
            control.close()
            control.releaseStop()
            control.awaitStopped()
            parentScope.cancel()
        }
    }

    @Test
    fun relayFailureClosesUninterruptibleSiblingBeforeJoiningPumps() = runBlocking {
        val releaseRemoteFailure = CompletableDeferred<Unit>()
        val parentScope = newParentScope()
        val control = FakeSessionControl(
            generation = 1L,
            protocol = FrpWireProtocol.V1,
            parentScope = parentScope,
            stopRelease = null,
        )
        val identity = control.open(epoch = 1L, transport = 1L)
        val stream = ScriptedMuxStream(
            inputBytes = FrpWireV1.encodeMessage(NewVisitorConnResp(proxyName = "ignored")),
            afterInputRelease = releaseRemoteFailure,
            afterInputResult = 0,
        )
        control.enqueueStream(stream)
        val local = UninterruptibleLocalConnection()
        val relay = FrpVisitorRelay(
            visitorRevision = 1L,
            listenerAttempt = 1L,
            control = control,
            identity = identity,
            visitor = FrpRelayVisitorConfig(
                serverName = "remote-service",
                serverUser = "",
                sessionUser = "",
                secretKey = SYNTHETIC_SECRET,
            ),
            local = local,
            parentScope = parentScope,
            unixTimeSource = FrpUnixTimeSource { FIXED_TIMESTAMP },
            ioDispatcher = Dispatchers.IO,
        )
        try {
            relay.start()
            local.awaitReadStarted()
            releaseRemoteFailure.complete(Unit)

            val failure = captureRuntimeFailure { relay.awaitStopped() }
            assertEquals(KotlinFrpcStcpVisitorFailure.RELAY_FAILED, failure.failure)
            assertEquals(1, local.closeCount.get())
            assertEquals(1, stream.resetCount.get())
            assertTrue(control.state.value is FrpControlState.Open)
        } finally {
            releaseRemoteFailure.complete(Unit)
            relay.close()
            control.close()
            control.releaseStop()
            control.awaitStopped()
            parentScope.cancel()
        }
    }

    @Test
    fun lazyRelayClosedBeforeFirstDispatchCompletesExactlyOnce() = runBlocking {
        val dispatcher = QueueingDispatcher()
        val parentScope = CoroutineScope(SupervisorJob() + dispatcher)
        val control = FakeSessionControl(
            generation = 1L,
            protocol = FrpWireProtocol.V1,
            parentScope = parentScope,
            stopRelease = null,
        )
        val identity = control.open(epoch = 1L, transport = 1L)
        val local = FakeLocalConnection(ByteArray(0))
        val relay = FrpVisitorRelay(
            visitorRevision = 1L,
            listenerAttempt = 1L,
            control = control,
            identity = identity,
            visitor = FrpRelayVisitorConfig(
                serverName = "remote-service",
                serverUser = "",
                sessionUser = "",
                secretKey = SYNTHETIC_SECRET,
            ),
            local = local,
            parentScope = parentScope,
            unixTimeSource = FrpUnixTimeSource { FIXED_TIMESTAMP },
            ioDispatcher = Dispatchers.IO,
        )
        try {
            relay.start()
            assertEquals(1, dispatcher.queuedCount)

            relay.close()

            withTimeout(5_000L) { relay.awaitStopped() }
            assertTrue(relay.isCompleted)
            assertEquals(1, local.closeCount.get())
            assertEquals(1, dispatcher.queuedCount)
        } finally {
            relay.close()
            parentScope.cancel()
        }
    }

    @Test
    fun parentCancellationBeforeActorFirstDispatchRunsEmergencyCleanup() = runBlocking {
        val dispatcher = QueueingDispatcher()
        val parentJob = SupervisorJob()
        val parentScope = CoroutineScope(parentJob + dispatcher)
        val controlFactory = FakeControlFactory()
        val cleanupDiagnostics = CompletableDeferred<KotlinFrpcSessionCleanupDiagnostics>()
        val client = newClient(
            parentScope = parentScope,
            controlFactory = controlFactory,
            listenerFactory = FakeListenerFactory(),
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                beforeSessionRegistryRemoval = { cleanupDiagnostics.complete(it) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            assertTrue(dispatcher.queuedCount >= 2)
            parentJob.cancel()
            val stopped = async { client.stopSession(sessionId, 5_000L) }

            withTimeout(5_000L) {
                while (!stopped.isCompleted) {
                    if (!dispatcher.runNextLifo()) delay(1L)
                }
            }

            assertEquals("closed", stopped.await().phase)
            val diagnostics = withTimeout(5_000L) { cleanupDiagnostics.await() }
            assertEquals(0, diagnostics.activeOwnedJobs)
            assertEquals(0, diagnostics.activeListeners)
            assertEquals(0, diagnostics.activeRelays)
            assertTrue(diagnostics.actorCompleted)
            assertTrue(diagnostics.controlCollectorCompleted)
            assertEquals(0, client.diagnostics().activeSessions)
            assertEquals(1, control.closeCount.get())
            dispatcher.runAllLifo()
            assertEquals(0, dispatcher.queuedCount)
        } finally {
            parentJob.cancel()
            dispatcher.runAllLifo()
            client.close()
        }
    }

    @Test
    fun defaultSessionLimitIsFour() = runBlocking {
        val controlFactory = FakeControlFactory()
        val sequence = AtomicInteger(0)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            FakeListenerFactory(),
            sessionIdSource = FrpSessionIdSource { "limit-session-${sequence.incrementAndGet()}" },
        )
        val sessions = mutableListOf<String>()
        try {
            repeat(4) { sessions += client.startSession(sessionConfig()) }
            val failure = captureRuntimeFailure { client.startSession(sessionConfig()) }
            assertEquals(KotlinFrpcStcpVisitorFailure.SESSION_LIMIT, failure.failure)
        } finally {
            controlFactory.snapshot().forEach(FakeSessionControl::releaseStop)
            sessions.forEach { client.stopSession(it, 5_000L) }
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun protocolNormalizationDefaultsBlanksButRejectsUppercaseValues() = runBlocking {
        val controlFactory = FakeControlFactory()
        val sequence = AtomicInteger(0)
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            FakeListenerFactory(),
            sessionIdSource = FrpSessionIdSource { "protocol-session-${sequence.incrementAndGet()}" },
        )
        try {
            val blank = client.startSession(
                sessionConfig(transportProtocol = "  ", wireProtocol = "\t"),
            )
            val normalized = controlFactory.configSnapshot().single()
            assertEquals("tcp", normalized.transportProtocol)
            assertEquals("v1", normalized.wireProtocol)
            client.stopSession(blank, 5_000L)

            assertEquals(
                KotlinFrpcStcpVisitorFailure.UNSUPPORTED_CONFIGURATION,
                captureRuntimeFailure {
                    client.startSession(sessionConfig(transportProtocol = "TCP", wireProtocol = "v1"))
                }.failure,
            )
            assertEquals(
                KotlinFrpcStcpVisitorFailure.UNSUPPORTED_CONFIGURATION,
                captureRuntimeFailure {
                    client.startSession(sessionConfig(transportProtocol = "tcp", wireProtocol = "V1"))
                }.failure,
            )
            assertEquals(1, controlFactory.snapshot().size)
        } finally {
            controlFactory.snapshot().forEach(FakeSessionControl::releaseStop)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun bindAndCommandSummariesNeverExposeVisitorName() = runBlocking {
        val marker = "synthetic-private-visitor-name"
        val summaries = Channel<String>(Channel.UNLIMITED)
        val controlFactory = FakeControlFactory()
        val listenerFactory = FakeListenerFactory()
        val parentScope = newParentScope()
        val client = newClient(
            parentScope,
            controlFactory,
            listenerFactory,
            hooks = KotlinFrpcRuntimeLifecycleHooks(
                observeRuntimeCommandSummary = { summary -> check(summaries.trySend(summary).isSuccess) },
            ),
        )
        val sessionId = client.startSession(sessionConfig())
        val control = controlFactory.single()
        try {
            control.open(epoch = 1L, transport = 1L)
            val ensured = client.ensureVisitor(
                sessionId,
                visitorConfig(name = marker, bindPort = 50_064),
            )
            listenerFactory.takeBinding()
            client.waitVisitorReady(sessionId, marker, ensured.desiredRevision, 1_000L)

            val observed = mutableListOf<String>()
            withTimeout(5_000L) {
                while (observed.none { it.startsWith("BindCompletedCommand") }) {
                    observed += summaries.receive()
                }
            }
            assertTrue(observed.any { it.contains("namePresent=true") })
            assertTrue(observed.none { it.contains(marker) })
        } finally {
            control.releaseStop()
            client.stopSession(sessionId, 5_000L)
            client.close()
            parentScope.cancel()
        }
    }

    @Test
    fun productionControlLeavesApplicationHeartbeatDisabled() {
        val config = productionFrpControlConfig(sessionConfig(), FrpWireProtocol.V1)

        assertEquals(FrpControlConfig.DISABLED_HEARTBEAT_MILLIS, config.heartbeatIntervalMillis)
        assertEquals(FrpControlConfig.DISABLED_HEARTBEAT_MILLIS, config.heartbeatTimeoutMillis)
    }

    private fun newClient(
        parentScope: CoroutineScope,
        controlFactory: FrpSessionControlFactory,
        listenerFactory: FrpLocalListenerFactory,
        unixTimeSource: FrpUnixTimeSource = FrpUnixTimeSource { FIXED_TIMESTAMP },
        hooks: KotlinFrpcRuntimeLifecycleHooks = KotlinFrpcRuntimeLifecycleHooks(),
        runtimeDelayer: FrpRuntimeDelayer = CoroutineFrpRuntimeDelayer,
        sessionIdSource: FrpSessionIdSource = FrpSessionIdSource { "runtime-session-fixture" },
        ioDispatcher: kotlinx.coroutines.CoroutineDispatcher = Dispatchers.IO,
    ): KotlinFrpcStcpVisitorClient = KotlinFrpcStcpVisitorClient(
        parentScope = parentScope,
        controlFactory = controlFactory,
        listenerFactory = listenerFactory,
        unixTimeSource = unixTimeSource,
        sessionIdSource = sessionIdSource,
        ioDispatcher = ioDispatcher,
        lifecycleHooks = hooks,
        runtimeDelayer = runtimeDelayer,
        testOnly = Unit,
    )

    private fun sessionConfig(
        user: String? = null,
        transportProtocol: String = "tcp",
        wireProtocol: String = "v1",
    ): FrpcSessionConfig = FrpcSessionConfig(
        serverAddr = "frps.example.invalid",
        serverPort = 7000,
        authToken = "synthetic-token",
        user = user,
        transportProtocol = transportProtocol,
        wireProtocol = wireProtocol,
    )

    private fun visitorConfig(
        name: String = "visitor",
        serverName: String = "remote-service",
        serverUser: String? = null,
        secret: String = SYNTHETIC_SECRET,
        bindAddr: String = "127.0.0.1",
        bindPort: Int,
        useEncryption: Boolean = false,
        useCompression: Boolean = false,
    ): FrpcStcpVisitorConfig = FrpcStcpVisitorConfig(
        name = name,
        serverName = serverName,
        serverUser = serverUser,
        secretKey = secret,
        bindAddr = bindAddr,
        bindPort = bindPort,
        useEncryption = useEncryption,
        useCompression = useCompression,
    )

    private fun encodeVisitorPayload(
        payload: ByteArray,
        useEncryption: Boolean,
        useCompression: Boolean,
    ): ByteArray {
        val encoded = ByteArrayOutputStream()
        var output: OutputStream = encoded
        if (useEncryption) {
            output = FrpV1CfbOutputStream(
                output,
                SYNTHETIC_SECRET,
                ByteArray(FrpV1Cfb.IV_SIZE) { index -> (0x40 + index).toByte() },
            )
        }
        if (useCompression) output = FrpSnappyFramedOutputStream(output)
        output.use { it.write(payload) }
        return encoded.toByteArray()
    }

    private fun decodeVisitorPayload(
        source: InputStream,
        useEncryption: Boolean,
        useCompression: Boolean,
    ): ByteArray {
        var input = source
        if (useEncryption) input = FrpV1Cfb.decrypting(input, SYNTHETIC_SECRET)
        if (useCompression) input = FrpSnappyFramedInputStream(input)
        val output = ByteArrayOutputStream()
        input.use {
            val buffer = ByteArray(257)
            try {
                while (true) {
                    val count = it.read(buffer)
                    if (count < 0) break
                    assertTrue(count > 0)
                    output.write(buffer, 0, count)
                }
            } finally {
                buffer.fill(0)
            }
        }
        return output.toByteArray()
    }

    private suspend fun awaitControlState(
        states: Channel<FrpControlState>,
        predicate: (FrpControlState) -> Boolean,
    ): FrpControlState = withTimeout(5_000L) {
        while (true) {
            val state = states.receive()
            if (predicate(state)) return@withTimeout state
        }
        throw AssertionError("unreachable")
    }

    private suspend fun waitUntil(predicate: suspend () -> Boolean) {
        withTimeout(5_000L) {
            while (!predicate()) delay(10L)
        }
    }

    private suspend fun captureRuntimeFailure(
        block: suspend () -> Any?,
    ): KotlinFrpcStcpVisitorException = try {
        block()
        throw AssertionError("operation unexpectedly succeeded")
    } catch (failure: KotlinFrpcStcpVisitorException) {
        failure
    }

    private fun newParentScope(): CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private companion object {
        const val FIXED_TIMESTAMP = 1_700_000_000L
        const val SYNTHETIC_SECRET = "obviously-synthetic-stcp-secret"
    }
}

private class FakeControlFactory(
    private val protocol: FrpWireProtocol = FrpWireProtocol.V1,
    private val stopRelease: CompletableDeferred<Unit>? = null,
) : FrpSessionControlFactory {
    private val controls = Collections.synchronizedList(mutableListOf<FakeSessionControl>())
    private val configs = Collections.synchronizedList(mutableListOf<FrpcSessionConfig>())

    override fun create(
        config: FrpcSessionConfig,
        parentScope: CoroutineScope,
        generation: Long,
    ): FrpSessionControl {
        configs += config
        return FakeSessionControl(
            generation = generation,
            protocol = protocol,
            parentScope = parentScope,
            stopRelease = stopRelease,
        ).also(controls::add)
    }

    fun single(): FakeSessionControl = synchronized(controls) { controls.single() }

    fun snapshot(): List<FakeSessionControl> = synchronized(controls) { controls.toList() }

    fun configSnapshot(): List<FrpcSessionConfig> = synchronized(configs) { configs.toList() }
}

private class FakeSessionControl(
    private val generation: Long,
    private val protocol: FrpWireProtocol,
    parentScope: CoroutineScope,
    private val stopRelease: CompletableDeferred<Unit>?,
) : FrpSessionControl {
    private val mutableState = MutableStateFlow<FrpControlState>(
        FrpControlState.Connecting(generation, attempt = 0),
    )
    private val streams = Channel<FrpMuxStream>(Channel.UNLIMITED)
    private val activeLeases = Collections.synchronizedSet(mutableSetOf<FrpClientStreamLease>())
    private val cleanupJob = SupervisorJob()
    private val cleanupScope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) + cleanupJob,
    )
    private val stopped = CompletableDeferred<Unit>()
    private val closed = AtomicBoolean(false)
    val closeCount = AtomicInteger(0)
    val invalidateCount = AtomicInteger(0)
    val closeCalled = CompletableDeferred<Unit>()
    var establishmentFailure: FrpControlFailure? = null
    var reconnectDiagnostics = FrpControlReconnectDiagnostics()

    override val state: StateFlow<FrpControlState> = mutableState

    override fun start() = Unit

    override fun invalidateTransport(): FrpControlIdentity? {
        val identity = (mutableState.value as? FrpControlState.Open)?.identity ?: return null
        invalidateCount.incrementAndGet()
        retry(attempt = 1)
        return identity
    }

    override fun reconnectEstablishmentFailure(): FrpControlFailure? = establishmentFailure

    override fun reconnectDiagnostics(): FrpControlReconnectDiagnostics = reconnectDiagnostics

    fun open(epoch: Long, transport: Long): FrpControlIdentity =
        FrpControlIdentity(generation, epoch, transport).also { identity ->
            mutableState.value = FrpControlState.Open(identity)
        }

    fun retry(
        attempt: Int,
        failure: FrpControlFailure = FrpControlFailure.TRANSPORT_FAILED,
    ) {
        mutableState.value = FrpControlState.Retrying(
            generation = generation,
            attempt = attempt,
            delayMillis = 100L,
            failure = failure,
        )
    }

    fun fail() {
        mutableState.value = FrpControlState.Failed(
            generation = generation,
            failure = io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlFailure.TRANSPORT_FAILED,
        )
    }

    fun enqueueStream(stream: FrpMuxStream) {
        check(streams.trySend(stream).isSuccess)
    }

    override suspend fun openClientStream(expectedIdentity: FrpControlIdentity): FrpClientStreamLease {
        val current = mutableState.value as? FrpControlState.Open
            ?: throw FrpTransportException(FrpTransportFailure.CLOSED)
        if (current.identity != expectedIdentity) throw FrpTransportException(FrpTransportFailure.CLOSED)
        val stream = streams.receive()
        lateinit var lease: FrpClientStreamLease
        lease = FrpClientStreamLease(
            identity = expectedIdentity,
            runId = "run-fixture",
            wireProtocol = protocol,
            delegate = stream,
            cleanupScope = cleanupScope,
            onReleased = { activeLeases.remove(it) },
        )
        activeLeases += lease
        return lease
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        closeCount.incrementAndGet()
        closeCalled.complete(Unit)
        mutableState.value = FrpControlState.Closed(generation)
        synchronized(activeLeases) { activeLeases.toList() }.forEach(FrpClientStreamLease::beginReset)
        cleanupScope.launch {
            stopRelease?.await()
            val leases = synchronized(activeLeases) { activeLeases.toList() }
            leases.forEach { it.reset() }
            stopped.complete(Unit)
            cleanupJob.cancel()
        }
    }

    fun releaseStop() {
        if (stopRelease == null) stopped.complete(Unit)
    }

    override suspend fun awaitStopped() {
        stopped.await()
    }

    override fun toString(): String =
        "FakeSessionControl(generation=$generation, protocol=$protocol, closed=${closed.get()})"
}

private class FakeListenerFactory(
    private val firstEphemeralPort: Int = 43_100,
) : FrpLocalListenerFactory {
    private val bindings = Channel<FakeBinding>(Channel.UNLIMITED)
    private val recordedBindings = Collections.synchronizedList(mutableListOf<FakeBinding>())
    private val ephemeralSequence = AtomicInteger(firstEphemeralPort)
    val bindCount = AtomicInteger(0)
    @Volatile
    var nextFailure: Exception? = null

    override suspend fun bind(address: InetAddress, port: Int): FrpLocalListener {
        bindCount.incrementAndGet()
        nextFailure?.let { failure ->
            nextFailure = null
            throw failure
        }
        val listener = FakeLocalListener(if (port == 0) ephemeralSequence.getAndIncrement() else port)
        val binding = FakeBinding(address, port, listener)
        recordedBindings += binding
        bindings.send(binding)
        return listener
    }

    suspend fun takeBinding(): FakeBinding = withTimeout(5_000L) { bindings.receive() }

    fun bindingSnapshot(): List<FakeBinding> = synchronized(recordedBindings) { recordedBindings.toList() }
}

private class LateListenerFactory : FrpLocalListenerFactory {
    private val requests = Channel<LateBindRequest>(Channel.UNLIMITED)
    private val recorded = Collections.synchronizedList(mutableListOf<LateBindRequest>())

    override suspend fun bind(address: InetAddress, port: Int): FrpLocalListener {
        val request = LateBindRequest(address, port, FakeLocalListener(port))
        recorded += request
        requests.send(request)
        return try {
            withContext(NonCancellable) { request.release.await() }
            currentCoroutineContext().ensureActive()
            request.listener
        } catch (failure: CancellationException) {
            request.listener.close()
            throw failure
        } catch (failure: Error) {
            request.listener.close()
            throw failure
        } catch (failure: Exception) {
            request.listener.close()
            throw failure
        }
    }

    suspend fun takeRequest(): LateBindRequest = withTimeout(5_000L) { requests.receive() }

    fun releaseAll() {
        synchronized(recorded) { recorded.toList() }.forEach { it.release.complete(Unit) }
    }
}

private data class LateBindRequest(
    val address: InetAddress,
    val requestedPort: Int,
    val listener: FakeLocalListener,
    val release: CompletableDeferred<Unit> = CompletableDeferred(),
)

private class ManualRuntimeDelayer : FrpRuntimeDelayer {
    private val calls = Channel<RuntimeDelayCall>(Channel.UNLIMITED)

    override suspend fun delayMillis(milliseconds: Long) {
        val call = RuntimeDelayCall(milliseconds)
        calls.send(call)
        call.release.await()
    }

    suspend fun takeCall(expectedMillis: Long): RuntimeDelayCall = withTimeout(5_000L) {
        calls.receive().also { assertEquals(expectedMillis, it.millis) }
    }
}

private data class RuntimeDelayCall(
    val millis: Long,
    val release: CompletableDeferred<Unit> = CompletableDeferred(),
)

private data class FakeBinding(
    val address: InetAddress,
    val requestedPort: Int,
    val listener: FakeLocalListener,
)

private class FakeLocalListener(
    override val bindPort: Int,
) : FrpLocalListener {
    private val connections = Channel<FrpLocalConnection>(Channel.UNLIMITED)
    private val closed = AtomicBoolean(false)
    val closeCount = AtomicInteger(0)
    val acceptCount = AtomicInteger(0)
    @Volatile
    var closeFailure: Error? = null

    fun offer(connection: FrpLocalConnection) {
        if (!connections.trySend(connection).isSuccess) connection.close()
    }

    fun failAccept(failure: Throwable) {
        connections.close(failure)
    }

    override suspend fun accept(): FrpLocalConnection = connections.receive().also { acceptCount.incrementAndGet() }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        closeCount.incrementAndGet()
        connections.close()
        closeFailure?.let { throw it }
    }

    override fun toString(): String = "FakeLocalListener(closed=${closed.get()})"
}

private class FakeLocalConnection(
    inputBytes: ByteArray,
) : FrpLocalConnection {
    private val closed = AtomicBoolean(false)
    private val outputBytes = ByteArrayOutputStream()
    override val input: InputStream = ByteArrayInputStream(inputBytes)
    override val output: OutputStream = outputBytes
    val shutdownOutputCount = AtomicInteger(0)
    val closeCount = AtomicInteger(0)

    override fun shutdownOutput() {
        shutdownOutputCount.incrementAndGet()
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        closeCount.incrementAndGet()
    }

    fun writtenBytes(): ByteArray = outputBytes.toByteArray()

    override fun toString(): String = "FakeLocalConnection(closed=${closed.get()})"
}

private class BlockingLocalConnection : FrpLocalConnection {
    private val release = CompletableDeferred<Unit>()
    private val closed = AtomicBoolean(false)
    private val outputBytes = ByteArrayOutputStream()
    val readStarted = CompletableDeferred<Unit>()
    val shutdownOutputCount = AtomicInteger(0)
    val closeCount = AtomicInteger(0)

    override val input: InputStream = object : InputStream() {
        override fun read(): Int {
            readStarted.complete(Unit)
            runBlocking { release.await() }
            return -1
        }

        override fun read(destination: ByteArray, offset: Int, length: Int): Int = read()
    }

    override val output: OutputStream = outputBytes

    override fun shutdownOutput() {
        shutdownOutputCount.incrementAndGet()
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        closeCount.incrementAndGet()
        release.complete(Unit)
    }

    fun releaseRead() {
        release.complete(Unit)
    }
}

private class UninterruptibleLocalConnection : FrpLocalConnection {
    private val readStarted = CountDownLatch(1)
    private val releaseRead = CountDownLatch(1)
    private val closed = AtomicBoolean(false)
    override val input: InputStream = object : InputStream() {
        override fun read(): Int {
            readStarted.countDown()
            while (true) {
                try {
                    releaseRead.await()
                    return -1
                } catch (_: InterruptedException) {
                    // The fixture models a socket read that ignores thread interruption.
                }
            }
        }

        override fun read(destination: ByteArray, offset: Int, length: Int): Int = read()
    }
    override val output: OutputStream = ByteArrayOutputStream()
    val closeCount = AtomicInteger(0)

    suspend fun awaitReadStarted() {
        assertTrue(withContext(Dispatchers.IO) { readStarted.await(5, TimeUnit.SECONDS) })
    }

    override fun shutdownOutput() = Unit

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        closeCount.incrementAndGet()
        releaseRead.countDown()
    }
}

private class ZeroReadLocalConnection : FrpLocalConnection {
    private val closed = AtomicBoolean(false)
    override val input: InputStream = object : InputStream() {
        override fun read(): Int = 0

        override fun read(destination: ByteArray, offset: Int, length: Int): Int = 0
    }
    override val output: OutputStream = ByteArrayOutputStream()

    override fun shutdownOutput() = Unit

    override fun close() {
        closed.set(true)
    }
}

private class ScriptedMuxStream(
    inputBytes: ByteArray,
    private val firstReadRelease: CompletableDeferred<Unit>? = null,
    private val afterInputRelease: CompletableDeferred<Unit>? = null,
    private val afterInputResult: Int = -1,
    private val afterInputFailure: Throwable? = null,
    private val closeWriteRelease: CompletableDeferred<Unit>? = null,
    private val closeWriteFailure: Throwable? = null,
    private val resetRelease: CompletableDeferred<Unit>? = null,
    private val resetFailure: Throwable? = null,
) : FrpMuxStream {
    private val input = inputBytes.copyOf()
    private var inputOffset = 0
    private val output = ByteArrayOutputStream()
    private val outputLock = Any()
    private val reset = AtomicBoolean(false)
    val closeWriteCount = AtomicInteger(0)
    val closeWriteStarted = CompletableDeferred<Unit>()
    val resetCount = AtomicInteger(0)
    val resetStarted = CompletableDeferred<Unit>()
    val readStarted = CompletableDeferred<Unit>()
    private val firstRead = AtomicBoolean(true)

    override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int {
        readStarted.complete(Unit)
        if (firstRead.compareAndSet(true, false)) firstReadRelease?.await()
        if (reset.get()) return -1
        if (inputOffset >= input.size) {
            afterInputRelease?.await()
            afterInputFailure?.let { throw it }
            if (reset.get()) return -1
            return afterInputResult
        }
        val count = minOf(length, input.size - inputOffset)
        input.copyInto(destination, offset, inputOffset, inputOffset + count)
        inputOffset += count
        return count
    }

    override suspend fun write(source: ByteArray, offset: Int, length: Int) {
        if (reset.get()) throw FrpTransportException(FrpTransportFailure.CLOSED)
        synchronized(outputLock) { output.write(source, offset, length) }
    }

    override suspend fun closeWrite() {
        closeWriteCount.incrementAndGet()
        closeWriteStarted.complete(Unit)
        closeWriteRelease?.let { release ->
            withContext(NonCancellable) { release.await() }
        }
        closeWriteFailure?.let { throw it }
    }

    override suspend fun reset() {
        if (!reset.compareAndSet(false, true)) return
        resetCount.incrementAndGet()
        resetStarted.complete(Unit)
        firstReadRelease?.complete(Unit)
        afterInputRelease?.complete(Unit)
        resetRelease?.await()
        resetFailure?.let { throw it }
    }

    fun writtenBytes(): ByteArray = synchronized(outputLock) { output.toByteArray() }

    override fun toString(): String = "ScriptedMuxStream(reset=${reset.get()})"
}

private class QueueingDispatcher : CoroutineDispatcher() {
    private val queue = ConcurrentLinkedDeque<Runnable>()

    val queuedCount: Int
        get() = queue.size

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        queue.addLast(block)
    }

    fun runNextLifo(): Boolean {
        val next = queue.pollLast() ?: return false
        next.run()
        return true
    }

    fun runAllLifo() {
        while (runNextLifo()) {
            // Drain tasks added by cleanup continuations before returning.
        }
    }
}

private class CountingDispatcher(
    private val delegate: CoroutineDispatcher,
) : CoroutineDispatcher() {
    val dispatchCount = AtomicInteger(0)

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        dispatchCount.incrementAndGet()
        delegate.dispatch(context, block)
    }
}
