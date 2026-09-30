package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcSessionConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcStcpVisitorConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.KotlinFrpcStcpVisitorClient
import io.github.ycfeng.ocdeck.frpcstcpvisitor.KotlinFrpcStcpVisitorException
import io.github.ycfeng.ocdeck.frpcstcpvisitor.KotlinFrpcStcpVisitorFailure
import java.net.BindException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Path
import java.security.SecureRandom
import java.util.Base64
import java.util.LinkedHashSet
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

object FrpcInteropHarness {
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            require(args.isEmpty()) { "frpcInteropTest does not accept arguments" }
            runBlocking { FrpcInteropRunner().run() }
        } catch (failure: InteropFailure) {
            System.err.println("frpc interop failed: ${failure.message.orEmpty()}")
            exitProcess(1)
        } catch (_: Exception) {
            System.err.println("frpc interop failed: unexpected harness failure")
            exitProcess(1)
        }
    }
}

private class FrpcInteropRunner {
    private val secureRandom = SecureRandom()
    private val allocatedPorts = LinkedHashSet<Int>()
    private var stage = "initialization"
    private var redactor = InteropRedactor()
    private var tlsCertificateSha256 = ""

    suspend fun run() {
        val resources = InteropResourceOwner()
        val topology = resources.topology
        var selectedFailure: InteropFailure? = null
        val shutdownHook = Thread(
            { resources.cleanup() },
            "frp-interop-shutdown-cleanup",
        ).apply { isDaemon = false }
        Runtime.getRuntime().addShutdownHook(shutdownHook)
        try {
            stage = "temporary workspace creation"
            val temporaryDirectory = resources.createTemporaryWorkspace()
            val secrets = InteropSecrets.create(secureRandom)
            val cacheDirectory = configuredCacheDirectory()
            redactor = InteropRedactor(
                temporaryDirectory.toString(),
                cacheDirectory.toString(),
                secrets.authToken,
                secrets.stcpSecret,
                secrets.wrongAuthToken,
                secrets.wrongStcpSecret,
            )

            stage = "official frp provisioning"
            val binaries = FrpReleaseProvisioner(cacheDirectory).provision()
            println("frpc interop: official frp 0.69.1 provisioned and verified")

            stage = "synthetic OpenCode server startup"
            val syntheticServer = resources.createSyntheticServer()
            allocatedPorts += syntheticServer.port
            val frpsPort = allocateLoopbackPort()
            val frpsConfig = temporaryDirectory.resolve("frps.toml")
            val providerConfig = temporaryDirectory.resolve("frpc-provider.toml")
            stage = "temporary TLS material creation"
            val tlsMaterial = InteropTlsMaterial.create(temporaryDirectory, secureRandom)
            tlsCertificateSha256 = tlsMaterial.certificateSha256
            redactor = redactor.including(
                tlsMaterial.certificatePath.toString(),
                tlsMaterial.privateKeyPath.toString(),
            )
            writePrivateFile(frpsConfig, frpsToml(frpsPort, secrets.authToken, tlsMaterial))
            writePrivateFile(
                providerConfig,
                providerToml(frpsPort, syntheticServer.port, secrets.authToken, secrets.stcpSecret),
            )
            redactor = redactor.including(frpsConfig.toString(), providerConfig.toString())

            stage = "frps startup"
            val frps = resources.startFrps {
                this@FrpcInteropRunner.startFrps(binaries, frpsConfig, temporaryDirectory)
            }
            awaitFrpsReady(frpsPort, frps, "frps startup")

            stage = "provider frpc startup"
            val provider = resources.startProvider {
                ManagedInteropProcess.start(
                    label = "provider-frpc",
                    executable = binaries.frpc,
                    arguments = listOf("-c", providerConfig.toString()),
                    workingDirectory = temporaryDirectory,
                    redactor = redactor,
                )
            }
            provider.awaitOutput(PROVIDER_READY_PATTERN, PROCESS_READY_TIMEOUT_MILLIS, "provider startup")

            val echoPayload = createEchoPayload()
            try {
                for (wireProtocol in listOf("v1", "v2")) {
                    for (useEncryption in listOf(false, true)) {
                        for (useCompression in listOf(false, true)) {
                            stage = "interop matrix $wireProtocol/$useEncryption/$useCompression"
                            runSuccessScenario(
                                frpsPort = frpsPort,
                                wireProtocol = wireProtocol,
                                useEncryption = useEncryption,
                                useCompression = useCompression,
                                authToken = secrets.authToken,
                                stcpSecret = secrets.stcpSecret,
                                payload = echoPayload,
                                topology = topology,
                                syntheticServer = syntheticServer,
                            )
                            println(
                                "frpc interop scenario passed: wire=$wireProtocol " +
                                    "encryption=$useEncryption compression=$useCompression",
                            )
                        }
                    }
                }
                stage = "wrong token scenario"
                runWrongTokenScenario(frpsPort, secrets.wrongAuthToken, secrets.stcpSecret)
                println("frpc interop negative scenario passed: wrong token")

                stage = "wrong TLS certificate pin scenario"
                runWrongTlsPinScenario(frpsPort, secrets.authToken, secrets.stcpSecret)
                println("frpc interop negative scenario passed: wrong TLS certificate pin")

                stage = "wrong STCP secret scenario"
                runWrongSecretScenario(frpsPort, secrets.authToken, secrets.wrongStcpSecret, topology)
                println("frpc interop negative scenario passed: wrong STCP secret")

                stage = "bind conflict scenario"
                runBindConflictScenario(frpsPort, secrets.authToken, secrets.stcpSecret)
                println("frpc interop negative scenario passed: bind port conflict")

                stage = "frps restart recovery scenario"
                runRestartRecoveryScenario(
                    frpsPort = frpsPort,
                    authToken = secrets.authToken,
                    stcpSecret = secrets.stcpSecret,
                    binaries = binaries,
                    frpsConfig = frpsConfig,
                    workingDirectory = temporaryDirectory,
                    topology = topology,
                    resources = resources,
                    payload = echoPayload,
                    syntheticServer = syntheticServer,
                )
                println("frpc interop recovery scenario passed: control epoch advanced after frps restart")
            } finally {
                echoPayload.fill(0)
            }
        } catch (failure: Exception) {
            selectedFailure = safeFailure(stage, failure, topology)
        } finally {
            stage = "process cleanup"
            val cleanupFailure = resources.cleanup()
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook)
            } catch (_: IllegalStateException) {
                // JVM shutdown has already started; the hook owns cleanup.
            }
            if (cleanupFailure is Error) throw cleanupFailure
            if (cleanupFailure != null) {
                val safeCleanup = InteropFailure("interoperability resource cleanup failed")
                selectedFailure = selectedFailure?.let {
                    InteropFailure("${it.message.orEmpty()}\n${safeCleanup.message.orEmpty()}")
                } ?: safeCleanup
            }
        }
        selectedFailure?.let { throw it }
    }

    private suspend fun runSuccessScenario(
        frpsPort: Int,
        wireProtocol: String,
        useEncryption: Boolean,
        useCompression: Boolean,
        authToken: String,
        stcpSecret: String,
        payload: ByteArray,
        topology: FrpProcessTopology,
        syntheticServer: SyntheticOpenCodeServer,
    ) = withTimeout(SCENARIO_TIMEOUT_MILLIS) {
        val scenario = "interop matrix $wireProtocol/$useEncryption/$useCompression"
        val bindPort = allocateLoopbackPort()
        val client = KotlinFrpcStcpVisitorClient()
        var sessionId: String? = null
        var primaryFailure: Throwable? = null
        try {
            stage = "$scenario session startup"
            sessionId = client.startSession(sessionConfig(frpsPort, authToken, wireProtocol))
            stage = "$scenario visitor setup"
            val ensured = client.ensureVisitor(
                sessionId,
                visitorConfig(bindPort, stcpSecret, useEncryption, useCompression),
            )
            if (ensured.bindPort != bindPort || ensured.desiredRevision <= 0L) {
                throw InteropFailure("public ensureVisitor result was invalid")
            }
            stage = "$scenario visitor readiness"
            val ready = client.waitVisitorReady(
                sessionId,
                VISITOR_NAME,
                ensured.desiredRevision,
                VISITOR_READY_TIMEOUT_MILLIS,
            )
            if (!ready.listenerBound || ready.phase != "ready" || ready.boundControlEpoch <= 0L) {
                throw InteropFailure("public waitVisitorReady result was invalid")
            }
            val state = client.getState(sessionId)
            val visitorState = state.visitors[VISITOR_NAME]
            if (state.phase != "open" || state.controlEpoch != ready.boundControlEpoch ||
                visitorState == null || visitorState.phase != "ready" || !visitorState.listenerBound ||
                visitorState.boundControlEpoch != state.controlEpoch
            ) {
                throw InteropFailure("public getState result was inconsistent with readiness")
            }
            stage = "$scenario tunnel health"
            awaitTunnelHealth(bindPort, topology)
            stage = "$scenario concurrent REST/SSE and flow control"
            runConcurrentTunnelTraffic(bindPort, payload, syntheticServer)
            stage = "$scenario relay settlement"
            // Let relay completion reach the session actor before testing explicit visitor shutdown.
            delay(RELAY_SETTLE_MILLIS)
        } catch (failure: Throwable) {
            primaryFailure = failure
            throw failure
        } finally {
            val cleanup = cleanupClient(client, sessionId, bindPort)
            if (primaryFailure == null && cleanup != null) throw cleanup
        }
    }

    private suspend fun runWrongTokenScenario(
        frpsPort: Int,
        wrongToken: String,
        runtimeSecret: String,
    ) =
        withTimeout(NEGATIVE_SCENARIO_TIMEOUT_MILLIS) {
            val bindPort = allocateLoopbackPort()
            val client = KotlinFrpcStcpVisitorClient()
            var sessionId: String? = null
            var primaryFailure: Throwable? = null
            try {
                sessionId = client.startSession(sessionConfig(frpsPort, wrongToken, "v2"))
                val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort, runtimeSecret, false, false))
                expectKotlinFailure(KotlinFrpcStcpVisitorFailure.CONTROL_FAILED) {
                    client.waitVisitorReady(
                        sessionId,
                        VISITOR_NAME,
                        ensured.desiredRevision,
                        VISITOR_READY_TIMEOUT_MILLIS,
                    )
                }
                if (client.getState(sessionId).phase != "failed") {
                    throw InteropFailure("wrong token did not produce a failed public state")
                }
            } catch (failure: Throwable) {
                primaryFailure = failure
                throw failure
            } finally {
                val cleanup = cleanupClient(client, sessionId, bindPort)
                if (primaryFailure == null && cleanup != null) throw cleanup
            }
        }

    private suspend fun runWrongTlsPinScenario(
        frpsPort: Int,
        authToken: String,
        runtimeSecret: String,
    ) = withTimeout(NEGATIVE_SCENARIO_TIMEOUT_MILLIS) {
        val bindPort = allocateLoopbackPort()
        val client = KotlinFrpcStcpVisitorClient()
        var sessionId: String? = null
        var primaryFailure: Throwable? = null
        try {
            sessionId = client.startSession(
                sessionConfig(
                    frpsPort = frpsPort,
                    authToken = authToken,
                    wireProtocol = "v2",
                    certificateSha256 = "00".repeat(32),
                ),
            )
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort, runtimeSecret, false, false))
            expectKotlinFailure(KotlinFrpcStcpVisitorFailure.CONTROL_FAILED) {
                client.waitVisitorReady(
                    sessionId,
                    VISITOR_NAME,
                    ensured.desiredRevision,
                    VISITOR_READY_TIMEOUT_MILLIS,
                )
            }
            if (client.getState(sessionId).phase != "failed") {
                throw InteropFailure("wrong TLS certificate pin did not produce a failed public state")
            }
        } catch (failure: Throwable) {
            primaryFailure = failure
            throw failure
        } finally {
            val cleanup = cleanupClient(client, sessionId, bindPort)
            if (primaryFailure == null && cleanup != null) throw cleanup
        }
    }

    private suspend fun runWrongSecretScenario(
        frpsPort: Int,
        authToken: String,
        wrongSecret: String,
        topology: FrpProcessTopology,
    ) = withTimeout(NEGATIVE_SCENARIO_TIMEOUT_MILLIS) {
        val bindPort = allocateLoopbackPort()
        val client = KotlinFrpcStcpVisitorClient()
        var sessionId: String? = null
        var primaryFailure: Throwable? = null
        try {
            sessionId = client.startSession(sessionConfig(frpsPort, authToken, "v2"))
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort, wrongSecret, false, false))
            client.waitVisitorReady(
                sessionId,
                VISITOR_NAME,
                ensured.desiredRevision,
                VISITOR_READY_TIMEOUT_MILLIS,
            )
            topology.requireAlive("wrong STCP secret scenario")
            val unexpectedlyHealthy = try {
                TunnelHttpProbe.health(bindPort)
                true
            } catch (_: TunnelProbeException) {
                false
            }
            if (unexpectedlyHealthy) throw InteropFailure("wrong STCP secret unexpectedly opened the tunnel")
            val state = client.getState(sessionId)
            if (state.phase != "open" || state.visitors[VISITOR_NAME]?.listenerBound != true) {
                throw InteropFailure("wrong STCP secret corrupted the control/listener state")
            }
        } catch (failure: Throwable) {
            primaryFailure = failure
            throw failure
        } finally {
            val cleanup = cleanupClient(client, sessionId, bindPort)
            if (primaryFailure == null && cleanup != null) throw cleanup
        }
    }

    private suspend fun runBindConflictScenario(
        frpsPort: Int,
        authToken: String,
        stcpSecret: String,
    ) = withTimeout(NEGATIVE_SCENARIO_TIMEOUT_MILLIS) {
        val occupied = ServerSocket().apply {
            reuseAddress = false
            bind(InetSocketAddress(IPV4_LOOPBACK, 0))
        }
        val bindPort = occupied.localPort
        allocatedPorts += bindPort
        val client = KotlinFrpcStcpVisitorClient()
        var sessionId: String? = null
        var primaryFailure: Throwable? = null
        try {
            sessionId = client.startSession(sessionConfig(frpsPort, authToken, "v2"))
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort, stcpSecret, false, false))
            expectKotlinFailure(KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT) {
                client.waitVisitorReady(
                    sessionId,
                    VISITOR_NAME,
                    ensured.desiredRevision,
                    VISITOR_READY_TIMEOUT_MILLIS,
                )
            }
            val visitor = client.getState(sessionId).visitors[VISITOR_NAME]
            if (visitor?.phase != "failed" || visitor.listenerBound) {
                throw InteropFailure("bind conflict did not produce the expected public visitor state")
            }
        } catch (failure: Throwable) {
            primaryFailure = failure
            throw failure
        } finally {
            val cleanup = cleanupClient(client, sessionId, bindPort, confirmPortRelease = false)
            try {
                occupied.close()
            } catch (_: Exception) {
                // The occupied socket exists only to force the typed bind failure.
            }
            val releaseFailure = try {
                awaitPortReleased(bindPort)
                null
            } catch (failure: InteropFailure) {
                failure
            }
            if (primaryFailure == null) {
                cleanup?.let { throw it }
                releaseFailure?.let { throw it }
            }
        }
    }

    private suspend fun runRestartRecoveryScenario(
        frpsPort: Int,
        authToken: String,
        stcpSecret: String,
        binaries: FrpReleaseBinaries,
        frpsConfig: Path,
        workingDirectory: Path,
        topology: FrpProcessTopology,
        resources: InteropResourceOwner,
        payload: ByteArray,
        syntheticServer: SyntheticOpenCodeServer,
    ) = withTimeout(RESTART_SCENARIO_TIMEOUT_MILLIS) {
        val bindPort = allocateLoopbackPort()
        val client = KotlinFrpcStcpVisitorClient()
        var sessionId: String? = null
        var primaryFailure: Throwable? = null
        try {
            sessionId = client.startSession(sessionConfig(frpsPort, authToken, "v2"))
            val ensured = client.ensureVisitor(sessionId, visitorConfig(bindPort, stcpSecret, true, true))
            val initialReady = client.waitVisitorReady(
                sessionId,
                VISITOR_NAME,
                ensured.desiredRevision,
                VISITOR_READY_TIMEOUT_MILLIS,
            )
            awaitTunnelHealth(bindPort, topology)
            val initialEpoch = initialReady.boundControlEpoch

            val liveGlobal = withContext(Dispatchers.IO) {
                TunnelHttpProbe.openSseSession(bindPort, "/global/event", "{\"scope\":\"global\"}")
            }
            val liveProject = try {
                withContext(Dispatchers.IO) {
                    TunnelHttpProbe.openSseSession(bindPort, "/event", "{\"scope\":\"project\"}")
                }
            } catch (failure: Throwable) {
                liveGlobal.close()
                throw failure
            }

            try {
                resources.stopFrps()
                withContext(Dispatchers.IO) {
                    listOf(
                        async { liveGlobal.awaitDisconnected(SSE_DISCONNECT_TIMEOUT_MILLIS) },
                        async { liveProject.awaitDisconnected(SSE_DISCONNECT_TIMEOUT_MILLIS) },
                    ).awaitAll()
                }
                awaitControlUnavailable(client, sessionId)
                topology.provider?.requireAlive("provider reconnect")
            } finally {
                closeSseSessions(liveGlobal, liveProject)
            }

            val restarted = resources.startFrps {
                this@FrpcInteropRunner.startFrps(binaries, frpsConfig, workingDirectory)
            }
            awaitFrpsReady(frpsPort, restarted, "frps restart")
            val recovered = client.waitVisitorReady(
                sessionId,
                VISITOR_NAME,
                ensured.desiredRevision,
                RESTART_READY_TIMEOUT_MILLIS,
            )
            if (recovered.boundControlEpoch <= initialEpoch) {
                throw InteropFailure("control epoch did not advance after frps restart")
            }
            val recoveredState = client.getState(sessionId)
            if (recoveredState.controlEpoch != recovered.boundControlEpoch ||
                recoveredState.visitors[VISITOR_NAME]?.boundControlEpoch != recovered.boundControlEpoch
            ) {
                throw InteropFailure("recovered public state did not match the new control epoch")
            }
            awaitTunnelHealth(bindPort, topology)
            runConcurrentTunnelTraffic(bindPort, payload, syntheticServer)
        } catch (failure: Throwable) {
            primaryFailure = failure
            throw failure
        } finally {
            val cleanup = cleanupClient(client, sessionId, bindPort)
            if (primaryFailure == null && cleanup != null) throw cleanup
        }
    }

    private suspend fun cleanupClient(
        client: KotlinFrpcStcpVisitorClient,
        sessionId: String?,
        bindPort: Int,
        confirmPortRelease: Boolean = true,
    ): InteropFailure? = withContext(NonCancellable) {
        var failure: InteropFailure? = null
        if (sessionId != null) {
            try {
                withTimeout(CLIENT_CLEANUP_TIMEOUT_MILLIS) {
                    client.stopVisitor(sessionId, VISITOR_NAME)
                }
            } catch (_: TimeoutCancellationException) {
                failure = InteropFailure("stopVisitor cleanup exceeded its deadline")
            } catch (cleanupFailure: KotlinFrpcStcpVisitorException) {
                failure = InteropFailure("stopVisitor cleanup failed with ${cleanupFailure.failure.name}")
            } catch (_: Exception) {
                failure = InteropFailure("stopVisitor cleanup failed")
            }
            try {
                val stopped = withTimeout(CLIENT_CLEANUP_TIMEOUT_MILLIS) {
                    client.stopSession(sessionId, CLIENT_CLEANUP_TIMEOUT_MILLIS)
                }
                if (stopped.phase != "closed" || client.getState(sessionId).phase != "closed") {
                    if (failure == null) failure = InteropFailure("stopSession did not publish closed state")
                }
            } catch (_: TimeoutCancellationException) {
                if (failure == null) failure = InteropFailure("stopSession cleanup exceeded its deadline")
            } catch (cleanupFailure: KotlinFrpcStcpVisitorException) {
                if (failure == null) {
                    failure = InteropFailure("stopSession cleanup failed with ${cleanupFailure.failure.name}")
                }
            } catch (_: Exception) {
                if (failure == null) failure = InteropFailure("stopSession cleanup failed")
            }
        }
        try {
            client.close()
        } catch (_: Exception) {
            if (failure == null) failure = InteropFailure("Kotlin client cleanup failed")
        }
        if (confirmPortRelease) {
            try {
                awaitPortReleased(bindPort)
            } catch (releaseFailure: InteropFailure) {
                if (failure == null) failure = releaseFailure
            }
        }
        failure
    }

    private suspend fun awaitTunnelHealth(bindPort: Int, topology: FrpProcessTopology) {
        val deadline = InteropDeadline.afterMillis(APPLICATION_READY_TIMEOUT_MILLIS)
        var delayMillis = 25L
        while (!deadline.isExpired()) {
            topology.requireAlive("tunnel readiness")
            try {
                TunnelHttpProbe.health(bindPort)
                return
            } catch (_: TunnelProbeException) {
                delay(minOf(delayMillis, deadline.remainingMillis()).coerceAtLeast(1L))
                delayMillis = (delayMillis * 2L).coerceAtMost(250L)
            }
        }
        throw InteropFailure("tunnel health did not become ready within its deadline")
    }

    private suspend fun awaitControlUnavailable(client: KotlinFrpcStcpVisitorClient, sessionId: String) {
        val deadline = InteropDeadline.afterMillis(CONTROL_UNAVAILABLE_TIMEOUT_MILLIS)
        while (!deadline.isExpired()) {
            val state = client.getState(sessionId)
            if (state.phase != "open" || state.visitors[VISITOR_NAME]?.listenerBound != true) return
            delay(25L)
        }
        throw InteropFailure("Kotlin control did not observe the frps shutdown")
    }

    private suspend fun awaitFrpsReady(port: Int, process: ManagedInteropProcess, stage: String) {
        withContext(Dispatchers.IO) {
            process.awaitOutput(FRPS_READY_PATTERN, PROCESS_READY_TIMEOUT_MILLIS, stage)
        }
        val deadline = InteropDeadline.afterMillis(PROCESS_READY_TIMEOUT_MILLIS)
        while (!deadline.isExpired()) {
            process.requireAlive(stage)
            val connected = try {
                Socket().use { socket ->
                    socket.connect(InetSocketAddress(IPV4_LOOPBACK, port), PORT_PROBE_TIMEOUT_MILLIS)
                }
                true
            } catch (_: Exception) {
                false
            }
            if (connected) return
            delay(25L)
        }
        throw InteropFailure("$stage did not bind its loopback port within the deadline")
    }

    private suspend fun runConcurrentTunnelTraffic(
        bindPort: Int,
        payload: ByteArray,
        syntheticServer: SyntheticOpenCodeServer,
    ) {
        val global = withContext(Dispatchers.IO) {
            TunnelHttpProbe.openSseSession(bindPort, "/global/event", "{\"scope\":\"global\"}")
        }
        val project = try {
            withContext(Dispatchers.IO) {
                TunnelHttpProbe.openSseSession(bindPort, "/event", "{\"scope\":\"project\"}")
            }
        } catch (failure: Throwable) {
            global.close()
            throw failure
        }
        var primaryFailure: Throwable? = null
        var trafficGate: ConcurrentTrafficGate? = null
        try {
            val gate = syntheticServer.beginConcurrentTraffic(CONCURRENT_LARGE_REQUESTS)
            trafficGate = gate
            coroutineScope {
                listOf(
                    async(Dispatchers.IO) { TunnelHttpProbe.echo(bindPort, payload) },
                    async(Dispatchers.IO) { TunnelHttpProbe.echo(bindPort, payload) },
                    async(Dispatchers.IO) { TunnelHttpProbe.largeDownload(bindPort) },
                    async(Dispatchers.IO) { TunnelHttpProbe.largeDownload(bindPort) },
                    async(Dispatchers.IO) { TunnelHttpProbe.health(bindPort) },
                ).awaitAll()
            }
            gate.requireSatisfied()
            val checkpoint = syntheticServer.publishSseCheckpoint()
            coroutineScope {
                listOf(
                    async(Dispatchers.IO) { global.awaitCheckpoint(checkpoint, SSE_CHECKPOINT_TIMEOUT_MILLIS) },
                    async(Dispatchers.IO) { project.awaitCheckpoint(checkpoint, SSE_CHECKPOINT_TIMEOUT_MILLIS) },
                ).awaitAll()
            }
            syntheticServer.requireHealthy()
        } catch (failure: Throwable) {
            primaryFailure = failure
            throw failure
        } finally {
            var cleanupFailure: InteropFailure? = null
            trafficGate?.let { gate ->
                try {
                    syntheticServer.endConcurrentTraffic(gate)
                } catch (failure: InteropFailure) {
                    cleanupFailure = failure
                }
            }
            val sseCleanupFailure = closeSseSessions(global, project)
            if (cleanupFailure == null) cleanupFailure = sseCleanupFailure
            if (primaryFailure == null && cleanupFailure != null) throw cleanupFailure
        }
    }

    private fun closeSseSessions(vararg sessions: LiveSseSession): InteropFailure? {
        var failure: InteropFailure? = null
        sessions.forEach { session ->
            try {
                session.close()
            } catch (_: Exception) {
                if (failure == null) failure = InteropFailure("SSE probe cleanup failed")
            }
        }
        return failure
    }

    private suspend fun awaitPortReleased(port: Int) {
        val deadline = InteropDeadline.afterMillis(PORT_RELEASE_TIMEOUT_MILLIS)
        while (!deadline.isExpired()) {
            val available = try {
                ServerSocket().use { socket ->
                    // Active listeners still block this bind, while reuse avoids TIME_WAIT false positives.
                    socket.reuseAddress = true
                    socket.bind(InetSocketAddress(IPV4_LOOPBACK, port))
                }
                true
            } catch (_: BindException) {
                false
            } catch (_: Exception) {
                false
            }
            if (available) return
            delay(25L)
        }
        throw InteropFailure("visitor loopback port was not released within the cleanup deadline")
    }

    private fun startFrps(
        binaries: FrpReleaseBinaries,
        config: Path,
        workingDirectory: Path,
    ): ManagedInteropProcess = ManagedInteropProcess.start(
        label = "frps",
        executable = binaries.frps,
        arguments = listOf("-c", config.toString()),
        workingDirectory = workingDirectory,
        redactor = redactor,
    )

    private fun cleanupTopology(topology: FrpProcessTopology?): Throwable? {
        var failure: Throwable? = null
        try {
            topology?.provider?.stop()
        } catch (cleanupFailure: Throwable) {
            failure = cleanupFailure
        }
        try {
            topology?.frps?.stop()
        } catch (cleanupFailure: Throwable) {
            failure = preferCleanupFailure(failure, cleanupFailure)
        }
        return failure
    }

    private fun preferCleanupFailure(current: Throwable?, candidate: Throwable): Throwable = when {
        current == null -> candidate
        current is Error -> current
        candidate is Error -> candidate
        else -> current
    }

    private inner class InteropResourceOwner {
        private val lock = Any()
        private val cleanupComplete = CountDownLatch(1)
        private var cleanupStarted = false
        @Volatile
        private var cleanupFailure: Throwable? = null
        private var temporaryDirectory: Path? = null
        private var syntheticServer: SyntheticOpenCodeServer? = null
        val topology = FrpProcessTopology()

        fun createTemporaryWorkspace(): Path = synchronized(lock) {
            requireActive()
            if (temporaryDirectory != null) throw InteropFailure("temporary workspace was already registered")
            val created = Files.createTempDirectory("ocdeck-frp-interop-")
            try {
                createPrivateDirectory(created)
            } catch (failure: Throwable) {
                deleteTreeBestEffort(created)
                throw failure
            }
            temporaryDirectory = created
            created
        }

        fun createSyntheticServer(): SyntheticOpenCodeServer = synchronized(lock) {
            requireActive()
            if (syntheticServer != null) throw InteropFailure("synthetic server was already registered")
            SyntheticOpenCodeServer().also { syntheticServer = it }
        }

        fun startFrps(factory: () -> ManagedInteropProcess): ManagedInteropProcess = synchronized(lock) {
            requireActive()
            if (topology.frps != null) throw InteropFailure("frps was already registered")
            factory().also { topology.frps = it }
        }

        fun startProvider(factory: () -> ManagedInteropProcess): ManagedInteropProcess = synchronized(lock) {
            requireActive()
            if (topology.provider != null) throw InteropFailure("provider frpc was already registered")
            factory().also { topology.provider = it }
        }

        fun stopFrps() = synchronized(lock) {
            requireActive()
            topology.frps?.stop()
            topology.frps = null
        }

        fun cleanup(): Throwable? {
            val owner = synchronized(lock) {
                if (cleanupStarted) {
                    false
                } else {
                    cleanupStarted = true
                    true
                }
            }
            if (!owner) return awaitCleanupCompletion()

            var failure: Throwable? = null
            try {
                failure = cleanupTopology(topology)
                try {
                    syntheticServer?.close()
                } catch (cleanupFailure: Throwable) {
                    failure = preferCleanupFailure(failure, cleanupFailure)
                }
                try {
                    deleteTreeChecked(temporaryDirectory)
                } catch (cleanupFailure: Throwable) {
                    failure = preferCleanupFailure(failure, cleanupFailure)
                }
                return failure
            } finally {
                cleanupFailure = failure
                cleanupComplete.countDown()
            }
        }

        private fun awaitCleanupCompletion(): Throwable? = try {
            if (!cleanupComplete.await(RESOURCE_CLEANUP_WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                InteropFailure("interoperability resource cleanup did not complete within its deadline")
            } else {
                cleanupFailure
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            InteropFailure("interoperability resource cleanup wait was interrupted")
        }

        private fun requireActive() {
            if (cleanupStarted) throw InteropFailure("interoperability resource cleanup had already started")
        }
    }

    private fun allocateLoopbackPort(): Int {
        repeat(MAXIMUM_PORT_ALLOCATION_ATTEMPTS) {
            val port = try {
                ServerSocket().use { socket ->
                    socket.reuseAddress = false
                    socket.bind(InetSocketAddress(IPV4_LOOPBACK, 0))
                    socket.localPort
                }
            } catch (_: Exception) {
                0
            }
            if (port > 0 && allocatedPorts.add(port)) return port
        }
        throw InteropFailure("a unique loopback port could not be allocated")
    }

    private fun configuredCacheDirectory(): Path = try {
        val configured = System.getProperty("ocdeck.frp.interop.cacheDir")
            ?.takeIf(String::isNotBlank)
            ?: Path.of(
                System.getProperty("user.home"),
                ".gradle",
                "caches",
                "ocdeck",
                "frp-interop",
                "v0.69.1",
            ).toString()
        Path.of(configured).toAbsolutePath().normalize()
    } catch (_: Exception) {
        throw InteropFailure("frp interoperability cache configuration was invalid")
    }

    private fun safeFailure(
        stage: String,
        failure: Throwable,
        topology: FrpProcessTopology?,
    ): InteropFailure {
        val summary = when (failure) {
            is InteropFailure -> "$stage: ${redactor.redact(failure.message.orEmpty())}"
            is CancellationException -> "$stage exceeded its deadline"
            is KotlinFrpcStcpVisitorException ->
                "$stage failed with KotlinFrpcStcpVisitorException(${failure.failure.name})"
            else -> "$stage failed with ${failure.javaClass.simpleName}"
        }
        val diagnostics = topology?.diagnosticSummary().orEmpty()
        return InteropFailure(
            if (diagnostics.isBlank()) summary else "$summary\n${redactor.redact(diagnostics)}",
        )
    }

    private suspend fun expectKotlinFailure(
        expected: KotlinFrpcStcpVisitorFailure,
        block: suspend () -> Unit,
    ) {
        val actual = try {
            block()
            null
        } catch (failure: KotlinFrpcStcpVisitorException) {
            failure.failure
        }
        if (actual != expected) throw InteropFailure("typed Kotlin visitor failure did not match the scenario")
    }

    private fun sessionConfig(
        frpsPort: Int,
        authToken: String,
        wireProtocol: String,
        certificateSha256: String = tlsCertificateSha256,
    ): FrpcSessionConfig =
        FrpcSessionConfig(
            serverAddr = "127.0.0.1",
            serverPort = frpsPort,
            authToken = authToken,
            user = VISITOR_USER,
            transportProtocol = "tcp",
            wireProtocol = wireProtocol,
            tlsServerName = "localhost",
            tlsServerCertificateSha256 = certificateSha256,
        )

    private fun visitorConfig(
        bindPort: Int,
        stcpSecret: String,
        useEncryption: Boolean,
        useCompression: Boolean,
    ): FrpcStcpVisitorConfig = FrpcStcpVisitorConfig(
        name = VISITOR_NAME,
        serverName = PROVIDER_PROXY_NAME,
        serverUser = PROVIDER_USER,
        secretKey = stcpSecret,
        bindAddr = "127.0.0.1",
        bindPort = bindPort,
        useEncryption = useEncryption,
        useCompression = useCompression,
    )

    private fun frpsToml(
        port: Int,
        authToken: String,
        tlsMaterial: InteropTlsMaterial,
    ): String = """
        bindAddr = "127.0.0.1"
        bindPort = $port
        proxyBindAddr = "127.0.0.1"
        detailedErrorsToClient = false

        log.to = "console"
        log.level = "info"
        log.maxDays = 1
        log.disablePrintColor = true

        transport.tcpMux = true
        transport.tls.force = true
        transport.tls.certFile = "${tlsMaterial.certificatePath.fileName}"
        transport.tls.keyFile = "${tlsMaterial.privateKeyPath.fileName}"

        auth.method = "token"
        auth.additionalScopes = ["HeartBeats", "NewWorkConns"]
        auth.token = "${tomlValue(authToken)}"
    """.trimIndent() + "\n"

    private fun providerToml(
        frpsPort: Int,
        syntheticPort: Int,
        authToken: String,
        stcpSecret: String,
    ): String = """
        user = "$PROVIDER_USER"
        serverAddr = "127.0.0.1"
        serverPort = $frpsPort
        loginFailExit = true

        log.to = "console"
        log.level = "info"
        log.maxDays = 1
        log.disablePrintColor = true

        auth.method = "token"
        auth.additionalScopes = ["HeartBeats", "NewWorkConns"]
        auth.token = "${tomlValue(authToken)}"

        transport.protocol = "tcp"
        transport.wireProtocol = "v2"
        transport.tcpMux = true
        transport.tls.enable = true
        transport.tls.disableCustomTLSFirstByte = true

        [[proxies]]
        name = "$PROVIDER_PROXY_NAME"
        type = "stcp"
        secretKey = "${tomlValue(stcpSecret)}"
        localIP = "127.0.0.1"
        localPort = $syntheticPort
        allowUsers = ["$VISITOR_USER"]
        transport.useEncryption = false
        transport.useCompression = false
    """.trimIndent() + "\n"

    private fun tomlValue(value: String): String {
        if (!SAFE_TOML_VALUE.matches(value)) throw InteropFailure("runtime credential generation was invalid")
        return value
    }

    private fun createEchoPayload(): ByteArray = deterministicInteropBytes(ECHO_PAYLOAD_BYTES, ECHO_PAYLOAD_SEED)

    private data class FrpProcessTopology(
        var frps: ManagedInteropProcess? = null,
        var provider: ManagedInteropProcess? = null,
    ) {
        fun requireAlive(stage: String) {
            frps?.requireAlive(stage) ?: throw InteropFailure("$stage: frps was not running")
            provider?.requireAlive(stage) ?: throw InteropFailure("$stage: provider frpc was not running")
        }

        fun diagnosticSummary(): String = listOfNotNull(
            frps?.diagnosticSummary()?.takeIf(String::isNotBlank),
            provider?.diagnosticSummary()?.takeIf(String::isNotBlank),
        ).joinToString("\n")
    }

    private data class InteropSecrets(
        val authToken: String,
        val stcpSecret: String,
        val wrongAuthToken: String,
        val wrongStcpSecret: String,
    ) {
        override fun toString(): String =
            "InteropSecrets(authToken=<redacted>, stcpSecret=<redacted>, " +
                "wrongAuthToken=<redacted>, wrongStcpSecret=<redacted>)"

        companion object {
            fun create(random: SecureRandom): InteropSecrets = InteropSecrets(
                authToken = randomValue(random, "auth"),
                stcpSecret = randomValue(random, "stcp"),
                wrongAuthToken = randomValue(random, "wrong-auth"),
                wrongStcpSecret = randomValue(random, "wrong-stcp"),
            )

            private fun randomValue(random: SecureRandom, prefix: String): String {
                val bytes = ByteArray(32)
                random.nextBytes(bytes)
                return try {
                    "$prefix-${Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)}"
                } finally {
                    bytes.fill(0)
                }
            }
        }
    }

    private companion object {
        val IPV4_LOOPBACK: InetAddress = InetAddress.getByAddress(byteArrayOf(127, 0, 0, 1))
        val SAFE_TOML_VALUE = Regex("[A-Za-z0-9_-]{16,256}")
        const val PROVIDER_USER = "interop-server"
        const val VISITOR_USER = "interop-visitor"
        const val PROVIDER_PROXY_NAME = "interop-opencode"
        const val VISITOR_NAME = "interop-visitor"
        val FRPS_READY_PATTERN = Regex("(?m)frps started successfully")
        val PROVIDER_READY_PATTERN = Regex("(?m)login to server success")
        const val ECHO_PAYLOAD_BYTES = 768 * 1024 + 37
        const val ECHO_PAYLOAD_SEED = 0x5354435056495349L
        const val MAXIMUM_PORT_ALLOCATION_ATTEMPTS = 64
        const val PORT_PROBE_TIMEOUT_MILLIS = 250
        const val PROCESS_READY_TIMEOUT_MILLIS = 15_000L
        const val APPLICATION_READY_TIMEOUT_MILLIS = 30_000L
        const val VISITOR_READY_TIMEOUT_MILLIS = 30_000L
        const val RESTART_READY_TIMEOUT_MILLIS = 60_000L
        const val CONTROL_UNAVAILABLE_TIMEOUT_MILLIS = 15_000L
        const val SSE_DISCONNECT_TIMEOUT_MILLIS = 15_000L
        const val SSE_CHECKPOINT_TIMEOUT_MILLIS = 10_000L
        const val CONCURRENT_LARGE_REQUESTS = 4
        const val PORT_RELEASE_TIMEOUT_MILLIS = 10_000L
        const val CLIENT_CLEANUP_TIMEOUT_MILLIS = 10_000L
        const val RELAY_SETTLE_MILLIS = 500L
        const val SCENARIO_TIMEOUT_MILLIS = 75_000L
        const val NEGATIVE_SCENARIO_TIMEOUT_MILLIS = 45_000L
        const val RESTART_SCENARIO_TIMEOUT_MILLIS = 120_000L
        const val RESOURCE_CLEANUP_WAIT_TIMEOUT_MILLIS = 45_000L
    }
}
