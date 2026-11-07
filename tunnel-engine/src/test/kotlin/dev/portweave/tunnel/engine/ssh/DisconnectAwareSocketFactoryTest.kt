package dev.portweave.tunnel.engine.ssh

import dev.portweave.tunnel.engine.RuntimeWorkPolicy
import java.net.ConnectException
import java.net.InetAddress
import java.net.ServerSocket
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class DisconnectAwareSocketFactoryTest {
    @Test fun closingTheEstablishedSocketSignalsConnectionLost() {
        ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { server ->
            val factory = DisconnectAwareSocketFactory().apply { connectTimeoutMillis = 5_000 }
            val socket = factory.createSocket("127.0.0.1", server.localPort)
            server.accept().use { }
            assertFalse(factory.connectionLost.isCompleted)
            socket.close()
            assertTrue(factory.connectionLost.isCompleted)
        }
    }

    @Test fun failedConnectIsReportedByTheExceptionNotAsALoss() {
        val freePort = ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { it.localPort }
        val factory = DisconnectAwareSocketFactory().apply { connectTimeoutMillis = 5_000 }
        assertThrows(ConnectException::class.java) { factory.createSocket("127.0.0.1", freePort) }
        assertFalse(factory.connectionLost.isCompleted)
    }

    @Test fun abortClosesTheLiveSocket() {
        ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { server ->
            val factory = DisconnectAwareSocketFactory().apply { connectTimeoutMillis = 5_000 }
            val socket = factory.createSocket("127.0.0.1", server.localPort)
            factory.abort()
            assertTrue(socket.isClosed)
            assertTrue(factory.connectionLost.isCompleted)
        }
    }

    @Test fun handleReturnsAsSoonAsTheSessionSignalsLossAndOtherwiseChecksOncePerMinute() = runTest {
        val policy = RuntimeWorkPolicy()
        val session = SignallingSession()
        val handle = SshBackendHandle(session, 15432, policy)
        var returned = false
        val waiter = launch { handle.awaitConnectionLost(); returned = true }
        runCurrent()
        advanceTimeBy(59_000)
        runCurrent()
        assertEquals(1L, policy.counters.snapshot()["sshStateChecks"])
        advanceTimeBy(1_000)
        runCurrent()
        assertEquals(2L, policy.counters.snapshot()["sshStateChecks"])
        assertFalse(returned)
        session.isConnected = false
        session.lost.complete(Unit)
        runCurrent()
        assertTrue(returned)
        assertEquals(2L, policy.counters.snapshot()["sshStateChecks"])
        waiter.cancel()
    }

    @Test fun completedSignalEndsTheWaitEvenIfTheSessionStillLooksConnected() = runTest {
        val session = SignallingSession().apply { lost.complete(Unit) }
        // Must return instead of spinning on an already completed signal.
        SshBackendHandle(session, 15432, RuntimeWorkPolicy()).awaitConnectionLost()
    }
}

private class SignallingSession : SshSessionHandle {
    val lost = CompletableDeferred<Unit>()
    override var isConnected: Boolean = true
    override val connectionLost = lost
    override fun setHostKeyRepository(repository: com.jcraft.jsch.HostKeyRepository) = Unit
    override fun setConfig(key: String, value: String) = Unit
    override fun setServerAliveInterval(intervalMillis: Int) = Unit
    override fun addIdentity(name: String, privateKey: ByteArray, passphrase: ByteArray?) = Unit
    override fun setPassword(password: ByteArray) = Unit
    override fun connect(timeoutMillis: Int) = Unit
    override fun setPortForwardingL(bindAddress: String, localPort: Int, remoteHost: String, remotePort: Int) = localPort
    override fun disconnect() { isConnected = false }
}
