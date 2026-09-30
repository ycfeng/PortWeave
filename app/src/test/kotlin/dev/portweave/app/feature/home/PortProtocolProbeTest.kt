package dev.portweave.app.feature.home

import java.net.InetAddress
import java.net.ServerSocket
import kotlin.concurrent.thread
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test

class PortProtocolProbeTest {
    private val probe = SocketPortProtocolProbe(
        ioDispatcher = Dispatchers.IO,
        connectTimeoutMillis = 1_000,
        readTimeoutMillis = 1_000,
    )

    @Test
    fun cancellationClosesSocketWithoutWaitingForReadTimeout() = runBlocking {
        ServerSocket(0, 1, InetAddress.getByName("127.0.0.1")).use { server ->
            val work = async { SocketPortProtocolProbe(readTimeoutMillis = 60_000).detect(server.localPort) }
            val accepted = withContext(Dispatchers.IO) { server.accept() }
            accepted.use { peer ->
                withTimeout(2_000) { work.cancelAndJoin() }
                peer.soTimeout = 1_000
                withContext(Dispatchers.IO) {
                    while (peer.getInputStream().read() != -1) { }
                }
            }
        }
    }

    @Test
    fun detectsHttpFromTheResponseStatusLine() = withServer(
        response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
    ) { port ->
        assertEquals(
            PortProtocolProbeResult.Detected(PortProtocol.HTTP),
            runBlocking { probe.detect(port) },
        )
    }

    @Test
    fun ignoresNonHttpServices() = withServer(response = "SSH-2.0-test\r\n") { port ->
        assertEquals(PortProtocolProbeResult.NotHttp, runBlocking { probe.detect(port) })
    }

    private fun withServer(response: String, assertion: (Int) -> Unit) {
        ServerSocket(0, 50, InetAddress.getByName("127.0.0.1")).use { server ->
            val worker = thread(name = "protocol-probe-test-server") {
                server.accept().use { socket ->
                    socket.soTimeout = 1_000
                    val request = socket.getInputStream()
                    var matched = 0
                    val terminator = "\r\n\r\n".toByteArray()
                    while (matched < terminator.size) {
                        val next = request.read()
                        if (next == -1) break
                        matched = if (next.toByte() == terminator[matched]) matched + 1 else 0
                    }
                    socket.getOutputStream().apply {
                        write(response.toByteArray())
                        flush()
                    }
                }
            }
            assertion(server.localPort)
            worker.join(2_000)
        }
    }
}
