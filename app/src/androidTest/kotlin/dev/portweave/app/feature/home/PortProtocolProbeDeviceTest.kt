package dev.portweave.app.feature.home

import androidx.test.ext.junit.runners.AndroidJUnit4
import java.net.InetAddress
import java.net.ServerSocket
import kotlin.concurrent.thread
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class PortProtocolProbeDeviceTest {
    @Test
    fun detectsHttpOnTheDeviceLoopbackInterface() {
        ServerSocket(0, 50, InetAddress.getByName("127.0.0.1")).use { server ->
            val worker = thread(name = "device-http-probe-server") {
                server.accept().use { socket ->
                    val input = socket.getInputStream()
                    var matched = 0
                    val terminator = "\r\n\r\n".toByteArray()
                    while (matched < terminator.size) {
                        val next = input.read()
                        if (next == -1) break
                        matched = if (next.toByte() == terminator[matched]) matched + 1 else 0
                    }
                    socket.getOutputStream().apply {
                        write("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".toByteArray())
                        flush()
                    }
                }
            }

            assertEquals(
                PortProtocolProbeResult.Detected(PortProtocol.HTTP),
                runBlocking { SocketPortProtocolProbe().detect(server.localPort) },
            )
            worker.join(2_000)
        }
    }
}
