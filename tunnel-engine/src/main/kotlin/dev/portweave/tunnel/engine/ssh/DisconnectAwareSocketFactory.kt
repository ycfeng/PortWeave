package dev.portweave.tunnel.engine.ssh

import com.jcraft.jsch.SocketFactory
import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred

/**
 * Lets a JSch session report its own end instead of being polled. In JSch 2.28.3 every way a session ends
 * (EOF or I/O error in the read loop, keepalive exhaustion, explicit `disconnect()`) runs through
 * `Session.disconnect()`, which clears `isConnected` and then closes the socket; so the socket's close is the
 * single, reliable "connection lost" signal. Read timeouts are how JSch schedules keepalives and are not losses.
 *
 * JSch skips its own connect timeout once a factory is installed, so [connectTimeoutMillis] must be set before
 * `Session.connect`.
 */
internal class DisconnectAwareSocketFactory : SocketFactory {
    private val lost = CompletableDeferred<Unit>()
    @Volatile private var current: Socket? = null
    @Volatile var connectTimeoutMillis: Int = 0

    /** Completes once the established connection's socket has been closed, for whatever reason. */
    val connectionLost: Deferred<Unit> get() = lost

    override fun createSocket(host: String, port: Int): Socket {
        val socket = SignallingSocket(lost)
        current = socket
        try {
            socket.connect(InetSocketAddress(host, port), connectTimeoutMillis)
        } catch (failure: Exception) {
            socket.closeSilently()
            throw failure
        }
        socket.armed = true
        return socket
    }

    override fun getInputStream(socket: Socket): InputStream = socket.getInputStream()

    override fun getOutputStream(socket: Socket): OutputStream = socket.getOutputStream()

    /**
     * Closes the socket even while it is still connecting. `Session.disconnect()` is a no-op until the TCP
     * connection exists, so without this a cancelled start would block until the connect timeout.
     */
    fun abort() {
        current?.let { runCatching { it.close() } }
    }

    private class SignallingSocket(private val lost: CompletableDeferred<Unit>) : Socket() {
        /** Only an established connection can be lost; a failed connect is reported by the exception. */
        @Volatile var armed = false

        override fun close() {
            try {
                super.close()
            } finally {
                if (armed) lost.complete(Unit)
            }
        }

        fun closeSilently() {
            runCatching { super.close() }
        }
    }
}
