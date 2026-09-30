package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime

import java.net.BindException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class SocketFrpLocalListenerFactoryTest {
    @Test
    fun activeListenerKeepsExclusivePortOwnership() = runBlocking {
        val address = InetAddress.getByAddress(byteArrayOf(127, 0, 0, 1))
        val factory = SocketFrpLocalListenerFactory()
        val first = factory.bind(address, 0)

        try {
            val failure = runCatching { factory.bind(address, first.bindPort) }.exceptionOrNull()
            assertTrue(failure is BindException)
        } finally {
            first.close()
        }
    }

    @Test
    fun rebindsAfterAcceptedConnectionCloses() = runBlocking {
        val address = InetAddress.getByAddress(byteArrayOf(127, 0, 0, 1))
        val factory = SocketFrpLocalListenerFactory()
        val first = factory.bind(address, 0)
        val port = first.bindPort

        try {
            withTimeout(TEST_TIMEOUT_MILLIS) {
                val accepted = async { first.accept() }
                Socket().use { client ->
                    client.connect(InetSocketAddress(address, port))
                    accepted.await().use { connection ->
                        connection.close()
                    }
                    client.soTimeout = SOCKET_TIMEOUT_MILLIS
                    assertEquals(-1, client.getInputStream().read())
                }
            }
        } finally {
            first.close()
        }

        factory.bind(address, port).close()
    }

    @Test
    fun acceptsOneHundredConcurrentClientsAndReleasesPort() = runBlocking {
        val address = InetAddress.getByAddress(byteArrayOf(127, 0, 0, 1))
        val factory = SocketFrpLocalListenerFactory()
        val listener = factory.bind(address, 0)
        val port = listener.bindPort

        try {
            withTimeout(TEST_TIMEOUT_MILLIS * 4) {
                val accepted = List(CLIENT_COUNT) { async { listener.accept() } }
                val clients = List(CLIENT_COUNT) {
                    Socket().apply { connect(InetSocketAddress(address, port)) }
                }
                accepted.awaitAll().forEach { it.close() }
                clients.forEach { client ->
                    client.use {
                        it.soTimeout = SOCKET_TIMEOUT_MILLIS
                        assertEquals(-1, it.getInputStream().read())
                    }
                }
            }
        } finally {
            listener.close()
        }

        factory.bind(address, port).close()
    }

    private companion object {
        const val TEST_TIMEOUT_MILLIS = 5_000L
        const val SOCKET_TIMEOUT_MILLIS = 2_000
        const val CLIENT_COUNT = 100
    }
}
