package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux

import kotlinx.coroutines.async
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class YamuxClientSessionContractTest {
    @Test
    fun clientLifecycleMatchesThePinnedK0Trace() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, fixtureConfig(YAMUX_FRP_WINDOW))
        val windowDelta = YAMUX_FRP_WINDOW - YAMUX_INITIAL_WINDOW

        val odd = session.openStream()
        assertEquals(1L, odd.streamId)
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L, windowDelta))
        runCurrent()
        odd.write("client-odd-data".encodeToByteArray())
        transport.feed(dataFrame(1L, "server-odd-data".encodeToByteArray()))
        assertArrayEquals("server-odd-data".encodeToByteArray(), readExactly(odd, 15))
        odd.closeWrite()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 1L))
        odd.awaitClosed()

        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L, windowDelta))
        val even = session.acceptStream()
        assertEquals(2L, even.streamId)
        transport.feed(dataFrame(2L, "server-even-data".encodeToByteArray()))
        assertArrayEquals("server-even-data".encodeToByteArray(), readExactly(even, 16))
        even.write("client-even-data".encodeToByteArray())
        even.closeWrite()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 2L))
        even.awaitClosed()

        session.sendGoAway()
        val ping = async { session.ping() }
        transport.awaitWrittenSize(127)
        transport.feed(frame(YamuxFrameType.PING, YamuxFlags.ACK, length = 0L))
        assertTrue(ping.await() >= 0L)
        transport.feed(frame(YamuxFrameType.GOAWAY))
        transport.feed(frame(YamuxFrameType.PING, YamuxFlags.SYN, length = 0L))
        transport.awaitWrittenSize(139)

        assertArrayEquals(loadYamuxFixture("client-to-server.trace.bin"), transport.writtenBytes())
        session.close()
    }

    @Test
    fun flowControlTraceUsesOneLegalHalfWindowDataFrame() = runTest {
        val transport = YamuxTestTransport()
        val oracleFrameSize = (YAMUX_INITIAL_WINDOW / 2L).toInt()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            fixtureConfig(
                window = YAMUX_INITIAL_WINDOW,
                outboundDataFrameSize = oracleFrameSize,
            ),
        )
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        runCurrent()
        val payload = ByteArray((YAMUX_INITIAL_WINDOW / 2L).toInt()) { index ->
            ('a'.code + index % 26).toByte()
        }

        stream.write(payload)
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, streamId = 1L, length = payload.size.toLong()))
        runCurrent()
        stream.closeWrite()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 1L))
        stream.awaitClosed()

        assertArrayEquals(
            loadYamuxFixture("flow-control/client-to-server.trace.bin"),
            transport.writtenBytes(),
        )
        session.close()
    }

    @Test
    fun consumingHalfTheReceiveWindowSendsThePinnedWindowUpdate() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, fixtureConfig(YAMUX_INITIAL_WINDOW))
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        val payload = ByteArray((YAMUX_INITIAL_WINDOW / 2L).toInt()) { index -> index.toByte() }
        transport.feed(dataFrame(1L, payload))

        assertArrayEquals(payload, readExactly(stream, payload.size))
        transport.awaitWrittenSize(2 * YamuxProtocol.HEADER_SIZE)

        val frames = parseYamuxTrace(transport.writtenBytes())
        assertEquals(2, frames.size)
        assertEquals(
            YamuxFrameHeader(YamuxFrameType.WINDOW_UPDATE, 0, 1L, payload.size.toLong()),
            frames[1].header,
        )
        assertEquals(0L, session.diagnostics().receiveBytes)
        session.close()
    }

    @Test
    fun resetScenarioMatchesThePinnedK0Trace() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            fixtureConfig(YAMUX_INITIAL_WINDOW, maxStreams = 2),
        )
        val surviving = session.openStream()
        val reset = session.openStream()
        assertEquals(1L, surviving.streamId)
        assertEquals(3L, reset.streamId)

        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.RST, 3L))
        val resetFailure = suspendFailureOf { reset.awaitClosed() }
        assertTrue(resetFailure is YamuxStreamResetFailure)
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        runCurrent()
        surviving.closeWrite()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 1L))
        surviving.awaitClosed()

        assertArrayEquals(loadYamuxFixture("reset/client-to-server.trace.bin"), transport.writtenBytes())
        assertEquals(0, session.diagnostics().activeStreams)
        assertEquals(0, session.diagnostics().activeStreamPermits)
        session.close()
    }

    private fun fixtureConfig(
        window: Long,
        maxStreams: Int = 4,
        outboundDataFrameSize: Int = 16 * 1024,
    ): YamuxConfig = YamuxConfig(
        receiveWindowSize = window,
        outboundDataFrameSize = outboundDataFrameSize,
        receiveSegmentSize = 4_096,
        maxBufferedReceiveBytes = 8L * 1024L * 1024L,
        maxStreams = maxStreams,
        maxPendingOpens = maxStreams,
        acceptBacklog = maxStreams,
        writerQueuedByteCapacity = maxOf(
            256L * 1024L,
            YamuxProtocol.HEADER_SIZE.toLong() + outboundDataFrameSize,
        ),
        keepAliveEnabled = false,
    )

    private fun frame(
        type: YamuxFrameType,
        flags: Int = 0,
        streamId: Long = 0L,
        length: Long = 0L,
    ): ByteArray = yamuxFrameBytes(type, flags, streamId, length)

    private fun dataFrame(streamId: Long, body: ByteArray): ByteArray = yamuxFrameBytes(
        type = YamuxFrameType.DATA,
        streamId = streamId,
        length = body.size.toLong(),
        body = body,
    )

    private suspend fun readExactly(stream: YamuxStream, length: Int): ByteArray {
        val result = ByteArray(length)
        var position = 0
        while (position < result.size) {
            val count = stream.read(result, position, result.size - position)
            check(count > 0)
            position += count
        }
        return result
    }

    private suspend fun suspendFailureOf(block: suspend () -> Unit): Throwable {
        try {
            block()
        } catch (failure: Throwable) {
            return failure
        }
        throw AssertionError("Expected failure")
    }
}
