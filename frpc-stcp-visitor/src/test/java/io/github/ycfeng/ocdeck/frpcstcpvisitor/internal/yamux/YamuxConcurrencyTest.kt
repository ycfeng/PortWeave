package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux

import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class YamuxConcurrencyTest {
    @Test
    fun concurrentWritesRemainCompletePhysicalFrames() = runTest {
        val transport = YamuxTestTransport(yieldBetweenWrittenBytes = true)
        var fatalFailure: Throwable? = null
        val writer = YamuxFrameWriter(
            transport = transport,
            config = testConfig(),
            scope = backgroundScope,
            onFatalFailure = { fatalFailure = it },
        )

        val payloads = (1L..32L).associateWith { streamId ->
            ByteArray(31 + streamId.toInt()) { index -> (streamId.toInt() + index).toByte() }
        }
        payloads.map { (streamId, body) ->
            async {
                writer.write(
                    YamuxFrameHeader(YamuxFrameType.DATA, 0, streamId, body.size.toLong()),
                    body,
                )
            }
        }.awaitAll()

        val frames = parseYamuxTrace(transport.writtenBytes())
        assertEquals(32, frames.size)
        assertEquals(payloads.keys, frames.map { it.header.streamId }.toSet())
        frames.forEach { frame ->
            assertEquals(YamuxFrameType.DATA, frame.header.type)
            assertArrayEquals(payloads.getValue(frame.header.streamId), frame.body)
        }
        assertNull(fatalFailure)
        writer.shutdown(YamuxSessionClosedFailure())
    }

    @Test
    fun dataFrameUsesOnePhysicalWriteAndOwnsEncodedBytesDuringTransportWrite() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate)
        val writer = YamuxFrameWriter(
            transport = transport,
            config = testConfig(),
            scope = backgroundScope,
            onFatalFailure = { throw it },
        )
        val body = byteArrayOf(1, 2, 3, 4)
        val writing = async {
            writer.write(
                YamuxFrameHeader(YamuxFrameType.DATA, 0, 1L, body.size.toLong()),
                body,
            )
        }
        transport.awaitWriteStarts(1)

        body.fill(9)
        gate.complete(Unit)
        writing.await()

        assertEquals(1, transport.writeStartCount)
        assertArrayEquals(byteArrayOf(1, 2, 3, 4), parseYamuxTrace(transport.writtenBytes()).single().body)
        writer.shutdownAndJoin(YamuxSessionClosedFailure())
    }

    @Test
    fun cancelledPhysicalDataWriteRetainsBudgetUntilTheSingleWriteCompletes() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate)
        val body = ByteArray(16 * 1024) { it.toByte() }
        val wireBytes = YamuxProtocol.HEADER_SIZE.toLong() + body.size
        val writer = YamuxFrameWriter(
            transport = transport,
            config = testConfig(writerQueuedByteCapacity = wireBytes),
            scope = backgroundScope,
            onFatalFailure = { throw it },
        )
        val writing = backgroundScope.async {
            writer.write(
                YamuxFrameHeader(YamuxFrameType.DATA, 0, 1L, body.size.toLong()),
                body,
            )
        }
        transport.awaitWriteStarts(1)

        writing.cancel(CancellationException("physical-data-cancelled"))
        runCurrent()
        assertEquals(wireBytes, writer.pendingByteCount)
        assertEquals(1, transport.activeWriteCount)

        gate.complete(Unit)
        assertTrue(suspendFailureOf { writing.await() } is CancellationException)
        assertEquals(0L, writer.pendingByteCount)
        assertEquals(0, transport.activeWriteCount)
        assertEquals(1, transport.writeStartCount)
        assertArrayEquals(body, parseYamuxTrace(transport.writtenBytes()).single().body)
        writer.shutdownAndJoin(YamuxSessionClosedFailure())
    }

    @Test
    fun writerQueueWaiterCanBeCancelledWithoutStoppingTheWriter() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate)
        var fatalFailure: Throwable? = null
        val writer = YamuxFrameWriter(
            transport = transport,
            config = testConfig(writerMailboxCapacity = 1),
            scope = backgroundScope,
            onFatalFailure = { fatalFailure = it },
        )
        val body = byteArrayOf(1)
        val first = async {
            writer.write(YamuxFrameHeader(YamuxFrameType.DATA, 0, 1L, 1L), body)
        }
        transport.awaitWriteStarts(1)
        val second = async {
            writer.write(YamuxFrameHeader(YamuxFrameType.DATA, 0, 3L, 1L), body)
        }
        runCurrent()
        val third = async {
            writer.write(YamuxFrameHeader(YamuxFrameType.DATA, 0, 5L, 1L), body)
        }
        runCurrent()
        assertEquals(3, writer.pendingFrameCount)

        val cancellation = CancellationException("writer-wait-cancelled")
        third.cancel(cancellation)
        val cancellationFailure = suspendFailureOf { third.await() }
        assertTrue(cancellationFailure is CancellationException)
        assertEquals(cancellation.message, cancellationFailure.message)
        assertEquals(2, writer.pendingFrameCount)

        gate.complete(Unit)
        first.await()
        second.await()
        runCurrent()
        assertEquals(0, writer.pendingFrameCount)
        assertNull(fatalFailure)
        assertEquals(listOf(1L, 3L), parseYamuxTrace(transport.writtenBytes()).map { it.header.streamId })
        writer.shutdown(YamuxSessionClosedFailure())
    }

    @Test
    fun productionDataWritesUseAtMostSixteenKiBFrames() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        runCurrent()
        val payload = ByteArray(40 * 1024) { index -> index.toByte() }

        stream.write(payload)

        val frames = parseYamuxTrace(transport.writtenBytes())
        assertEquals(
            listOf(16L * 1024L, 16L * 1024L, 8L * 1024L),
            frames.filter { it.header.type == YamuxFrameType.DATA }.map { it.header.length },
        )
        session.close()
    }

    @Test
    fun writerQueuedByteBudgetBlocksCopiesBeyondTheConfiguredLimit() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate)
        var fatalFailure: Throwable? = null
        val body = ByteArray(16 * 1024) { index -> index.toByte() }
        val wireBytes = YamuxProtocol.HEADER_SIZE.toLong() + body.size
        val writer = YamuxFrameWriter(
            transport = transport,
            config = testConfig(writerMailboxCapacity = 8, writerQueuedByteCapacity = wireBytes),
            scope = backgroundScope,
            onFatalFailure = { fatalFailure = it },
        )
        val header = YamuxFrameHeader(YamuxFrameType.DATA, 0, 1L, body.size.toLong())
        val first = async { writer.write(header, body) }
        transport.awaitWriteStarts(1)
        val second = async { writer.write(header.copy(streamId = 3L), body) }
        runCurrent()

        assertEquals(1, writer.pendingFrameCount)
        assertEquals(wireBytes, writer.pendingByteCount)
        second.cancel(CancellationException("queued-byte-budget-cancelled"))
        assertTrue(suspendFailureOf { second.await() } is CancellationException)
        assertEquals(wireBytes, writer.pendingByteCount)

        gate.complete(Unit)
        first.await()
        runCurrent()
        assertEquals(0L, writer.pendingByteCount)
        assertEquals(listOf(1L), parseYamuxTrace(transport.writtenBytes()).map { it.header.streamId })
        assertNull(fatalFailure)
        writer.shutdownAndJoin(YamuxSessionClosedFailure())
    }

    @Test
    fun sustainedControlTrafficCannotStarveQueuedData() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate)
        var fatalFailure: Throwable? = null
        val writer = YamuxFrameWriter(
            transport = transport,
            config = testConfig(maxStreams = 16, writerMailboxCapacity = 4),
            scope = backgroundScope,
            onFatalFailure = { fatalFailure = it },
        )
        val firstControl = async { writer.write(pingHeader(0L)) }
        transport.awaitWriteStarts(1)
        val data = async {
            writer.write(
                header = YamuxFrameHeader(YamuxFrameType.DATA, 0, 99L, 1L),
                body = byteArrayOf(7),
            )
        }
        val controlProducer = backgroundScope.launch {
            repeat(64) { index -> writer.enqueueControl(pingHeader(index.toLong() + 1L)) }
        }
        runCurrent()

        gate.complete(Unit)
        firstControl.await()
        data.await()
        controlProducer.cancel()
        controlProducer.join()

        val frames = parseYamuxTrace(transport.writtenBytes())
        val dataIndex = frames.indexOfFirst { it.header.type == YamuxFrameType.DATA }
        assertTrue(dataIndex in 1..YamuxFrameWriter.MAX_CONTROL_BURST)
        assertNull(fatalFailure)
        writer.shutdownAndJoin(YamuxSessionClosedFailure())
    }

    @Test
    fun writerByteBudgetWaitersAcquirePermitsInFifoOrder() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate)
        val body = ByteArray(16 * 1024) { index -> index.toByte() }
        val wireBytes = YamuxProtocol.HEADER_SIZE.toLong() + body.size
        val writer = YamuxFrameWriter(
            transport = transport,
            config = testConfig(
                maxStreams = 2,
                writerMailboxCapacity = 2,
                writerQueuedByteCapacity = wireBytes,
            ),
            scope = backgroundScope,
            onFatalFailure = { throw it },
        )
        val active = async {
            writer.write(YamuxFrameHeader(YamuxFrameType.DATA, 0, 1L, body.size.toLong()), body)
        }
        transport.awaitWriteStarts(1)
        val oldest = async {
            writer.write(YamuxFrameHeader(YamuxFrameType.DATA, 0, 3L, body.size.toLong()), body)
        }
        runCurrent()
        val youngest = async {
            writer.write(YamuxFrameHeader(YamuxFrameType.DATA, 0, 5L, 1L), byteArrayOf(5))
        }
        runCurrent()

        gate.complete(Unit)
        active.await()
        oldest.await()
        youngest.await()

        assertEquals(
            listOf(1L, 3L, 5L),
            parseYamuxTrace(transport.writtenBytes()).map { it.header.streamId },
        )
        assertEquals(0L, writer.pendingByteCount)
        writer.shutdownAndJoin(YamuxSessionClosedFailure())
    }

    @Test
    fun abortBeforeMailboxVisibilityRollsBackWithoutPhysicalWrite() = runTest {
        val beforeSendEntered = CompletableDeferred<Unit>()
        val allowSend = CompletableDeferred<Unit>()
        val abortSignal = CompletableDeferred<Unit>()
        val expected = YamuxStreamResetFailure()
        val rollbackCount = AtomicInteger(0)
        val transport = YamuxTestTransport()
        val writer = YamuxFrameWriter(
            transport = transport,
            config = testConfig(maxStreams = 1),
            scope = backgroundScope,
            onFatalFailure = { throw it },
            beforeMailboxSend = {
                beforeSendEntered.complete(Unit)
                allowSend.await()
            },
        )
        val result = backgroundScope.async {
            suspendFailureOf {
                writer.write(
                    header = YamuxFrameHeader(YamuxFrameType.DATA, 0, 1L, 1L),
                    body = byteArrayOf(1),
                    abort = YamuxWriteAbort(abortSignal) { expected },
                    onNotStarted = { rollbackCount.incrementAndGet() },
                )
            }
        }
        beforeSendEntered.await()

        abortSignal.complete(Unit)
        runCurrent()
        allowSend.complete(Unit)

        assertSame(expected, result.await())
        assertEquals(1, rollbackCount.get())
        assertEquals(0, transport.writeStartCount)
        assertEquals(0, writer.pendingFrameCount)
        assertEquals(0L, writer.pendingByteCount)
        writer.shutdownAndJoin(YamuxSessionClosedFailure())
    }

    @Test
    fun physicalWriterTimeoutStopsTheWriter() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate)
        var fatalFailure: Throwable? = null
        val writer = YamuxFrameWriter(
            transport = transport,
            config = testConfig(writeTimeoutMillis = 1_000L),
            scope = backgroundScope,
            onFatalFailure = { fatalFailure = it },
        )
        val writeResult = CompletableDeferred<Throwable>()
        backgroundScope.launch {
            writeResult.complete(suspendFailureOf { writer.write(pingHeader(1L)) })
        }
        transport.awaitWriteStarts(1)

        advanceTimeBy(1_000L)
        runCurrent()

        val failure = writeResult.await()
        assertTrue(failure is YamuxTimeoutFailure)
        assertEquals(YamuxTimeoutKind.WRITE, (failure as YamuxTimeoutFailure).kind)
        assertTrue(fatalFailure is YamuxTimeoutFailure)
        assertEquals(0, writer.pendingFrameCount)
        assertEquals(0L, writer.pendingByteCount)
        assertEquals(0, transport.activeWriteCount)
        writer.shutdownAndJoin(YamuxSessionClosedFailure())
    }

    @Test
    fun writesResumeOnlyAfterAWindowUpdateBeyondTheInitialWindow() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        runCurrent()
        val remainder = 32 * 1024
        val payload = ByteArray(YAMUX_INITIAL_WINDOW.toInt() + remainder) { index -> index.toByte() }

        val sending = async { stream.write(payload) }
        transport.awaitWrittenSize(
            YamuxProtocol.HEADER_SIZE +
                (YAMUX_INITIAL_WINDOW / (16 * 1024L)).toInt() * YamuxProtocol.HEADER_SIZE +
                YAMUX_INITIAL_WINDOW.toInt(),
        )
        runCurrent()
        assertFalse(sending.isCompleted)

        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, streamId = 1L, length = remainder.toLong()))
        sending.await()
        val dataLengths = parseYamuxTrace(transport.writtenBytes())
            .filter { it.header.type == YamuxFrameType.DATA }
            .map { it.header.length }
        assertEquals(List(18) { 16L * 1024L }, dataLengths)
        session.close()
    }

    @Test
    fun sessionCloseWakesPendingReaderAndWindowBlockedWriterWithTheSameFailure() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        runCurrent()
        stream.write(ByteArray(YAMUX_INITIAL_WINDOW.toInt()))
        supervisorScope {
            val pendingRead = async { stream.read(ByteArray(1)) }
            val pendingWrite = async { stream.write(byteArrayOf(1)) }
            runCurrent()
            assertFalse(pendingRead.isCompleted)
            assertFalse(pendingWrite.isCompleted)

            session.close()

            val readFailure = suspendFailureOf { pendingRead.await() }
            val writeFailure = suspendFailureOf { pendingWrite.await() }
            assertTrue(readFailure is YamuxSessionClosedFailure)
            assertTrue(writeFailure is YamuxSessionClosedFailure)
            assertEquals(readFailure.message, writeFailure.message)
        }
        assertEquals(0, session.diagnostics().activeStreams)
        assertEquals(0, session.diagnostics().activeStreamPermits)
    }

    @Test
    fun closeHardStopsQueuedDataWithoutSendingGoAway() = runTest {
        val streamCount = 8
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate, writeGateAfterStarts = streamCount)
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(
                maxStreams = streamCount,
                writerMailboxCapacity = streamCount,
                writeTimeoutMillis = 1_000L,
            ),
        )
        val streams = List(streamCount) { session.openStream() }
        streams.forEach { stream ->
            transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, stream.streamId))
        }
        runCurrent()
        val sends = streams.mapIndexed { index, stream ->
            backgroundScope.async {
                suspendFailureOf { stream.write(byteArrayOf(index.toByte())) }
            }
        }
        transport.awaitWriteStarts(streamCount + 1)
        runCurrent()
        assertEquals(streamCount, session.diagnostics().writerPendingFrames)

        val closing = backgroundScope.async { session.close() }
        runCurrent()
        closing.await()
        val sendFailures = sends.awaitAll()

        val framesAtClose = parseYamuxTrace(transport.writtenBytes())
        assertTrue(sendFailures.all { it is YamuxSessionClosedFailure })
        assertEquals(streamCount, framesAtClose.size)
        assertTrue(framesAtClose.none { it.header.type == YamuxFrameType.DATA })
        assertTrue(framesAtClose.none { it.header.type == YamuxFrameType.GOAWAY })
        assertEquals(0, session.diagnostics().writerPendingFrames)
        assertEquals(0L, session.diagnostics().writerPendingBytes)
        assertEquals(0, transport.activeWriteCount)
        assertTrue(transport.isClosed)
    }

    @Test
    fun cancelledQueuedFinDoesNotCommitTheLocalHalfClose() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate, writeGateAfterStarts = 1)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        val ping = async { session.ping() }
        transport.awaitWriteStarts(2)
        val closing = async { stream.closeWrite() }
        runCurrent()
        assertEquals(2, session.diagnostics().writerPendingFrames)

        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 1L))
        runCurrent()
        assertFalse(stream.isClosed())

        closing.cancel(CancellationException("queued-fin-cancelled"))
        assertTrue(suspendFailureOf { closing.await() } is CancellationException)
        gate.complete(Unit)
        transport.awaitWrittenSize(2 * YamuxProtocol.HEADER_SIZE)
        transport.feed(frame(YamuxFrameType.PING, YamuxFlags.ACK, length = 0L))
        ping.await()

        val beforeCommittedClose = parseYamuxTrace(transport.writtenBytes())
        assertTrue(beforeCommittedClose.none { it.header.flags and YamuxFlags.FIN != 0 })

        stream.closeWrite()
        transport.awaitWrittenSize(3 * YamuxProtocol.HEADER_SIZE)
        stream.awaitClosed()
        val afterCommittedClose = parseYamuxTrace(transport.writtenBytes())
        assertEquals(1, afterCommittedClose.count { it.header.flags and YamuxFlags.FIN != 0 })
        session.close()
    }

    @Test
    fun cancellationAfterPhysicalFinStartsCommitsOnceWithoutDuplicateFin() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate, writeGateAfterStarts = 1)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 1L))
        runCurrent()
        val closing = backgroundScope.async { stream.closeWrite() }
        transport.awaitWriteStarts(2)

        closing.cancel(CancellationException("physical-fin-cancelled"))
        gate.complete(Unit)
        assertTrue(suspendFailureOf { closing.await() } is CancellationException)
        stream.awaitClosed()

        stream.closeWrite()
        val frames = parseYamuxTrace(transport.writtenBytes())
        assertEquals(1, frames.count { it.header.flags and YamuxFlags.FIN != 0 })
        session.close()
    }

    @Test
    fun receiveWindowViolationTerminatesAndReleasesBufferedBytes() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        transport.feed(dataFrame(1L, ByteArray(200 * 1024)))
        transport.feed(
            frame(
                YamuxFrameType.DATA,
                streamId = 1L,
                length = (100 * 1024).toLong(),
            ),
        )

        val failure = suspendFailureOf { session.awaitTermination() }

        assertTrue(failure is YamuxProtocolFailure)
        assertEquals(YamuxProtocolError.RECEIVE_WINDOW_EXCEEDED, (failure as YamuxProtocolFailure).reason)
        assertSame(failure, suspendFailureOf { stream.awaitClosed() })
        val goAway = parseYamuxTrace(transport.writtenBytes()).single {
            it.header.type == YamuxFrameType.GOAWAY
        }
        assertEquals(YamuxGoAwayCode.PROTOCOL_ERROR.wireValue, goAway.header.length)
        assertEquals(0L, session.diagnostics().receiveBytes)
        assertTrue(transport.isClosed)
    }

    @Test
    fun remoteStreamHasOnlyTheProtocolInitialReceiveCreditBeforeAck() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(receiveWindowSize = YAMUX_FRP_WINDOW, writeTimeoutMillis = 1_000L),
        )
        val initialPayload = ByteArray(YAMUX_INITIAL_WINDOW.toInt()) { index -> index.toByte() }
        transport.feed(
            frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L) +
                dataFrame(2L, initialPayload),
        )
        runCurrent()

        assertFalse(session.diagnostics().ending)
        assertEquals(YAMUX_INITIAL_WINDOW, session.diagnostics().receiveBytes)

        transport.feed(frame(YamuxFrameType.DATA, streamId = 2L, length = 1L))
        val failure = suspendFailureOf { session.awaitTermination() }

        assertTrue(failure is YamuxProtocolFailure)
        assertEquals(YamuxProtocolError.RECEIVE_WINDOW_EXCEEDED, (failure as YamuxProtocolFailure).reason)
        assertEquals(0L, session.diagnostics().receiveBytes)
    }

    @Test
    fun remoteAckCommitsReceiveCreditBeforeTheFirstHeaderWrite() = runTest {
        val payload = ByteArray(YAMUX_INITIAL_WINDOW.toInt() + 1) { index -> index.toByte() }
        val injected = AtomicBoolean(false)
        lateinit var transport: YamuxTestTransport
        transport = YamuxTestTransport(
            beforeWrite = { writeStart, _, _, _ ->
                if (writeStart == 1 && injected.compareAndSet(false, true)) {
                    transport.feed(dataFrame(2L, payload))
                }
            },
        )
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(receiveWindowSize = YAMUX_FRP_WINDOW),
        )
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L))
        val stream = session.acceptStream()
        runCurrent()
        val destination = ByteArray(payload.size)

        assertEquals(payload.size, stream.read(destination))
        assertArrayEquals(payload, destination)
        assertFalse(session.diagnostics().ending)
        assertEquals(
            YamuxFrameHeader(
                YamuxFrameType.WINDOW_UPDATE,
                YamuxFlags.ACK,
                2L,
                YAMUX_FRP_WINDOW - YAMUX_INITIAL_WINDOW,
            ),
            parseYamuxTrace(transport.writtenBytes()).single().header,
        )
        session.close()
    }

    @Test
    fun windowUpdateCommitsReceiveCreditBeforeTheFirstHeaderWrite() = runTest {
        val threshold = (YAMUX_INITIAL_WINDOW / 2L).toInt()
        val firstPayload = ByteArray(threshold) { index -> index.toByte() }
        val immediatePayload = ByteArray(threshold + 1) { index -> (index + 1).toByte() }
        val injected = AtomicBoolean(false)
        lateinit var transport: YamuxTestTransport
        transport = YamuxTestTransport(
            beforeWrite = { _, source, offset, length ->
                if (length == YamuxProtocol.HEADER_SIZE) {
                    val header = YamuxFrameCodec.decodeHeader(source, offset)
                    if (
                        header.type == YamuxFrameType.WINDOW_UPDATE &&
                        header.flags == 0 &&
                        injected.compareAndSet(false, true)
                    ) {
                        transport.feed(dataFrame(1L, immediatePayload))
                    }
                }
            },
        )
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        transport.feed(dataFrame(1L, firstPayload))
        runCurrent()

        val firstDestination = ByteArray(firstPayload.size)
        assertEquals(firstPayload.size, stream.read(firstDestination))
        assertArrayEquals(firstPayload, firstDestination)
        transport.awaitWriteStarts(2)
        runCurrent()

        val immediateDestination = ByteArray(immediatePayload.size)
        assertEquals(immediatePayload.size, stream.read(immediateDestination))
        assertArrayEquals(immediatePayload, immediateDestination)
        assertFalse(session.diagnostics().ending)
        session.close()
    }

    @Test
    fun dataResetOverCurrentCreditFailsClosedBeforeResetOrDrain() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(receiveWindowSize = YAMUX_FRP_WINDOW),
        )
        transport.feed(
            frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L) +
                frame(
                    YamuxFrameType.DATA,
                    YamuxFlags.RST,
                    streamId = 2L,
                    length = YAMUX_INITIAL_WINDOW + 1L,
                ),
        )

        val failure = suspendFailureOf { session.awaitTermination() }

        assertTrue(failure is YamuxProtocolFailure)
        assertEquals(YamuxProtocolError.RECEIVE_WINDOW_EXCEEDED, (failure as YamuxProtocolFailure).reason)
        assertEquals(0L, session.diagnostics().receiveBytes)
    }

    @Test
    fun reservedSendCreditParticipatesInWindowUpdateOverflowAndRollbackIsSafe() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate, writeGateAfterStarts = 1)
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(writeTimeoutMillis = 1_000L),
        )
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        runCurrent()
        val ping = backgroundScope.async { suspendFailureOf { session.ping() } }
        transport.awaitWriteStarts(2)
        val sending = backgroundScope.async {
            suspendFailureOf { stream.write(ByteArray(16 * 1024)) }
        }
        runCurrent()
        assertEquals(16L * 1024L, stream.sendCreditSnapshot().reserved)

        val overflowingDelta = YamuxProtocol.MAX_UINT32 - YAMUX_INITIAL_WINDOW + 1L
        transport.feed(
            frame(
                YamuxFrameType.WINDOW_UPDATE,
                streamId = 1L,
                length = overflowingDelta,
            ),
        )
        runCurrent()
        advanceTimeBy(1_000L)
        runCurrent()
        val failure = suspendFailureOf { session.awaitTermination() }

        assertTrue(failure is YamuxProtocolFailure)
        assertEquals(YamuxProtocolError.SEND_WINDOW_OVERFLOW, (failure as YamuxProtocolFailure).reason)
        assertTrue(sending.await() is YamuxFailure)
        assertTrue(ping.await() is YamuxFailure)
        assertEquals(0, session.diagnostics().activeChildJobs)
        assertEquals(0L, session.diagnostics().writerPendingBytes)
        assertEquals(0, transport.activeWriteCount)
    }

    @Test
    fun endingAfterSendReservationButBeforeEnqueueRollsBackExactlyOnce() = runTest {
        val gate = CompletableDeferred<Unit>()
        val frameBytes = YamuxProtocol.HEADER_SIZE.toLong() + 16L * 1024L
        val transport = YamuxTestTransport(writeGate = gate, writeGateAfterStarts = 2)
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(
                maxStreams = 2,
                writerMailboxCapacity = 2,
                writerQueuedByteCapacity = frameBytes,
                writeTimeoutMillis = 1_000L,
            ),
        )
        val first = session.openStream()
        val second = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, first.streamId))
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, second.streamId))
        runCurrent()

        val firstWrite = backgroundScope.async {
            suspendFailureOf { first.write(ByteArray(16 * 1024)) }
        }
        transport.awaitWriteStarts(3)
        val secondWrite = backgroundScope.async {
            suspendFailureOf { second.write(ByteArray(16 * 1024)) }
        }
        runCurrent()
        assertEquals(
            YamuxSendCreditSnapshot(
                available = YAMUX_INITIAL_WINDOW - 16L * 1024L,
                reserved = 16L * 1024L,
            ),
            second.sendCreditSnapshot(),
        )

        val closing = backgroundScope.async { session.close() }
        runCurrent()
        assertEquals(
            YamuxSendCreditSnapshot(available = YAMUX_INITIAL_WINDOW, reserved = 0L),
            second.sendCreditSnapshot(),
        )
        advanceTimeBy(1_000L)
        runCurrent()
        closing.await()
        assertTrue(firstWrite.await() is YamuxFailure)
        assertTrue(secondWrite.await() is YamuxFailure)
        assertEquals(
            YamuxSendCreditSnapshot(available = YAMUX_INITIAL_WINDOW, reserved = 0L),
            second.sendCreditSnapshot(),
        )
        assertEquals(0L, session.diagnostics().writerPendingBytes)
    }

    @Test
    fun consumedReadReturnsBeforeItsWindowUpdateFailureTerminatesTheSession() = runTest {
        val gate = CompletableDeferred<Unit>()
        val expected = YamuxTransportFailure(YamuxTransportOperation.WRITE)
        val transport = YamuxTestTransport(
            writeGate = gate,
            writeGateAfterStarts = 1,
            writeFailure = expected,
            writeFailureOnStart = 2,
        )
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        val payload = ByteArray((YAMUX_INITIAL_WINDOW / 2L).toInt()) { index -> index.toByte() }
        transport.feed(dataFrame(1L, payload))
        runCurrent()
        val destination = ByteArray(payload.size)

        assertEquals(payload.size, stream.read(destination))
        assertArrayEquals(payload, destination)
        assertEquals(0L, session.diagnostics().receiveBytes)
        transport.awaitWriteStarts(2)
        gate.complete(Unit)

        assertSame(expected, suspendFailureOf { session.awaitTermination() })
        assertSame(expected, suspendFailureOf { stream.read(ByteArray(1)) })
        assertEquals(0, session.diagnostics().activeChildJobs)
    }

    @Test
    fun thirtyTwoStreamsMergeWindowUpdatesWithoutCompetingWithTheFullDataMailbox() = runTest {
        val streamCount = 32
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate, writeGateAfterStarts = streamCount)
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(
                maxStreams = streamCount,
                writerMailboxCapacity = streamCount,
                writerQueuedByteCapacity = 1024L * 1024L,
            ),
        )
        val streams = List(streamCount) { session.openStream() }
        val inboundPayload = ByteArray((YAMUX_INITIAL_WINDOW / 2L).toInt()) { index -> index.toByte() }
        streams.forEach { stream ->
            transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, stream.streamId))
            transport.feed(dataFrame(stream.streamId, inboundPayload))
        }
        runCurrent()

        val sends = streams.mapIndexed { index, stream ->
            backgroundScope.async { stream.write(byteArrayOf(index.toByte())) }
        }
        transport.awaitWriteStarts(streamCount + 1)
        val destination = ByteArray(inboundPayload.size)
        streams.forEach { stream ->
            assertEquals(inboundPayload.size, stream.read(destination))
            assertArrayEquals(inboundPayload, destination)
        }
        runCurrent()

        assertFalse(session.diagnostics().ending)
        assertTrue(session.diagnostics().writerPendingFrames >= streamCount + 1)
        gate.complete(Unit)
        sends.awaitAll()
        val expectedBytes =
            streamCount * YamuxProtocol.HEADER_SIZE +
                streamCount * (YamuxProtocol.HEADER_SIZE + 1) +
                streamCount * YamuxProtocol.HEADER_SIZE
        transport.awaitWrittenSize(expectedBytes)

        val frames = parseYamuxTrace(transport.writtenBytes())
        assertEquals(streamCount, frames.count { it.header.type == YamuxFrameType.DATA })
        assertEquals(
            streamCount,
            frames.count { it.header.type == YamuxFrameType.WINDOW_UPDATE && it.header.flags == 0 },
        )
        assertFalse(session.diagnostics().ending)
        session.close()
        assertEquals(0, session.diagnostics().writerPendingFrames)
        assertEquals(0L, session.diagnostics().writerPendingBytes)
    }

    @Test
    fun gracefullyClosedStreamKeepsItsPermitUntilBufferedDataIsRead() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig(maxStreams = 1))
        val stream = session.openStream()
        val body = byteArrayOf(1, 2, 3, 4)
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        transport.feed(
            yamuxFrameBytes(
                type = YamuxFrameType.DATA,
                flags = YamuxFlags.FIN,
                streamId = 1L,
                length = body.size.toLong(),
                body = body,
            ),
        )
        stream.closeWrite()
        stream.awaitClosed()

        assertEquals(1, session.diagnostics().activeStreams)
        assertEquals(1, session.diagnostics().activeStreamPermits)
        assertEquals(body.size.toLong(), session.diagnostics().receiveBytes)
        val destination = ByteArray(body.size)
        assertEquals(body.size, stream.read(destination))
        assertArrayEquals(body, destination)
        assertEquals(-1, stream.read(ByteArray(1)))
        assertEquals(0, session.diagnostics().activeStreams)
        assertEquals(0, session.diagnostics().activeStreamPermits)
        assertEquals(0L, session.diagnostics().receiveBytes)
        session.close()
    }

    @Test
    fun sessionCloseDetachesButPreservesUnreadDataFromAGracefullyClosedStream() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig(maxStreams = 1))
        val stream = session.openStream()
        val body = byteArrayOf(1, 2, 3, 4)
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        transport.feed(
            yamuxFrameBytes(
                type = YamuxFrameType.DATA,
                flags = YamuxFlags.FIN,
                streamId = 1L,
                length = body.size.toLong(),
                body = body,
            ),
        )
        stream.closeWrite()
        stream.awaitClosed()

        session.close()

        assertEquals(0, session.diagnostics().activeStreams)
        assertEquals(0, session.diagnostics().activeStreamPermits)
        assertEquals(0L, session.diagnostics().receiveBytes)
        val destination = ByteArray(body.size)
        assertEquals(body.size, stream.read(destination))
        assertArrayEquals(body, destination)
        assertEquals(-1, stream.read(ByteArray(1)))
    }

    @Test
    fun gracefullyClosedBufferedStreamSurvivesALaterSessionFailure() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig(maxStreams = 1))
        val stream = session.openStream()
        val body = byteArrayOf(5, 6, 7, 8)
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        transport.feed(
            yamuxFrameBytes(
                type = YamuxFrameType.DATA,
                flags = YamuxFlags.FIN,
                streamId = 1L,
                length = body.size.toLong(),
                body = body,
            ),
        )
        stream.closeWrite()
        stream.awaitClosed()
        assertEquals(1, stream.bufferedBlockCount())
        val expected = YamuxTransportFailure(YamuxTransportOperation.READ)

        transport.failInput(expected)
        assertSame(expected, suspendFailureOf { session.awaitTermination() })

        assertEquals(0L, session.diagnostics().receiveBytes)
        assertEquals(0, session.diagnostics().activeStreamPermits)
        assertEquals(1, stream.bufferedBlockCount())
        val destination = ByteArray(body.size)
        assertEquals(body.size, stream.read(destination))
        assertArrayEquals(body, destination)
        assertEquals(-1, stream.read(ByteArray(1)))
        assertEquals(0, stream.bufferedBlockCount())
        assertEquals(0L, session.diagnostics().receiveBytes)
    }

    @Test
    fun inboundBacklogOverflowResetsOnlyTheRejectedStream() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(maxStreams = 2, acceptBacklog = 1),
        )
        transport.feed(
            frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L) +
                frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 4L),
        )
        transport.awaitWrittenSize(YamuxProtocol.HEADER_SIZE)

        val accepted = session.acceptStream()
        assertEquals(2L, accepted.streamId)
        val frames = parseYamuxTrace(transport.writtenBytes())
        assertEquals(
            listOf(
                YamuxFrameHeader(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.RST, 4L, 0L),
                YamuxFrameHeader(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 2L, 0L),
            ),
            frames.map(YamuxTestFrame::header),
        )
        assertEquals(1, session.diagnostics().activeStreamPermits)

        accepted.closeWrite()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 2L))
        accepted.awaitClosed()
        assertEquals(0, session.diagnostics().activeStreamPermits)
        session.close()
    }

    @Test
    fun rejectedDataSynUsesTheProtocolInitialWindowEvenWithASixMiBConfig() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(
                receiveWindowSize = YAMUX_FRP_WINDOW,
                maxStreams = 1,
                acceptBacklog = 1,
            ),
        )
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L))
        transport.feed(
            frame(
                type = YamuxFrameType.DATA,
                flags = YamuxFlags.SYN,
                streamId = 4L,
                length = YAMUX_INITIAL_WINDOW + 1L,
            ),
        )

        val failure = suspendFailureOf { session.awaitTermination() }

        assertTrue(failure is YamuxProtocolFailure)
        assertEquals(YamuxProtocolError.RECEIVE_WINDOW_EXCEEDED, (failure as YamuxProtocolFailure).reason)
        assertEquals(0L, session.diagnostics().receiveBytes)
        assertEquals(0, session.diagnostics().activeStreamPermits)
    }

    @Test
    fun concurrentRemoteSynFramesMayArriveOutOfStreamIdOrder() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(maxStreams = 2, acceptBacklog = 2),
        )
        transport.feed(
            frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 4L) +
                frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L),
        )

        val first = session.acceptStream()
        val second = session.acceptStream()

        assertEquals(listOf(4L, 2L), listOf(first.streamId, second.streamId))
        first.closeWrite()
        second.closeWrite()
        transport.feed(
            frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 4L) +
                frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 2L),
        )
        first.awaitClosed()
        second.awaitClosed()
        session.close()
    }

    @Test
    fun concurrentAcceptWaitersEachReceiveOneInboundStream() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(maxStreams = 2, acceptBacklog = 1),
        )
        val first = async { session.acceptStream() }
        val second = async { session.acceptStream() }
        runCurrent()
        assertEquals(2, session.diagnostics().acceptWaiters)

        transport.feed(
            frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 4L) +
                frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L),
        )
        val accepted = listOf(first.await(), second.await())

        assertEquals(listOf(4L, 2L), accepted.map { it.streamId })
        assertEquals(0, session.diagnostics().acceptWaiters)
        assertEquals(0, session.diagnostics().acceptBacklog)
        accepted.forEach { it.closeWrite() }
        transport.feed(
            frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 4L) +
                frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 2L),
        )
        accepted.forEach { it.awaitClosed() }
        session.close()
    }

    @Test
    fun acceptWaitersAreBoundedAndAllWakeOnClose() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(maxStreams = 2, acceptBacklog = 1),
        )
        val first = backgroundScope.async { suspendFailureOf { session.acceptStream() } }
        val second = backgroundScope.async { suspendFailureOf { session.acceptStream() } }
        runCurrent()
        assertEquals(2, session.diagnostics().acceptWaiters)

        val overflow = suspendFailureOf { session.acceptStream() }
        assertTrue(overflow is YamuxResourceLimitFailure)
        assertEquals(YamuxResourceKind.ACCEPT_BACKLOG, (overflow as YamuxResourceLimitFailure).resource)

        session.close()

        assertTrue(first.await() is YamuxSessionClosedFailure)
        assertTrue(second.await() is YamuxSessionClosedFailure)
        assertEquals(0, session.diagnostics().acceptWaiters)
        assertEquals(0, session.diagnostics().activeChildJobs)
    }

    @Test
    fun receiveWindowUpdatesFollowBoundedFifoOrderInsteadOfStreamIdOrder() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate, writeGateAfterStarts = 3)
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(maxStreams = 3),
        )
        val streams = List(3) { session.openStream() }
        streams.forEach { stream ->
            transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, stream.streamId))
        }
        runCurrent()

        val ping = backgroundScope.async { session.ping() }
        transport.awaitWriteStarts(4)
        val closing = streams.map { stream -> backgroundScope.async { stream.closeWrite() } }
        runCurrent()
        assertEquals(4, session.diagnostics().writerPendingFrames)

        val threshold = (YAMUX_INITIAL_WINDOW / 2L).toInt()
        val payload = ByteArray(threshold) { index -> index.toByte() }
        val dispatchOrder = listOf(streams[2], streams[1], streams[0])
        dispatchOrder.forEachIndexed { index, stream ->
            transport.feed(dataFrame(stream.streamId, payload))
            assertEquals(threshold, stream.read(ByteArray(threshold)))
            runCurrent()
            if (index == 0) assertEquals(5, session.diagnostics().writerPendingFrames)
        }
        assertEquals(2, session.diagnostics().pendingReceiveUpdates)

        gate.complete(Unit)
        transport.feed(frame(YamuxFrameType.PING, YamuxFlags.ACK, length = 0L))
        ping.await()
        closing.awaitAll()
        transport.awaitWrittenSize(10 * YamuxProtocol.HEADER_SIZE)

        val updateOrder = parseYamuxTrace(transport.writtenBytes())
            .filter { frame ->
                frame.header.type == YamuxFrameType.WINDOW_UPDATE &&
                    frame.header.flags == 0 &&
                    frame.header.length == threshold.toLong()
            }
            .map { it.header.streamId }
        assertEquals(listOf(5L, 3L, 1L), updateOrder)
        assertEquals(0, session.diagnostics().pendingReceiveUpdates)
        assertEquals(0L, session.diagnostics().receiveBytes)
        session.close()
    }

    @Test
    fun lateDataAfterTombstoneEvictionIsDrainedWithoutTerminatingTheSession() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(maxStreams = 1, tombstoneCapacity = 1),
        )
        val first = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.RST, 1L))
        assertTrue(suspendFailureOf { first.awaitClosed() } is YamuxStreamResetFailure)
        val second = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.RST, 3L))
        assertTrue(suspendFailureOf { second.awaitClosed() } is YamuxStreamResetFailure)

        transport.feed(
            dataFrame(1L, byteArrayOf(1, 2, 3, 4)) +
                frame(YamuxFrameType.PING, YamuxFlags.SYN, length = 77L),
        )
        transport.awaitWrittenSize(4 * YamuxProtocol.HEADER_SIZE)

        assertFalse(session.diagnostics().ending)
        assertEquals(1, session.diagnostics().tombstones)
        assertEquals(
            listOf(
                YamuxFrameHeader(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.RST, 1L, 0L),
                YamuxFrameHeader(YamuxFrameType.PING, YamuxFlags.ACK, 0L, 77L),
            ),
            parseYamuxTrace(transport.writtenBytes()).takeLast(2).map(YamuxTestFrame::header),
        )
        session.close()
    }

    @Test
    fun dataResetBodyIsDrainedBeforeTheNextFrame() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        val resetBody = byteArrayOf(1, 2, 3, 4)
        transport.feed(
            yamuxFrameBytes(
                type = YamuxFrameType.DATA,
                flags = YamuxFlags.RST or YamuxFlags.FIN,
                streamId = 1L,
                length = resetBody.size.toLong(),
                body = resetBody,
            ) +
                dataFrame(stream.streamId, byteArrayOf(5, 6, 7)) +
                frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, stream.streamId) +
                frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.RST, stream.streamId) +
                frame(YamuxFrameType.PING, YamuxFlags.SYN, length = 77L),
        )

        assertTrue(suspendFailureOf { stream.awaitClosed() } is YamuxStreamResetFailure)
        transport.awaitWrittenSize(2 * YamuxProtocol.HEADER_SIZE)

        assertFalse(session.diagnostics().ending)
        assertEquals(
            YamuxFrameHeader(YamuxFrameType.PING, YamuxFlags.ACK, 0L, 77L),
            parseYamuxTrace(transport.writtenBytes()).last().header,
        )
        assertEquals(
            0,
            parseYamuxTrace(transport.writtenBytes()).count {
                it.header.flags and YamuxFlags.RST != 0 && it.header.streamId == stream.streamId
            },
        )
        session.close()
    }

    @Test
    fun failedOpenAfterRemoteGoAwayDoesNotReleaseAnotherStreamsPermit() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(maxStreams = 2),
        )
        val existing = session.openStream()
        transport.feed(frame(YamuxFrameType.GOAWAY))
        runCurrent()

        val failure = suspendFailureOf { session.openStream() }

        assertTrue(failure is YamuxRemoteGoAwayFailure)
        assertEquals(1, session.diagnostics().activeStreams)
        assertEquals(1, session.diagnostics().activeStreamPermits)
        existing.closeWrite()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, 1L))
        existing.awaitClosed()
        session.close()
    }

    @Test
    fun queuedNormalGoAwayCancellationRollsBackAndASecondAttemptCanSend() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val ping = backgroundScope.async { session.ping() }
        transport.awaitWriteStarts(1)

        val first = backgroundScope.async { session.sendGoAway() }
        runCurrent()
        assertEquals(2, session.diagnostics().writerPendingFrames)
        first.cancel(CancellationException("cancel-first-go-away"))
        assertTrue(suspendFailureOf { first.await() } is CancellationException)
        runCurrent()

        assertEquals(0, transport.writtenBytes().size)
        val second = backgroundScope.async { session.sendGoAway() }
        runCurrent()
        assertFalse(second.isCompleted)

        gate.complete(Unit)
        transport.awaitWrittenSize(2 * YamuxProtocol.HEADER_SIZE)
        transport.feed(frame(YamuxFrameType.PING, YamuxFlags.ACK, length = 0L))
        ping.await()
        second.await()

        assertEquals(
            1,
            parseYamuxTrace(transport.writtenBytes()).count { it.header.type == YamuxFrameType.GOAWAY },
        )
        session.close()
        assertEquals(
            1,
            parseYamuxTrace(transport.writtenBytes()).count { it.header.type == YamuxFrameType.GOAWAY },
        )
    }

    @Test
    fun concurrentNormalGoAwayCallsShareOnePhysicalCompletion() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())

        val first = backgroundScope.async { session.sendGoAway() }
        transport.awaitWriteStarts(1)
        val second = backgroundScope.async { session.sendGoAway() }
        runCurrent()

        assertFalse(first.isCompleted)
        assertFalse(second.isCompleted)
        assertEquals(1, session.diagnostics().writerPendingFrames)

        gate.complete(Unit)
        transport.awaitWrittenSize(YamuxProtocol.HEADER_SIZE)
        first.await()
        second.await()
        assertEquals(
            listOf(YamuxFrameHeader(YamuxFrameType.GOAWAY, 0, 0L, 0L)),
            parseYamuxTrace(transport.writtenBytes()).map(YamuxTestFrame::header),
        )

        session.close()
        assertEquals(
            1,
            parseYamuxTrace(transport.writtenBytes()).count { it.header.type == YamuxFrameType.GOAWAY },
        )
    }

    @Test
    fun hardCloseWakesNormalGoAwayCallersAndCancelsThePendingFrame() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val first = backgroundScope.async { suspendFailureOf { session.sendGoAway() } }
        transport.awaitWriteStarts(1)
        val second = backgroundScope.async { suspendFailureOf { session.sendGoAway() } }
        runCurrent()

        val closing = backgroundScope.async { session.close() }
        runCurrent()
        assertTrue(first.isCompleted)
        assertTrue(second.isCompleted)
        assertTrue(first.await() is YamuxSessionClosedFailure)
        assertTrue(second.await() is YamuxSessionClosedFailure)

        gate.complete(Unit)
        closing.await()
        assertEquals(
            0,
            parseYamuxTrace(transport.writtenBytes()).count { it.header.type == YamuxFrameType.GOAWAY },
        )
        assertEquals(0, session.diagnostics().activeChildJobs)
    }

    @Test
    fun normalGoAwayWriteErrorWakesEverySharedCaller() = runTest {
        val gate = CompletableDeferred<Unit>()
        val expected = AssertionError("normal-go-away-write-error")
        val transport = YamuxTestTransport(writeGate = gate, writeFailure = expected)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val first = backgroundScope.async { suspendFailureOf { session.sendGoAway() } }
        transport.awaitWriteStarts(1)
        val second = backgroundScope.async { suspendFailureOf { session.sendGoAway() } }
        runCurrent()

        gate.complete(Unit)

        val sharedFailure = first.await()
        assertTrue(sharedFailure is AssertionError)
        assertEquals(expected.message, sharedFailure.message)
        assertSame(sharedFailure, second.await())
        assertSame(sharedFailure, suspendFailureOf { session.awaitTermination() })
        assertEquals(0, session.diagnostics().activeChildJobs)
    }

    @Test
    fun queuedNormalGoAwayDoesNotRejectInboundSynBeforePhysicalOwnership() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val ping = backgroundScope.async { session.ping() }
        transport.awaitWriteStarts(1)
        val goAway = backgroundScope.async { session.sendGoAway() }
        val accepting = backgroundScope.async { session.acceptStream() }
        runCurrent()

        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L))
        runCurrent()
        assertEquals(1, session.diagnostics().activeStreams)
        assertFalse(accepting.isCompleted)

        gate.complete(Unit)
        transport.awaitWrittenSize(3 * YamuxProtocol.HEADER_SIZE)
        transport.feed(frame(YamuxFrameType.PING, YamuxFlags.ACK, length = 0L))
        ping.await()
        goAway.await()
        assertEquals(2L, accepting.await().streamId)

        val headers = parseYamuxTrace(transport.writtenBytes()).map(YamuxTestFrame::header)
        assertEquals(1, headers.count { it.type == YamuxFrameType.GOAWAY })
        assertEquals(0, headers.count { it.flags and YamuxFlags.RST != 0 })
        session.close()
    }

    @Test
    fun localGoAwayRejectsInboundStreamsButStillAllowsLocalOpen() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(maxStreams = 2),
        )

        session.sendGoAway()
        val outbound = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L))
        transport.awaitWrittenSize(3 * YamuxProtocol.HEADER_SIZE)

        assertEquals(1L, outbound.streamId)
        assertEquals(
            listOf(
                YamuxFrameHeader(YamuxFrameType.GOAWAY, 0, 0L, 0L),
                YamuxFrameHeader(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 1L, 0L),
                YamuxFrameHeader(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.RST, 2L, 0L),
            ),
            parseYamuxTrace(transport.writtenBytes()).map(YamuxTestFrame::header),
        )
        assertEquals(0, session.diagnostics().acceptBacklog)
        session.close()
    }

    @Test
    fun duplicateNormalGoAwayAndUnknownPingAckAreIgnored() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        transport.feed(
            frame(YamuxFrameType.PING, YamuxFlags.ACK, length = 99L) +
                frame(YamuxFrameType.GOAWAY) +
                frame(YamuxFrameType.GOAWAY),
        )
        runCurrent()

        assertFalse(session.diagnostics().ending)
        val ping = async { session.ping() }
        transport.awaitWrittenSize(YamuxProtocol.HEADER_SIZE)
        transport.feed(frame(YamuxFrameType.PING, YamuxFlags.ACK, length = 0L))
        assertTrue(ping.await() >= 0L)
        assertFalse(session.diagnostics().ending)
        session.close()
    }

    @Test
    fun cancelledResetWaiterDoesNotCancelTheSessionOwnedRst() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate, writeGateAfterStarts = 1)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, stream.streamId))
        runCurrent()
        val ping = backgroundScope.async { session.ping() }
        transport.awaitWriteStarts(2)

        val resetting = backgroundScope.async { stream.reset() }
        runCurrent()
        assertEquals(0, session.diagnostics().activeStreams)
        assertEquals(1, session.diagnostics().activeStreamPermits)
        assertEquals(1, session.diagnostics().pendingResetRequests)
        resetting.cancel(CancellationException("cancel-reset-waiter"))
        assertTrue(suspendFailureOf { resetting.await() } is CancellationException)
        runCurrent()
        assertEquals(1, session.diagnostics().pendingResetRequests)

        gate.complete(Unit)
        transport.awaitWrittenSize(3 * YamuxProtocol.HEADER_SIZE)
        transport.feed(frame(YamuxFrameType.PING, YamuxFlags.ACK, length = 0L))
        ping.await()
        runCurrent()

        assertEquals(0, session.diagnostics().pendingResetRequests)
        assertEquals(0, session.diagnostics().activeStreamPermits)
        assertEquals(
            1,
            parseYamuxTrace(transport.writtenBytes()).count {
                it.header.type == YamuxFrameType.WINDOW_UPDATE &&
                    it.header.flags and YamuxFlags.RST != 0 &&
                    it.header.streamId == stream.streamId
            },
        )
        session.close()
        assertEquals(
            1,
            parseYamuxTrace(transport.writtenBytes()).count {
                it.header.type == YamuxFrameType.WINDOW_UPDATE &&
                    it.header.flags and YamuxFlags.RST != 0 &&
                    it.header.streamId == stream.streamId
            },
        )
    }

    @Test
    fun localResetSendsOnlyOneRstAcrossMultipleLateFrames() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, stream.streamId))
        runCurrent()

        stream.reset()
        transport.awaitWrittenSize(2 * YamuxProtocol.HEADER_SIZE)
        transport.feed(
            dataFrame(stream.streamId, byteArrayOf(1, 2, 3)) +
                frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, stream.streamId) +
                dataFrame(stream.streamId, byteArrayOf(4, 5)) +
                frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.RST, stream.streamId) +
                frame(YamuxFrameType.PING, YamuxFlags.SYN, length = 91L),
        )
        transport.awaitWrittenSize(3 * YamuxProtocol.HEADER_SIZE)
        runCurrent()

        assertFalse(session.diagnostics().ending)
        assertEquals(
            1,
            parseYamuxTrace(transport.writtenBytes()).count {
                it.header.type == YamuxFrameType.WINDOW_UPDATE &&
                    it.header.flags and YamuxFlags.RST != 0 &&
                    it.header.streamId == stream.streamId
            },
        )
        assertEquals(
            YamuxFrameHeader(YamuxFrameType.PING, YamuxFlags.ACK, 0L, 91L),
            parseYamuxTrace(transport.writtenBytes()).last().header,
        )
        session.close()
    }

    @Test
    fun resetAdmissionAfterEndingAndDispatcherExitCompletesSynchronously() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate, writeGateAfterStarts = 1)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, stream.streamId))
        runCurrent()
        val ping = backgroundScope.async { suspendFailureOf { session.ping() } }
        transport.awaitWriteStarts(2)

        assertTrue(stream.failLocallyForReset(YamuxStreamResetFailure()))
        assertEquals(1, session.diagnostics().activeStreamPermits)
        val closing = backgroundScope.async { session.close() }
        session.awaitResetDispatcherStopped()

        val lateAdmission = session.scheduleResetBestEffort(stream)
        assertTrue(lateAdmission.isCompleted)
        assertNull(lateAdmission.await())
        assertEquals(0, session.diagnostics().pendingResetRequests)
        assertEquals(0, session.diagnostics().activeStreamPermits)

        gate.complete(Unit)
        closing.await()
        assertTrue(ping.await() is YamuxSessionClosedFailure)
        assertEquals(0, session.diagnostics().activeChildJobs)
        assertEquals(
            0,
            parseYamuxTrace(transport.writtenBytes()).count {
                it.header.type == YamuxFrameType.WINDOW_UPDATE &&
                    it.header.flags and YamuxFlags.RST != 0 &&
                    it.header.streamId == stream.streamId
            },
        )
    }

    @Test
    fun sessionCloseTerminatesQueuedResetWithoutLeakingAChild() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate, writeGateAfterStarts = 1)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, stream.streamId))
        runCurrent()
        val ping = backgroundScope.async { suspendFailureOf { session.ping() } }
        transport.awaitWriteStarts(2)
        val resetting = backgroundScope.async { stream.reset() }
        runCurrent()
        assertEquals(1, session.diagnostics().pendingResetRequests)

        val closing = backgroundScope.async { session.close() }
        runCurrent()
        gate.complete(Unit)
        closing.await()
        resetting.await()

        assertTrue(ping.await() is YamuxSessionClosedFailure)
        assertEquals(0, session.diagnostics().pendingResetRequests)
        assertEquals(0, session.diagnostics().activeStreamPermits)
        assertEquals(0, session.diagnostics().activeChildJobs)
        assertEquals(
            0,
            parseYamuxTrace(transport.writtenBytes()).count {
                it.header.type == YamuxFrameType.WINDOW_UPDATE &&
                    it.header.flags and YamuxFlags.RST != 0 &&
                    it.header.streamId == stream.streamId
            },
        )
    }

    @Test
    fun parentCompletionClosesTheSessionIncludingAlreadyCancelledParents() = runTest {
        val completedParent = Job()
        val completedTransport = YamuxTestTransport()
        val completedSession = createYamuxClientSession(
            completedTransport,
            CoroutineScope(coroutineContext + completedParent),
            testConfig(),
        )
        completedParent.complete()
        assertTrue(suspendFailureOf { completedSession.awaitTermination() } is YamuxSessionClosedFailure)
        assertTrue(completedTransport.isClosed)

        val cancellation = CancellationException("already-cancelled-parent")
        val cancelledParent = Job().also { it.cancel(cancellation) }
        val cancelledTransport = YamuxTestTransport()
        val cancelledSession = createYamuxClientSession(
            cancelledTransport,
            CoroutineScope(coroutineContext + cancelledParent),
            testConfig(),
        )
        val cancellationFailure = suspendFailureOf { cancelledSession.awaitTermination() }
        assertTrue(cancellationFailure is CancellationException)
        assertEquals(cancellation.message, cancellationFailure.message)
        assertTrue(cancelledTransport.isClosed)
    }

    @Test
    fun terminationWaitsForEveryChildAndPreservesReaderErrorDuringClose() = runTest {
        val readerStarted = CompletableDeferred<Unit>()
        val allowReaderExit = CompletableDeferred<Unit>()
        val activeReads = AtomicInteger(0)
        val physicalWrites = AtomicInteger(0)
        val transportClosed = AtomicBoolean(false)
        val expected = AssertionError("reader-exit-error")
        val transport = object : YamuxPhysicalTransport {
            override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int {
                activeReads.incrementAndGet()
                readerStarted.complete(Unit)
                try {
                    awaitCancellation()
                } finally {
                    withContext(NonCancellable) {
                        allowReaderExit.await()
                    }
                    activeReads.decrementAndGet()
                    throw expected
                }
            }

            override suspend fun writeFully(source: ByteArray, offset: Int, length: Int) {
                physicalWrites.incrementAndGet()
            }

            override fun close() {
                transportClosed.set(true)
            }
        }
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        readerStarted.await()
        val closeResult = CompletableDeferred<Throwable?>()
        backgroundScope.launch {
            val failure = try {
                session.close()
                null
            } catch (caught: Throwable) {
                caught
            }
            closeResult.complete(failure)
        }
        runCurrent()

        assertFalse(closeResult.isCompleted)
        assertEquals(1, activeReads.get())
        assertTrue(transportClosed.get())

        allowReaderExit.complete(Unit)
        assertSame(expected, closeResult.await())
        val diagnostics = session.diagnostics()
        assertEquals(0, diagnostics.activeChildJobs)
        assertEquals(0, diagnostics.writerPendingFrames)
        assertEquals(0L, diagnostics.writerPendingBytes)
        assertEquals(0, activeReads.get())
        val writesAfterTermination = physicalWrites.get()
        advanceTimeBy(10_000L)
        runCurrent()
        assertEquals(writesAfterTermination, physicalWrites.get())
    }

    @Test
    fun writerFatalOnAnUnconfinedDispatcherDoesNotSelfJoinTheFinalizer() = runTest {
        val expected = YamuxTransportFailure(YamuxTransportOperation.WRITE)
        val transport = YamuxTestTransport(writeFailure = expected)
        val session = createYamuxClientSession(
            transport,
            CoroutineScope(UnconfinedTestDispatcher(testScheduler)),
            testConfig(),
        )

        assertSame(expected, suspendFailureOf { session.openStream() })
        assertSame(expected, suspendFailureOf { session.awaitTermination() })
        assertEquals(0, session.diagnostics().activeChildJobs)
        assertEquals(0, transport.activeWriteCount)
    }

    @Test
    fun keepAliveCoroutineUsesVirtualTimeAndTerminatesWithoutLeakingChildren() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(
                keepAliveEnabled = true,
                keepAliveIntervalMillis = 1_000L,
                keepAliveTimeoutMillis = 500L,
            ),
        )

        advanceTimeBy(1_000L)
        runCurrent()
        transport.awaitWrittenSize(YamuxProtocol.HEADER_SIZE)
        assertEquals(
            YamuxFrameHeader(YamuxFrameType.PING, YamuxFlags.SYN, 0L, 0L),
            parseYamuxTrace(transport.writtenBytes()).single().header,
        )

        advanceTimeBy(500L)
        runCurrent()
        val failure = suspendFailureOf { session.awaitTermination() }

        assertTrue(failure is YamuxTimeoutFailure)
        assertEquals(YamuxTimeoutKind.KEEPALIVE, (failure as YamuxTimeoutFailure).kind)
        assertEquals(0, session.diagnostics().activeChildJobs)
        assertEquals(0, transport.activeReadCount)
        assertEquals(0, transport.activeWriteCount)
    }

    @Test
    fun sendWindowOverflowTerminatesWithAProtocolFailure() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        session.openStream()
        transport.feed(
            frame(
                YamuxFrameType.WINDOW_UPDATE,
                YamuxFlags.ACK,
                streamId = 1L,
                length = YamuxProtocol.MAX_UINT32,
            ),
        )

        val failure = suspendFailureOf { session.awaitTermination() }

        assertTrue(failure is YamuxProtocolFailure)
        assertEquals(YamuxProtocolError.SEND_WINDOW_OVERFLOW, (failure as YamuxProtocolFailure).reason)
        assertEquals(0, session.diagnostics().activeStreamPermits)
    }

    @Test
    fun clientStreamIdsFailBeforeUint32WrapAndReturnThePermit() = runTest {
        val transport = YamuxTestTransport()
        val lastUsableId = YamuxProtocol.MAX_UINT32 - 2L
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(maxStreams = 1, initialLocalStreamId = lastUsableId),
        )
        val lastStream = session.openStream()
        assertEquals(lastUsableId, lastStream.streamId)
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.RST, lastUsableId))
        assertTrue(suspendFailureOf { lastStream.awaitClosed() } is YamuxStreamResetFailure)

        assertTrue(suspendFailureOf { session.openStream() } is YamuxStreamIdExhaustedFailure)
        assertEquals(0, session.diagnostics().activeStreams)
        assertEquals(0, session.diagnostics().activeStreamPermits)
        session.close()
    }

    @Test
    fun pingTimeoutTerminatesTheSessionWithTheOperationSpecificKind() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(keepAliveTimeoutMillis = 1_000L),
        )
        val pingFailure = async { suspendFailureOf { session.ping() } }
        transport.awaitWrittenSize(YamuxProtocol.HEADER_SIZE)

        advanceTimeBy(1_000L)
        runCurrent()

        val failure = pingFailure.await()
        assertTrue(failure is YamuxTimeoutFailure)
        assertEquals(YamuxTimeoutKind.PING, (failure as YamuxTimeoutFailure).kind)
        assertTrue(session.diagnostics().terminated)
        assertTrue(transport.isClosed)
    }

    @Test
    fun repeatedOpenAndCloseLeavesOnlyTheBoundedTombstoneSet() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(maxStreams = 1, tombstoneCapacity = 8),
        )

        repeat(64) { index ->
            val stream = session.openStream()
            val streamId = index.toLong() * 2L + 1L
            assertEquals(streamId, stream.streamId)
            transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, streamId))
            runCurrent()
            stream.closeWrite()
            transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.FIN, streamId))
            stream.awaitClosed()
            val diagnostics = session.diagnostics()
            assertEquals(0, diagnostics.activeStreams)
            assertEquals(0, diagnostics.activeStreamPermits)
            assertEquals(0, diagnostics.pendingOpenAcknowledgements)
            assertEquals(0L, diagnostics.receiveBytes)
        }

        assertEquals(8, session.diagnostics().tombstones)
        session.close()
    }

    @Test
    fun inboundSynRacingSessionCloseCannotLeaveAStreamOrPermit() = runTest {
        repeat(32) { index ->
            val transport = YamuxTestTransport()
            val session = createYamuxClientSession(
                transport,
                backgroundScope,
                testConfig(maxStreams = 1),
            )
            if (index and 1 == 0) {
                transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L))
            }
            val closing = async { session.close() }
            if (index and 1 != 0) {
                transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.SYN, 2L))
            }
            closing.await()
            runCurrent()

            val diagnostics = session.diagnostics()
            assertEquals(0, diagnostics.activeStreams)
            assertEquals(0, diagnostics.activeStreamPermits)
            assertEquals(0L, diagnostics.receiveBytes)
            assertTrue(diagnostics.terminated)
        }
    }

    @Test
    fun streamOpenAndCloseTimeoutsAreTypedAndBounded() = runTest {
        val openTransport = YamuxTestTransport()
        val openSession = createYamuxClientSession(
            openTransport,
            backgroundScope,
            testConfig(streamOpenTimeoutMillis = 1_000L),
        )
        openSession.openStream()
        advanceTimeBy(1_000L)
        runCurrent()
        val openFailure = suspendFailureOf { openSession.awaitTermination() }
        assertTrue(openFailure is YamuxTimeoutFailure)
        assertEquals(YamuxTimeoutKind.STREAM_OPEN, (openFailure as YamuxTimeoutFailure).kind)

        val closeTransport = YamuxTestTransport()
        val closeSession = createYamuxClientSession(
            closeTransport,
            backgroundScope,
            testConfig(streamCloseTimeoutMillis = 1_000L),
        )
        val stream = closeSession.openStream()
        closeTransport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, 1L))
        runCurrent()
        stream.closeWrite()
        advanceTimeBy(1_000L)
        runCurrent()
        val closeFailure = suspendFailureOf { stream.awaitClosed() }
        assertTrue(closeFailure is YamuxTimeoutFailure)
        assertEquals(YamuxTimeoutKind.STREAM_CLOSE, (closeFailure as YamuxTimeoutFailure).kind)
        closeTransport.awaitWrittenSize(3 * YamuxProtocol.HEADER_SIZE)
        runCurrent()
        assertFalse(closeSession.diagnostics().ending)
        assertEquals(0, closeSession.diagnostics().activeStreams)
        assertEquals(0, closeSession.diagnostics().activeStreamPermits)
        assertEquals(0, closeSession.diagnostics().pendingResetRequests)
        assertEquals(
            1,
            parseYamuxTrace(closeTransport.writtenBytes()).count {
                it.header.type == YamuxFrameType.WINDOW_UPDATE &&
                    it.header.flags and YamuxFlags.RST != 0 &&
                    it.header.streamId == stream.streamId
            },
        )
        closeSession.close()
    }

    @Test
    fun transportExceptionsAreSanitizedAndCancellationAndErrorsKeepIdentity() = runTest {
        val unsafeMarker = "endpoint-payload-leak-marker"
        val failedTransport = YamuxTestTransport()
        val failedSession = createYamuxClientSession(failedTransport, backgroundScope, testConfig())
        failedTransport.failInput(IllegalStateException(unsafeMarker))
        val transportFailure = suspendFailureOf { failedSession.awaitTermination() }
        assertTrue(transportFailure is YamuxTransportFailure)
        assertEquals(YamuxTransportOperation.READ, (transportFailure as YamuxTransportFailure).operation)
        assertFalse(transportFailure.toString().contains(unsafeMarker))
        assertNull(transportFailure.cause)

        val cancellation = CancellationException("cancellation-identity-marker")
        val cancelledTransport = YamuxTestTransport()
        val cancelledSession = createYamuxClientSession(cancelledTransport, backgroundScope, testConfig())
        cancelledTransport.failInput(cancellation)
        assertSame(cancellation, suspendFailureOf { cancelledSession.awaitTermination() })

        val error = AssertionError("error-identity-marker")
        val errorTransport = YamuxTestTransport()
        val errorSession = createYamuxClientSession(errorTransport, backgroundScope, testConfig())
        errorTransport.failInput(error)
        assertSame(error, suspendFailureOf { errorSession.awaitTermination() })
    }

    @Test
    fun rollbackCallbacksPreserveErrorOverCancellationAndReachTheSessionWaiter() = runTest {
        val gate = CompletableDeferred<Unit>()
        val transport = YamuxTestTransport(writeGate = gate, writeGateAfterStarts = 1)
        val session = createYamuxClientSession(
            transport,
            backgroundScope,
            testConfig(writeTimeoutMillis = 1_000L),
        )
        val stream = session.openStream()
        transport.feed(frame(YamuxFrameType.WINDOW_UPDATE, YamuxFlags.ACK, stream.streamId))
        runCurrent()
        val ping = backgroundScope.async { suspendFailureOf { session.ping() } }
        transport.awaitWriteStarts(2)
        val cancellation = CancellationException("rollback-cancellation")
        val error = AssertionError("rollback-error")
        val cancelledRequest = backgroundScope.async {
            suspendFailureOf {
                session.sendStreamFrame(
                    stream = stream,
                    type = YamuxFrameType.WINDOW_UPDATE,
                    flags = 0,
                    length = 1L,
                    onNotStarted = { throw cancellation },
                )
            }
        }
        runCurrent()
        val erroredRequest = backgroundScope.async {
            suspendFailureOf {
                session.sendStreamFrame(
                    stream = stream,
                    type = YamuxFrameType.WINDOW_UPDATE,
                    flags = 0,
                    length = 1L,
                    onNotStarted = { throw error },
                )
            }
        }
        runCurrent()
        assertEquals(3, session.diagnostics().writerPendingFrames)

        stream.reset()
        runCurrent()
        gate.complete(Unit)

        assertSame(cancellation, cancelledRequest.await())
        assertSame(error, erroredRequest.await())
        assertSame(error, suspendFailureOf { session.awaitTermination() })
        assertSame(cancellation, ping.await())
        assertEquals(0, session.diagnostics().activeChildJobs)
        assertEquals(0L, session.diagnostics().writerPendingBytes)
    }

    @Test
    fun physicalOwnershipCallbackOrdinaryFailureIsSessionFatal() = runTest {
        val transport = YamuxTestTransport()
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        val stream = session.openStream()
        val expected = IllegalStateException("ownership-callback-failure")

        val operationFailure = suspendFailureOf {
            session.sendStreamFrame(
                stream = stream,
                type = YamuxFrameType.WINDOW_UPDATE,
                flags = 0,
                length = 1L,
                onWriteStarted = { throw expected },
            )
        }

        assertSame(expected, operationFailure)
        assertSame(expected, suspendFailureOf { session.awaitTermination() })
        assertEquals(0, session.diagnostics().activeChildJobs)
    }

    @Test
    fun explicitClosePreservesJvmErrorFromTransportClose() = runTest {
        val expected = AssertionError("close-error-identity-marker")
        val transport = YamuxTestTransport(closeFailure = expected)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())

        assertSame(expected, suspendFailureOf { session.close() })
    }

    @Test
    fun transportCloseErrorOverridesAnEarlierWriteCancellation() = runTest {
        val original = CancellationException("close-write-cancellation-identity-marker")
        val secondary = AssertionError("close-error-identity-marker")
        val transport = YamuxTestTransport(writeFailure = original, closeFailure = secondary)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())

        assertSame(secondary, suspendFailureOf { session.close() })
    }

    @Test
    fun transportCloseErrorDoesNotReplaceTheOriginalJvmError() = runTest {
        val original = AssertionError("original-error-identity-marker")
        val secondary = AssertionError("secondary-error-identity-marker")
        val transport = YamuxTestTransport(closeFailure = secondary)
        val session = createYamuxClientSession(transport, backgroundScope, testConfig())
        transport.failInput(original)

        assertSame(original, suspendFailureOf { session.awaitTermination() })
    }

    private fun testConfig(
        writerMailboxCapacity: Int = 64,
        writerQueuedByteCapacity: Long = 512L * 1024L,
        outboundDataFrameSize: Int = 16 * 1024,
        receiveWindowSize: Long = YAMUX_INITIAL_WINDOW,
        maxBufferedReceiveBytes: Long = 8L * 1024L * 1024L,
        maxStreams: Int = 4,
        acceptBacklog: Int = maxStreams,
        tombstoneCapacity: Int = 16,
        streamOpenTimeoutMillis: Long = 60_000L,
        streamCloseTimeoutMillis: Long = 60_000L,
        writeTimeoutMillis: Long = 10_000L,
        keepAliveEnabled: Boolean = false,
        keepAliveIntervalMillis: Long = 30_000L,
        keepAliveTimeoutMillis: Long = 10_000L,
        initialLocalStreamId: Long = 1L,
    ): YamuxConfig = YamuxConfig(
        receiveWindowSize = receiveWindowSize,
        outboundDataFrameSize = outboundDataFrameSize,
        maxBufferedReceiveBytes = maxBufferedReceiveBytes,
        writerMailboxCapacity = writerMailboxCapacity,
        writerQueuedByteCapacity = writerQueuedByteCapacity,
        maxStreams = maxStreams,
        maxPendingOpens = maxStreams,
        acceptBacklog = acceptBacklog,
        tombstoneCapacity = tombstoneCapacity,
        streamOpenTimeoutMillis = streamOpenTimeoutMillis,
        streamCloseTimeoutMillis = streamCloseTimeoutMillis,
        writeTimeoutMillis = writeTimeoutMillis,
        keepAliveEnabled = keepAliveEnabled,
        keepAliveIntervalMillis = keepAliveIntervalMillis,
        keepAliveTimeoutMillis = keepAliveTimeoutMillis,
        initialLocalStreamId = initialLocalStreamId,
    )

    private fun pingHeader(id: Long): YamuxFrameHeader = YamuxFrameHeader(
        type = YamuxFrameType.PING,
        flags = YamuxFlags.SYN,
        streamId = 0L,
        length = id,
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

    private suspend fun suspendFailureOf(block: suspend () -> Unit): Throwable {
        try {
            block()
        } catch (failure: Throwable) {
            return failure
        }
        throw AssertionError("Expected failure")
    }
}
