package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux

import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal enum class YamuxStreamInitiator {
    LOCAL,
    REMOTE,
}

internal class YamuxStream internal constructor(
    val streamId: Long,
    internal val initiator: YamuxStreamInitiator,
    private val session: YamuxClientSession,
    private val config: YamuxConfig,
) {
    private val stateLock = Any()
    private val readMutex = Mutex()
    private val writeMutex = Mutex()
    private val readSignal = Channel<Unit>(Channel.CONFLATED)
    private val writeSignal = Channel<Unit>(Channel.CONFLATED)
    private val failureSignal = CompletableDeferred<Unit>()
    private val closedSignal = CompletableDeferred<Unit>()
    private val receiveBuffer = YamuxReceiveBuffer()

    private var accepted = initiator == YamuxStreamInitiator.LOCAL
    private var accepting = false
    private var acknowledged = initiator == YamuxStreamInitiator.REMOTE
    private var sendCreditAvailable = YamuxProtocol.INITIAL_STREAM_WINDOW
    private var sendCreditReserved = 0L
    private var receiveCredit = if (initiator == YamuxStreamInitiator.LOCAL) {
        config.receiveWindowSize
    } else {
        YamuxProtocol.INITIAL_STREAM_WINDOW
    }
    private var unadvertisedReceiveBytes = 0L
    private var receiveUpdateDispatchPending = false
    private var receiveUpdateInFlight = 0L
    private var reservedInboundBytes = 0L
    private var receiveBudgetAttached = true
    private var localWriteClosed = false
    private var remoteWriteClosed = false
    private var closed = false
    private var terminalFailure: Throwable? = null
    private var permitReleased = false
    private var openTimeoutJob: Job? = null
    private var closeTimeoutJob: Job? = null

    suspend fun read(
        destination: ByteArray,
        offset: Int = 0,
        length: Int = destination.size - offset,
    ): Int {
        requireRange(destination.size, offset, length)
        if (length == 0) return 0
        return readMutex.withLock {
            while (true) {
                var count = 0
                var releaseBudget = 0L
                var scheduleReceiveUpdate = false
                var failure: Throwable? = null
                var eof = false
                var releaseStream = false
                synchronized(stateLock) {
                    if (receiveBuffer.byteCount > 0L) {
                        count = receiveBuffer.read(destination, offset, length)
                        if (receiveBudgetAttached) releaseBudget = count.toLong()
                        unadvertisedReceiveBytes += count.toLong()
                        if (
                            !closed &&
                            !receiveUpdateDispatchPending &&
                            receiveUpdateInFlight == 0L &&
                            unadvertisedReceiveBytes >= config.windowUpdateThreshold
                        ) {
                            receiveUpdateDispatchPending = true
                            scheduleReceiveUpdate = true
                        }
                        releaseStream = closed && receiveBuffer.byteCount == 0L && reservedInboundBytes == 0L
                    } else {
                        failure = terminalFailure
                        eof = failure == null && remoteWriteClosed
                    }
                }
                if (count > 0) {
                    if (releaseBudget > 0L) session.releaseReceiveBytes(releaseBudget)
                    if (scheduleReceiveUpdate) session.signalReceiveUpdate(this)
                    if (releaseStream) session.onStreamClosed(this)
                    return@withLock count
                }
                failure?.let { throw it }
                if (eof) return@withLock -1
                readSignal.receive()
            }
            error("Unreachable")
        }
    }

    suspend fun write(
        source: ByteArray,
        offset: Int = 0,
        length: Int = source.size - offset,
    ) {
        requireRange(source.size, offset, length)
        if (length == 0) return
        writeMutex.withLock {
            var position = offset
            var remaining = length
            while (remaining > 0) {
                val reservation = reserveSendCredit(minOf(remaining, config.outboundDataFrameSize))
                session.sendStreamFrame(
                    stream = this,
                    type = YamuxFrameType.DATA,
                    flags = 0,
                    length = reservation.length,
                    body = source,
                    bodyOffset = position,
                    bodyLength = reservation.length.toInt(),
                    onWriteStarted = reservation::commit,
                    onNotStarted = reservation::rollback,
                )
                position += reservation.length.toInt()
                remaining -= reservation.length.toInt()
            }
        }
    }

    suspend fun closeWrite() {
        writeMutex.withLock {
            synchronized(stateLock) {
                terminalFailure?.let { throw it }
                if (localWriteClosed) return@withLock
            }

            session.sendStreamFrame(
                stream = this,
                type = YamuxFrameType.WINDOW_UPDATE,
                flags = YamuxFlags.FIN,
                length = 0L,
                onWriteStarted = ::commitLocalFin,
            )
        }
    }

    suspend fun reset() {
        val changed = terminate(
            failure = YamuxStreamResetFailure(),
            deferPermitRelease = true,
            tombstoneResetClaimed = true,
        )
        if (changed) {
            session.scheduleResetBestEffort(this).await()?.let { throw it }
        }
    }

    suspend fun awaitClosed() {
        closedSignal.await()
        synchronized(stateLock) {
            terminalFailure?.let { throw it }
        }
    }

    internal suspend fun acceptInbound() {
        synchronized(stateLock) {
            terminalFailure?.let { throw it }
            if (closed) throw YamuxStreamClosedFailure()
            check(initiator == YamuxStreamInitiator.REMOTE)
            check(!accepted && !accepting)
            accepting = true
        }
        try {
            session.sendStreamFrame(
                stream = this,
                type = YamuxFrameType.WINDOW_UPDATE,
                flags = YamuxFlags.ACK,
                length = config.receiveWindowDelta,
                onWriteStarted = ::commitInboundAcceptance,
                onNotStarted = ::rollbackInboundAcceptance,
            )
        } catch (failure: CancellationException) {
            cancelAccept()
            throw failure
        } catch (failure: Error) {
            cancelAccept()
            throw failure
        } catch (failure: Exception) {
            cancelAccept()
            throw failure
        }
    }

    internal fun startOpenTimeout() {
        synchronized(stateLock) {
            if (acknowledged || closed || terminalFailure != null || openTimeoutJob != null) return
            openTimeoutJob = session.launchStreamTimeout(config.streamOpenTimeoutMillis) {
                val expired = synchronized(stateLock) {
                    val shouldExpire = !acknowledged && !closed && terminalFailure == null
                    if (shouldExpire) openTimeoutJob = null
                    shouldExpire
                }
                if (expired) session.onStreamOpenTimeout(this)
            }
        }
    }

    internal fun prepareInboundData(length: Long, flags: Int): DataPreparation {
        var acknowledgedNow = false
        synchronized(stateLock) {
            if (terminalFailure != null || closed) return DataPreparation(false, false)
            if (remoteWriteClosed) {
                val reason = if (flags and YamuxFlags.FIN != 0) {
                    YamuxProtocolError.DUPLICATE_FIN
                } else {
                    YamuxProtocolError.DATA_AFTER_FIN
                }
                throw YamuxProtocolFailure(reason)
            }
            if (flags and YamuxFlags.ACK != 0) acknowledgedNow = acknowledgeLocked()
            if (length > receiveCredit) {
                throw YamuxProtocolFailure(YamuxProtocolError.RECEIVE_WINDOW_EXCEEDED)
            }
            receiveCredit -= length
        }
        return DataPreparation(acknowledgedNow, true)
    }

    internal fun validateInboundResetData(length: Long) {
        synchronized(stateLock) {
            if (terminalFailure != null || closed) return
            if (length > receiveCredit) {
                throw YamuxProtocolFailure(YamuxProtocolError.RECEIVE_WINDOW_EXCEEDED)
            }
        }
    }

    internal fun publishInboundData(payload: YamuxPayloadSegments, hasFin: Boolean): Boolean {
        var published = false
        var notifyClosed = false
        synchronized(stateLock) {
            if (terminalFailure == null && !closed) {
                check(reservedInboundBytes >= payload.length.toLong())
                reservedInboundBytes -= payload.length.toLong()
                receiveBuffer.appendFrame(payload)
                published = true
                if (hasFin) remoteWriteClosed = true
                notifyClosed = completeGracefullyIfReadyLocked()
            }
        }
        if (published && (payload.length > 0 || hasFin)) readSignal.trySend(Unit)
        if (notifyClosed) session.onStreamClosed(this)
        return published
    }

    internal fun registerInboundReservation(length: Long): Boolean = synchronized(stateLock) {
        if (terminalFailure != null || closed) {
            false
        } else {
            reservedInboundBytes += length
            true
        }
    }

    internal fun cancelInboundReservation(length: Long): Long = synchronized(stateLock) {
        val released = minOf(length, reservedInboundBytes)
        reservedInboundBytes -= released
        released
    }

    internal fun receiveWindowUpdate(delta: Long, flags: Int): Boolean {
        var acknowledgedNow = false
        var notifyClosed = false
        synchronized(stateLock) {
            if (terminalFailure != null || closed) return false
            if (flags and YamuxFlags.ACK != 0) acknowledgedNow = acknowledgeLocked()
            if (flags and YamuxFlags.FIN != 0) {
                if (remoteWriteClosed) throw YamuxProtocolFailure(YamuxProtocolError.DUPLICATE_FIN)
                remoteWriteClosed = true
            }
            if (delta > 0L) {
                val totalCredit = sendCreditAvailable + sendCreditReserved
                if (delta > YamuxProtocol.MAX_UINT32 - totalCredit) {
                    throw YamuxProtocolFailure(YamuxProtocolError.SEND_WINDOW_OVERFLOW)
                }
                sendCreditAvailable += delta
            }
            notifyClosed = completeGracefullyIfReadyLocked()
        }
        if (delta > 0L) writeSignal.trySend(Unit)
        if (flags and YamuxFlags.FIN != 0) readSignal.trySend(Unit)
        if (notifyClosed) session.onStreamClosed(this)
        return acknowledgedNow
    }

    internal fun receiveReset() {
        terminate(YamuxStreamResetFailure(), tombstoneResetClaimed = true)
    }

    internal fun failFromSession(failure: Throwable) {
        var released = 0L
        val wasGracefullyClosed = synchronized(stateLock) {
            if (closed && terminalFailure == null) {
                if (receiveBudgetAttached) {
                    released = receiveBuffer.byteCount + reservedInboundBytes
                    receiveBudgetAttached = false
                }
                reservedInboundBytes = 0L
                unadvertisedReceiveBytes = 0L
                receiveUpdateDispatchPending = false
                true
            } else {
                false
            }
        }
        if (wasGracefullyClosed) {
            if (released > 0L) session.releaseReceiveBytes(released)
            session.onStreamClosed(this)
        } else {
            terminate(failure)
        }
    }

    internal fun failLocallyForReset(failure: Throwable): Boolean =
        terminate(failure, deferPermitRelease = true, tombstoneResetClaimed = true)

    internal fun releasePermitOnce(): Boolean = synchronized(stateLock) {
        if (permitReleased) {
            false
        } else {
            permitReleased = true
            true
        }
    }

    internal fun isAcknowledged(): Boolean = synchronized(stateLock) { acknowledged }

    internal fun isClosed(): Boolean = synchronized(stateLock) { closed }

    internal fun bufferedByteCount(): Long = synchronized(stateLock) { receiveBuffer.byteCount }

    internal fun bufferedBlockCount(): Int = synchronized(stateLock) { receiveBuffer.blockCount }

    internal fun takePendingReceiveUpdate(): ReceiveCreditUpdate? = synchronized(stateLock) {
        if (!receiveUpdateDispatchPending) return@synchronized null
        receiveUpdateDispatchPending = false
        if (terminalFailure != null || closed || unadvertisedReceiveBytes == 0L) {
            if (receiveUpdateInFlight == 0L) unadvertisedReceiveBytes = 0L
            return@synchronized null
        }
        check(receiveUpdateInFlight == 0L)
        ReceiveCreditUpdate(unadvertisedReceiveBytes).also { update ->
            receiveUpdateInFlight = update.length
        }
    }

    internal fun sendCreditSnapshot(): YamuxSendCreditSnapshot = synchronized(stateLock) {
        YamuxSendCreditSnapshot(sendCreditAvailable, sendCreditReserved)
    }

    internal fun writeAbort(): YamuxWriteAbort = YamuxWriteAbort(failureSignal) {
        synchronized(stateLock) {
            terminalFailure ?: session.operationFailure() ?: YamuxStreamResetFailure()
        }
    }

    private suspend fun reserveSendCredit(maximum: Int): SendCreditReservation {
        while (true) {
            var reserved = 0L
            synchronized(stateLock) {
                terminalFailure?.let { throw it }
                if (localWriteClosed) throw YamuxStreamClosedFailure()
                if (sendCreditAvailable > 0L) {
                    reserved = minOf(maximum.toLong(), sendCreditAvailable)
                    sendCreditAvailable -= reserved
                    sendCreditReserved += reserved
                }
            }
            if (reserved > 0L) {
                val reservation = SendCreditReservation(reserved)
                session.operationFailure()?.let { failure ->
                    reservation.rollback()
                    throw failure
                }
                return reservation
            }
            val stopped = select {
                writeSignal.onReceive { false }
                failureSignal.onAwait { true }
                session.operationStopSignal().onAwait { true }
            }
            if (stopped) {
                throw synchronized(stateLock) {
                    terminalFailure ?: session.operationFailure() ?: YamuxSessionClosedFailure()
                }
            }
        }
    }

    private fun commitLocalFin() {
        var notifyClosed = false
        var scheduleTimeout = false
        synchronized(stateLock) {
            if (terminalFailure != null || closed || localWriteClosed) return
            localWriteClosed = true
            notifyClosed = completeGracefullyIfReadyLocked()
            scheduleTimeout = !closed && !remoteWriteClosed && closeTimeoutJob == null
        }
        if (notifyClosed) session.onStreamClosed(this)
        if (scheduleTimeout) startCloseTimeout()
    }

    private fun commitInboundAcceptance() {
        synchronized(stateLock) {
            accepting = false
            if (terminalFailure != null || closed) return
            accepted = true
            receiveCredit = minOf(config.receiveWindowSize, receiveCredit + config.receiveWindowDelta)
        }
    }

    private fun rollbackInboundAcceptance() {
        synchronized(stateLock) {
            accepting = false
        }
    }

    private fun acknowledgeLocked(): Boolean {
        if (initiator != YamuxStreamInitiator.LOCAL || acknowledged) {
            throw YamuxProtocolFailure(YamuxProtocolError.DUPLICATE_ACK)
        }
        acknowledged = true
        openTimeoutJob?.cancel()
        openTimeoutJob = null
        return true
    }

    private fun startCloseTimeout() {
        val job = session.launchStreamTimeout(config.streamCloseTimeoutMillis) {
            val failure = YamuxTimeoutFailure(YamuxTimeoutKind.STREAM_CLOSE)
            val expired = synchronized(stateLock) {
                val shouldExpire = localWriteClosed && !remoteWriteClosed && !closed && terminalFailure == null
                if (shouldExpire) closeTimeoutJob = null
                shouldExpire
            }
            if (
                expired && terminate(
                    failure = failure,
                    deferPermitRelease = true,
                    tombstoneResetClaimed = true,
                )
            ) {
                session.scheduleResetBestEffort(this)
            }
        }
        synchronized(stateLock) {
            if (!closed && terminalFailure == null && closeTimeoutJob == null) {
                closeTimeoutJob = job
            } else {
                job.cancel()
            }
        }
    }

    private fun cancelAccept() {
        val changed = terminate(
            failure = YamuxStreamResetFailure(),
            deferPermitRelease = true,
            tombstoneResetClaimed = true,
        )
        if (changed) session.scheduleResetBestEffort(this)
    }

    private fun terminate(
        failure: Throwable,
        deferPermitRelease: Boolean = false,
        tombstoneResetClaimed: Boolean = false,
    ): Boolean {
        var released = 0L
        var changed = false
        synchronized(stateLock) {
            if (!closed && terminalFailure == null) {
                terminalFailure = failure
                closed = true
                val discarded = receiveBuffer.clear() + reservedInboundBytes
                if (receiveBudgetAttached) released = discarded
                receiveBudgetAttached = false
                reservedInboundBytes = 0L
                receiveUpdateDispatchPending = false
                if (receiveUpdateInFlight == 0L) unadvertisedReceiveBytes = 0L
                openTimeoutJob?.cancel()
                closeTimeoutJob?.cancel()
                openTimeoutJob = null
                closeTimeoutJob = null
                failureSignal.complete(Unit)
                closedSignal.complete(Unit)
                changed = true
            }
        }
        if (!changed) return false
        if (released > 0L) session.releaseReceiveBytes(released)
        readSignal.trySend(Unit)
        writeSignal.trySend(Unit)
        session.onStreamClosed(
            stream = this,
            releasePermit = !deferPermitRelease,
            tombstoneResetClaimed = tombstoneResetClaimed,
        )
        return true
    }

    private fun completeGracefullyIfReadyLocked(): Boolean {
        if (closed || terminalFailure != null || !localWriteClosed || !remoteWriteClosed) return false
        closed = true
        openTimeoutJob?.cancel()
        closeTimeoutJob?.cancel()
        openTimeoutJob = null
        closeTimeoutJob = null
        closedSignal.complete(Unit)
        return receiveBuffer.byteCount == 0L && reservedInboundBytes == 0L
    }

    private fun requireRange(size: Int, offset: Int, length: Int) {
        require(offset >= 0 && length >= 0 && offset <= size - length)
    }

    override fun toString(): String = synchronized(stateLock) {
        "YamuxStream(streamId=$streamId, initiator=$initiator, accepted=$accepted, " +
            "acknowledged=$acknowledged, localWriteClosed=$localWriteClosed, " +
            "remoteWriteClosed=$remoteWriteClosed, closed=$closed, " +
            "bufferedBytes=${receiveBuffer.byteCount}, reservedInboundBytes=$reservedInboundBytes, " +
            "failurePresent=${terminalFailure != null})"
    }

    private inner class SendCreditReservation(val length: Long) {
        private val resolved = AtomicBoolean(false)

        fun commit() {
            if (!resolved.compareAndSet(false, true)) return
            synchronized(stateLock) {
                val committed = minOf(length, sendCreditReserved)
                sendCreditReserved -= committed
            }
        }

        fun rollback() {
            if (!resolved.compareAndSet(false, true)) return
            var restored = 0L
            synchronized(stateLock) {
                restored = minOf(length, sendCreditReserved)
                sendCreditReserved -= restored
                sendCreditAvailable = minOf(
                    YamuxProtocol.MAX_UINT32,
                    sendCreditAvailable + restored,
                )
            }
            if (restored > 0L) writeSignal.trySend(Unit)
        }
    }

    internal inner class ReceiveCreditUpdate(val length: Long) {
        private val resolved = AtomicBoolean(false)

        fun commit() {
            if (!resolved.compareAndSet(false, true)) return
            var scheduleNext = false
            synchronized(stateLock) {
                if (receiveUpdateInFlight == length) {
                    receiveUpdateInFlight = 0L
                    unadvertisedReceiveBytes = (unadvertisedReceiveBytes - length).coerceAtLeast(0L)
                    receiveCredit = minOf(config.receiveWindowSize, receiveCredit + length)
                    if (
                        terminalFailure == null &&
                        !closed &&
                        unadvertisedReceiveBytes >= config.windowUpdateThreshold
                    ) {
                        receiveUpdateDispatchPending = true
                        scheduleNext = true
                    }
                }
            }
            if (scheduleNext) session.signalReceiveUpdate(this@YamuxStream)
        }

        fun rollback() {
            if (!resolved.compareAndSet(false, true)) return
            var retry = false
            synchronized(stateLock) {
                if (receiveUpdateInFlight == length) {
                    receiveUpdateInFlight = 0L
                    if (terminalFailure == null && !closed) {
                        receiveUpdateDispatchPending = true
                        retry = true
                    } else {
                        unadvertisedReceiveBytes = 0L
                    }
                }
            }
            if (retry) session.signalReceiveUpdate(this@YamuxStream)
        }
    }

    internal data class DataPreparation(
        val acknowledgedNow: Boolean,
        val shouldRead: Boolean,
    )
}

internal data class YamuxSendCreditSnapshot(
    val available: Long,
    val reserved: Long,
)
