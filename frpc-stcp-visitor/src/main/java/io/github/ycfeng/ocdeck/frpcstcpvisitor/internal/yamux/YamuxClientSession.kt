package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux

import java.util.ArrayDeque
import java.util.LinkedHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield

internal sealed interface YamuxTermination {
    data object Closed : YamuxTermination
}

internal data class YamuxDiagnostics(
    val activeStreams: Int,
    val activeStreamPermits: Int,
    val pendingOpenCalls: Int,
    val pendingOpenAcknowledgements: Int,
    val acceptBacklog: Int,
    val acceptWaiters: Int,
    val pendingReceiveUpdates: Int,
    val pendingResetRequests: Int,
    val tombstones: Int,
    val pendingPing: Boolean,
    val writerPendingFrames: Int,
    val writerPendingBytes: Long,
    val receiveBytes: Long,
    val activeChildJobs: Int,
    val ending: Boolean,
    val terminated: Boolean,
)

internal object YamuxClientSessionFactory {
    fun create(
        transport: YamuxPhysicalTransport,
        parentScope: CoroutineScope,
        config: YamuxConfig = YamuxConfig(),
    ): YamuxClientSession = YamuxClientSession(transport, parentScope, config)
}

internal fun createYamuxClientSession(
    transport: YamuxPhysicalTransport,
    parentScope: CoroutineScope,
    config: YamuxConfig = YamuxConfig(),
): YamuxClientSession = YamuxClientSessionFactory.create(transport, parentScope, config)

internal class YamuxClientSession(
    private val transport: YamuxPhysicalTransport,
    parentScope: CoroutineScope,
    internal val config: YamuxConfig = YamuxConfig(),
) {
    private val stateLock = Any()
    private val endingLock = Any()
    private val sessionJob = SupervisorJob()
    private val scope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) +
            sessionJob +
            CoroutineName("YamuxClientSession"),
    )
    private val finalizerScope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) + CoroutineName("YamuxClientSessionFinalizer"),
    )
    private val streamPermits = Channel<Unit>(config.maxStreams)
    private val activePermitCount = AtomicInteger(0)
    private val pendingOpenCallCount = AtomicInteger(0)
    private val pendingResetRequestCount = AtomicInteger(0)
    private val totalReceiveBytes = AtomicLong(0L)
    private val nextPingId = AtomicLong(0L)
    private val pingMutex = Mutex()
    private val receiveUpdateSignal = Channel<Unit>(Channel.CONFLATED)
    private val resetRequestSignal = Channel<Unit>(Channel.CONFLATED)
    private val newStreamsClosedSignal = CompletableDeferred<Unit>()
    private val operationStopSignal = CompletableDeferred<Unit>()
    private val terminationDoneSignal = CompletableDeferred<Unit>()
    private val forcedTerminationSignal = CompletableDeferred<Unit>()
    private val operationFailureRef = AtomicReference<Throwable?>()
    private val finalFailureRef = AtomicReference<Throwable?>()
    private val terminationRef = AtomicReference<YamuxTermination?>()

    private val streams = LinkedHashMap<Long, YamuxStream>()
    private val pendingOpenAcknowledgements = HashSet<Long>()
    private val acceptBacklog = ArrayDeque<YamuxStream>()
    private val acceptWaiters = ArrayDeque<AcceptWaiter>()
    private val receiveUpdateQueue = ArrayDeque<YamuxStream>()
    private val resetRequestQueue = ArrayDeque<ResetRequest>()
    private val tombstones = LinkedHashMap<Long, Tombstone>()
    private var nextLocalStreamId = config.initialLocalStreamId
    private var normalGoAwayPhase = NormalGoAwayPhase.NOT_SENT
    private var normalGoAwayAttempt: NormalGoAwayAttempt? = null
    private var remoteGoAway = false
    private var pendingPing: PendingPing? = null

    @Volatile
    private var ending = false

    private val writer = YamuxFrameWriter(
        transport = transport,
        config = config,
        scope = scope,
        onFatalFailure = ::terminateNow,
        onCallbackFailure = ::terminateFromCallback,
    )
    private var parentCompletionHandle: DisposableHandle? = null
    private var readerJob: Job? = null
    private var keepAliveJob: Job? = null
    private var receiveUpdateJob: Job? = null
    private var resetDispatcherJob: Job? = null

    init {
        repeat(config.maxStreams) {
            check(streamPermits.trySend(Unit).isSuccess)
        }
        readerJob = scope.launch { runReaderJob() }
        receiveUpdateJob = scope.launch { runReceiveUpdateDispatcher() }
        resetDispatcherJob = scope.launch { runResetDispatcher() }
        keepAliveJob = if (config.keepAliveEnabled && !ending) {
            scope.launch { runKeepAlive() }
        } else {
            null
        }
        val completionHandle = parentScope.coroutineContext[Job]?.invokeOnCompletion { cause ->
            when (cause) {
                is CancellationException -> terminateNow(cause)
                is Error -> terminateNow(cause)
                else -> terminateNow(YamuxSessionClosedFailure())
            }
        }
        synchronized(endingLock) {
            if (ending) {
                completionHandle?.dispose()
            } else {
                parentCompletionHandle = completionHandle
            }
        }
    }

    suspend fun openStream(): YamuxStream {
        val pending = pendingOpenCallCount.incrementAndGet()
        if (pending > config.maxPendingOpens) {
            pendingOpenCallCount.decrementAndGet()
            throw YamuxResourceLimitFailure(YamuxResourceKind.PENDING_OPENS)
        }
        var permitAcquired = false
        var stream: YamuxStream? = null
        try {
            acquireOpenPermit()
            permitAcquired = true
            stream = synchronized(stateLock) {
                throwIfEndingLocked()
                newStreamFailureLocked()?.let { throw it }
                val streamId = allocateLocalStreamIdLocked()
                YamuxStream(
                    streamId = streamId,
                    initiator = YamuxStreamInitiator.LOCAL,
                    session = this,
                    config = config,
                ).also { created ->
                    streams[streamId] = created
                    pendingOpenAcknowledgements += streamId
                    activePermitCount.incrementAndGet()
                }
            }
            permitAcquired = false
            try {
                sendStreamFrame(
                    stream = stream,
                    type = YamuxFrameType.WINDOW_UPDATE,
                    flags = YamuxFlags.SYN,
                    length = config.receiveWindowDelta,
                )
            } catch (failure: CancellationException) {
                cancelOpen(stream)
                throw failure
            } catch (failure: Error) {
                cancelOpen(stream)
                throw failure
            } catch (failure: Exception) {
                cancelOpen(stream)
                throw failure
            }
            stream.startOpenTimeout()
            return stream
        } finally {
            if (permitAcquired) returnPermitToken()
            pendingOpenCallCount.decrementAndGet()
        }
    }

    suspend fun acceptStream(): YamuxStream {
        var waiter: AcceptWaiter? = null
        var stream = synchronized(stateLock) {
            throwIfEndingLocked()
            if (acceptBacklog.isEmpty()) {
                if (acceptWaiters.size >= config.maxStreams) {
                    throw YamuxResourceLimitFailure(YamuxResourceKind.ACCEPT_BACKLOG)
                }
                AcceptWaiter().also {
                    acceptWaiters.addLast(it)
                    waiter = it
                }
                null
            } else {
                acceptBacklog.removeFirst()
            }
        }
        if (stream == null) {
            val registered = checkNotNull(waiter)
            val stopped = try {
                select<Boolean> {
                    registered.signal.onAwait { false }
                    operationStopSignal.onAwait { true }
                }
            } catch (failure: CancellationException) {
                cancelAcceptWaiter(registered)
                throw failure
            } catch (failure: Error) {
                cancelAcceptWaiter(registered)
                throw failure
            } catch (failure: Exception) {
                cancelAcceptWaiter(registered)
                throw failure
            }
            if (stopped) {
                cancelAcceptWaiter(registered)
                throw requiredOperationFailure()
            }
            stream = synchronized(stateLock) {
                check(!registered.cancelled)
                registered.consumed = true
                checkNotNull(registered.assigned).also { registered.assigned = null }
            }
        }
        val acceptedStream = checkNotNull(stream)
        acceptedStream.acceptInbound()
        return acceptedStream
    }

    suspend fun ping(): Long = pingInternal(YamuxTimeoutKind.PING)

    suspend fun sendGoAway(code: Long = YamuxGoAwayCode.NORMAL.wireValue) {
        val goAwayCode = YamuxGoAwayCode.fromWireValue(code)
            ?: throw YamuxProtocolFailure(YamuxProtocolError.INVALID_GOAWAY_CODE)
        if (goAwayCode == YamuxGoAwayCode.NORMAL) {
            sendNormalGoAway()
            return
        }

        val failure = YamuxLocalGoAwayFailure(goAwayCode)
        terminateWithGoAway(failure, goAwayCode)
    }

    suspend fun sendGoAway(code: YamuxGoAwayCode) {
        sendGoAway(code.wireValue)
    }

    private suspend fun sendNormalGoAway() {
        while (true) {
            var ownsAttempt = false
            val attempt = synchronized(stateLock) {
                throwIfEndingLocked()
                normalGoAwayAttempt ?: if (normalGoAwayPhase == NormalGoAwayPhase.SENT) {
                    null
                } else {
                    NormalGoAwayAttempt().also {
                        normalGoAwayPhase = NormalGoAwayPhase.SENDING
                        normalGoAwayAttempt = it
                        ownsAttempt = true
                    }
                }
            } ?: return

            if (ownsAttempt) {
                val writerJob = scope.launch(start = CoroutineStart.UNDISPATCHED) {
                    executeNormalGoAway(attempt)
                }
                attempt.attachWriterJob(writerJob)
            }

            val result = try {
                attempt.result.await()
            } catch (failure: CancellationException) {
                if (ownsAttempt) attempt.cancelWriter(failure)
                throw preferredNormalGoAwayFailure(attempt, failure)
            }
            when (result) {
                NormalGoAwayResult.Success -> return
                NormalGoAwayResult.Retry -> continue
                is NormalGoAwayResult.Failure -> throw result.failure
            }
        }
    }

    private suspend fun executeNormalGoAway(attempt: NormalGoAwayAttempt) {
        try {
            writer.write(
                header = YamuxFrameHeader(
                    type = YamuxFrameType.GOAWAY,
                    flags = 0,
                    streamId = 0L,
                    length = YamuxGoAwayCode.NORMAL.wireValue,
                ),
                onWriteStarted = { commitNormalGoAway(attempt) },
                onNotStarted = { rollbackNormalGoAway(attempt) },
            )
            completeNormalGoAway(attempt, NormalGoAwayResult.Success)
        } catch (failure: CancellationException) {
            resolveNormalGoAwayOwnerFailure(attempt, failure)
        } catch (failure: Error) {
            terminateNow(failure)
            resolveNormalGoAwayOwnerFailure(attempt, failure)
        } catch (failure: Exception) {
            if (!ending) terminateNow(safeFailure(failure, YamuxTransportOperation.WRITE))
            resolveNormalGoAwayOwnerFailure(attempt, failure)
        } finally {
            attempt.completePhysicalWrite()
        }
    }

    private fun commitNormalGoAway(attempt: NormalGoAwayAttempt) {
        attempt.completeOwnership(owned = true)
        synchronized(stateLock) {
            if (
                normalGoAwayAttempt === attempt &&
                normalGoAwayPhase == NormalGoAwayPhase.SENDING
            ) {
                normalGoAwayPhase = NormalGoAwayPhase.SENT
            }
        }
    }

    private fun rollbackNormalGoAway(attempt: NormalGoAwayAttempt) {
        attempt.completeOwnership(owned = false)
        synchronized(stateLock) {
            if (
                normalGoAwayAttempt !== attempt ||
                normalGoAwayPhase != NormalGoAwayPhase.SENDING
            ) {
                return@synchronized
            }
            normalGoAwayAttempt = null
            normalGoAwayPhase = NormalGoAwayPhase.NOT_SENT
        }
    }

    private fun completeNormalGoAway(
        attempt: NormalGoAwayAttempt,
        result: NormalGoAwayResult,
    ) {
        if (result == NormalGoAwayResult.Success) attempt.completeOwnership(owned = true)
        val shouldComplete = synchronized(stateLock) {
            if (normalGoAwayAttempt !== attempt) {
                false
            } else {
                normalGoAwayAttempt = null
                if (
                    result != NormalGoAwayResult.Success &&
                    normalGoAwayPhase == NormalGoAwayPhase.SENDING
                ) {
                    normalGoAwayPhase = NormalGoAwayPhase.NOT_SENT
                }
                true
            }
        }
        if (shouldComplete) attempt.complete(result)
    }

    private fun resolveNormalGoAwayOwnerFailure(
        attempt: NormalGoAwayAttempt,
        failure: Throwable,
    ) {
        var ownership: Boolean? = null
        val result = synchronized(stateLock) {
            if (normalGoAwayAttempt !== attempt) {
                null
            } else {
                normalGoAwayAttempt = null
                when (normalGoAwayPhase) {
                    NormalGoAwayPhase.NOT_SENT -> {
                        ownership = false
                        NormalGoAwayResult.Retry
                    }
                    NormalGoAwayPhase.SENDING -> {
                        ownership = false
                        normalGoAwayPhase = NormalGoAwayPhase.NOT_SENT
                        operationFailure()?.let(NormalGoAwayResult::Failure)
                            ?: if (failure is CancellationException) {
                                NormalGoAwayResult.Retry
                            } else {
                                NormalGoAwayResult.Failure(failure)
                            }
                    }

                    NormalGoAwayPhase.SENT -> {
                        ownership = true
                        val endingFailure = operationFailure()
                        when {
                            endingFailure != null -> NormalGoAwayResult.Failure(endingFailure)
                            failure is CancellationException -> NormalGoAwayResult.Success
                            else -> NormalGoAwayResult.Failure(failure)
                        }
                    }
                }
            }
        }
        ownership?.let(attempt::completeOwnership)
        val fallback = if (result == null && !attempt.result.isCompleted) {
            operationFailure()?.let(NormalGoAwayResult::Failure)
                ?: if (failure is CancellationException) {
                    NormalGoAwayResult.Retry
                } else {
                    NormalGoAwayResult.Failure(failure)
                }
        } else {
            null
        }
        (result ?: fallback)?.let(attempt::complete)
    }

    private fun preferredNormalGoAwayFailure(
        attempt: NormalGoAwayAttempt,
        failure: Throwable,
    ): Throwable = checkNotNull(
        preferYamuxFailure(
            preferYamuxFailure(failure, attempt.failure()),
            operationFailure(),
        ),
    )

    suspend fun close() {
        beginEnding(
            operationFailure = YamuxSessionClosedFailure(),
            finalFailure = null,
            termination = YamuxTermination.Closed,
        )
        terminationDoneSignal.await()
        finalFailureRef.get()?.let { throw it }
    }

    suspend fun awaitTermination(): YamuxTermination {
        terminationDoneSignal.await()
        finalFailureRef.get()?.let { throw it }
        return terminationRef.get() ?: YamuxTermination.Closed
    }

    internal fun forceTerminationAfterPhysicalClose() {
        if (!ending) {
            val failure = YamuxSessionClosedFailure()
            beginEnding(failure, finalFailure = null, termination = YamuxTermination.Closed)
        }
        forcedTerminationSignal.complete(Unit)
    }

    fun diagnostics(): YamuxDiagnostics {
        val state = synchronized(stateLock) {
            DiagnosticsState(
                activeStreams = streams.size,
                pendingOpenAcknowledgements = pendingOpenAcknowledgements.size,
                acceptBacklog = acceptBacklog.size,
                acceptWaiters = acceptWaiters.size,
                pendingReceiveUpdates = receiveUpdateQueue.size,
                tombstones = tombstones.size,
                pendingPing = pendingPing != null,
            )
        }
        return YamuxDiagnostics(
            activeStreams = state.activeStreams,
            activeStreamPermits = activePermitCount.get(),
            pendingOpenCalls = pendingOpenCallCount.get(),
            pendingOpenAcknowledgements = state.pendingOpenAcknowledgements,
            acceptBacklog = state.acceptBacklog,
            acceptWaiters = state.acceptWaiters,
            pendingReceiveUpdates = state.pendingReceiveUpdates,
            pendingResetRequests = pendingResetRequestCount.get(),
            tombstones = state.tombstones,
            pendingPing = state.pendingPing,
            writerPendingFrames = writer.pendingFrameCount,
            writerPendingBytes = writer.pendingByteCount,
            receiveBytes = totalReceiveBytes.get(),
            activeChildJobs = sessionJob.children.count { !it.isCompleted },
            ending = ending,
            terminated = terminationDoneSignal.isCompleted,
        )
    }

    internal suspend fun sendStreamFrame(
        stream: YamuxStream,
        type: YamuxFrameType,
        flags: Int,
        length: Long,
        body: ByteArray? = null,
        bodyOffset: Int = 0,
        bodyLength: Int = body?.size?.minus(bodyOffset) ?: 0,
        onWriteStarted: () -> Unit = {},
        onNotStarted: () -> Unit = {},
    ) {
        operationFailure()?.let { failure ->
            val callbackFailure = invokeYamuxCallback(onNotStarted)
            if (callbackFailure != null) terminateFromCallback(callbackFailure)
            throw checkNotNull(preferYamuxFailure(failure, callbackFailure))
        }
        writer.write(
            header = YamuxFrameHeader(
                type = type,
                flags = flags,
                streamId = stream.streamId,
                length = length,
            ),
            body = body,
            bodyOffset = bodyOffset,
            bodyLength = bodyLength,
            abort = stream.writeAbort(),
            onWriteStarted = onWriteStarted,
            onNotStarted = onNotStarted,
        )
    }

    internal fun signalReceiveUpdate(stream: YamuxStream) {
        val enqueued = synchronized(stateLock) {
            if (ending || streams[stream.streamId] !== stream || receiveUpdateQueue.contains(stream)) {
                false
            } else {
                check(receiveUpdateQueue.size < config.maxStreams)
                receiveUpdateQueue.addLast(stream)
                true
            }
        }
        if (enqueued) receiveUpdateSignal.trySend(Unit)
    }

    internal fun operationStopSignal(): Deferred<Unit> = operationStopSignal

    internal suspend fun awaitResetDispatcherStopped() {
        resetDispatcherJob?.join()
    }

    internal fun scheduleResetBestEffort(stream: YamuxStream): Deferred<Throwable?> {
        val request = ResetRequest(stream)
        val admission = synchronized(stateLock) {
            when {
                ending -> ResetAdmission.STOPPED
                pendingResetRequestCount.get() >= config.maxStreams -> ResetAdmission.OVERFLOW
                else -> {
                    request.counted.set(true)
                    pendingResetRequestCount.incrementAndGet()
                    resetRequestQueue.addLast(request)
                    ResetAdmission.ADMITTED
                }
            }
        }
        when (admission) {
            ResetAdmission.ADMITTED -> resetRequestSignal.trySend(Unit)
            ResetAdmission.STOPPED -> finishResetRequest(request, failure = null)
            ResetAdmission.OVERFLOW -> {
                val failure = YamuxResourceLimitFailure(YamuxResourceKind.WRITER_FRAMES)
                finishResetRequest(request, failure)
                terminateNow(failure)
            }
        }
        return request.completion
    }

    internal fun launchStreamTimeout(
        timeoutMillis: Long,
        block: suspend () -> Unit,
    ): Job = scope.launch {
        delay(timeoutMillis)
        block()
    }

    internal suspend fun onStreamOpenTimeout(stream: YamuxStream) {
        synchronized(stateLock) {
            if (streams[stream.streamId] !== stream) return
        }
        terminateWithGoAway(
            YamuxTimeoutFailure(YamuxTimeoutKind.STREAM_OPEN),
            YamuxGoAwayCode.INTERNAL_ERROR,
        )
    }

    internal fun onStreamClosed(
        stream: YamuxStream,
        releasePermit: Boolean = true,
        tombstoneResetClaimed: Boolean = false,
    ) {
        synchronized(stateLock) {
            if (streams[stream.streamId] === stream) streams.remove(stream.streamId)
            pendingOpenAcknowledgements.remove(stream.streamId)
            acceptBacklog.remove(stream)
            receiveUpdateQueue.remove(stream)
            if (!ending) addTombstoneLocked(stream.streamId, tombstoneResetClaimed)
        }
        if (releasePermit && stream.releasePermitOnce()) releaseActivePermitToken()
    }

    internal fun releaseReceiveBytes(count: Long) {
        if (count == 0L) return
        while (true) {
            val current = totalReceiveBytes.get()
            val updated = (current - count).coerceAtLeast(0L)
            if (totalReceiveBytes.compareAndSet(current, updated)) return
        }
    }

    internal fun operationFailure(): Throwable? = if (ending) operationFailureRef.get() else null

    private suspend fun runReceiveUpdateDispatcher() {
        while (!ending) {
            val stopped = select {
                receiveUpdateSignal.onReceive { false }
                operationStopSignal.onAwait { true }
            }
            if (stopped) return
            while (!ending) {
                val stream = synchronized(stateLock) {
                    if (receiveUpdateQueue.isEmpty()) null else receiveUpdateQueue.removeFirst()
                } ?: break
                val update = stream.takePendingReceiveUpdate() ?: continue
                try {
                    writer.enqueueControl(
                        header = YamuxFrameHeader(
                            type = YamuxFrameType.WINDOW_UPDATE,
                            flags = 0,
                            streamId = stream.streamId,
                            length = update.length,
                        ),
                        abort = stream.writeAbort(),
                        onWriteStarted = update::commit,
                        onNotStarted = update::rollback,
                    )
                } catch (failure: CancellationException) {
                    if (!ending && !stream.isClosed()) terminateNow(failure)
                    if (ending) return
                } catch (failure: Error) {
                    terminateNow(failure)
                    return
                } catch (failure: Exception) {
                    if (!ending && !stream.isClosed()) {
                        terminateNow(safeFailure(failure, YamuxTransportOperation.WRITE))
                        return
                    }
                }
            }
        }
    }

    private suspend fun runResetDispatcher() {
        try {
            while (true) {
                val request = synchronized(stateLock) {
                    if (resetRequestQueue.isEmpty()) null else resetRequestQueue.removeFirst()
                }
                if (request != null) {
                    processResetRequest(request)
                    continue
                }
                if (ending) return
                select {
                    operationStopSignal.onAwait { }
                    resetRequestSignal.onReceive { }
                }
            }
        } catch (failure: CancellationException) {
            if (!ending) terminateNow(failure)
        } catch (failure: Error) {
            terminateNow(failure)
        } finally {
            drainResetRequests()
        }
    }

    private suspend fun processResetRequest(request: ResetRequest) {
        var resultFailure: Throwable? = null
        try {
            val remainingMillis = config.writeTimeoutMillis - request.queuedAt.elapsedNow().inWholeMilliseconds
            if (!ending && remainingMillis > 0L) {
                withTimeoutOrNull(remainingMillis) {
                    writer.writeBestEffort(resetHeader(request.stream.streamId))
                }
            }
        } catch (failure: CancellationException) {
            if (!ending) {
                resultFailure = failure
                terminateNow(failure)
            }
        } catch (failure: Error) {
            resultFailure = failure
            terminateNow(failure)
        } catch (failure: Exception) {
            if (!ending) terminateNow(safeFailure(failure, YamuxTransportOperation.WRITE))
        } finally {
            finishResetRequest(request, resultFailure)
        }
    }

    private fun drainResetRequests() {
        val requests = synchronized(stateLock) {
            resetRequestQueue.toList().also { resetRequestQueue.clear() }
        }
        requests.forEach { finishResetRequest(it, failure = null) }
    }

    private fun finishResetRequest(request: ResetRequest, failure: Throwable?) {
        if (!request.finished.compareAndSet(false, true)) return
        if (request.counted.compareAndSet(true, false)) {
            pendingResetRequestCount.updateAndGet { current -> (current - 1).coerceAtLeast(0) }
        }
        if (request.stream.releasePermitOnce()) releaseActivePermitToken()
        request.completion.complete(failure)
    }

    private suspend fun runReaderJob() {
        try {
            transport.runReaderLoop(::runReader)
        } catch (failure: CancellationException) {
            if (!ending) terminateNow(failure)
        } catch (failure: Error) {
            terminateNow(failure)
        } catch (failure: YamuxProtocolFailure) {
            if (!ending) terminateWithGoAway(failure, YamuxGoAwayCode.PROTOCOL_ERROR)
        } catch (failure: YamuxTruncatedFrameFailure) {
            if (!ending) terminateWithGoAway(failure, YamuxGoAwayCode.PROTOCOL_ERROR)
        } catch (failure: YamuxReceiveBudgetFailure) {
            if (!ending) terminateWithGoAway(failure, YamuxGoAwayCode.INTERNAL_ERROR)
        } catch (failure: YamuxFailure) {
            if (!ending) terminateNow(failure)
        } catch (failure: Exception) {
            if (!ending) terminateNow(safeFailure(failure, YamuxTransportOperation.READ))
        }
    }

    private suspend fun runReader() {
        val headerScratch = ByteArray(YamuxProtocol.HEADER_SIZE)
        while (!ending) {
            val header = YamuxFrameCodec.readHeaderOrNull(transport, headerScratch)
                ?: throw YamuxTransportFailure(YamuxTransportOperation.READ)
            handleFrame(header)
        }
    }

    private suspend fun handleFrame(header: YamuxFrameHeader) {
        when (header.type) {
            YamuxFrameType.PING -> handlePing(header)
            YamuxFrameType.GOAWAY -> handleGoAway(header)
            YamuxFrameType.DATA,
            YamuxFrameType.WINDOW_UPDATE,
            -> handleStreamFrame(header)
        }
    }

    private suspend fun handlePing(header: YamuxFrameHeader) {
        if (header.flags == YamuxFlags.SYN) {
            writer.write(
                YamuxFrameHeader(
                    type = YamuxFrameType.PING,
                    flags = YamuxFlags.ACK,
                    streamId = 0L,
                    length = header.length,
                ),
            )
            return
        }

        val waiter = synchronized(stateLock) {
            val current = pendingPing
            when {
                current != null && current.id == header.length -> {
                    pendingPing = null
                    current
                }

                else -> null
            }
        }
        waiter?.signal?.complete(Unit)
    }

    private fun handleGoAway(header: YamuxFrameHeader) {
        val code = YamuxGoAwayCode.fromWireValue(header.length)
            ?: throw YamuxProtocolFailure(YamuxProtocolError.INVALID_GOAWAY_CODE)
        if (code == YamuxGoAwayCode.NORMAL) {
            synchronized(stateLock) {
                remoteGoAway = true
                newStreamsClosedSignal.complete(Unit)
            }
        } else {
            terminateNow(YamuxRemoteGoAwayFailure(code))
        }
    }

    private suspend fun handleStreamFrame(header: YamuxFrameHeader) {
        val resolution = resolveStream(header)
        when (resolution) {
            StreamResolution.Stopping -> return

            is StreamResolution.Reject -> {
                discardRejectedStreamBody(header)
                if (resolution.sendReset) sendResetFrame(header.streamId)
                return
            }

            is StreamResolution.Tombstone -> {
                discardDataBody(header)
                if (resolution.sendReset) sendResetFrame(header.streamId)
                return
            }

            is StreamResolution.Active -> handleActiveStreamFrame(resolution.stream, header)
        }
    }

    private fun resolveStream(header: YamuxFrameHeader): StreamResolution {
        val hasSyn = header.flags and YamuxFlags.SYN != 0
        if (!hasSyn) {
            return synchronized(stateLock) {
                if (ending) return@synchronized StreamResolution.Stopping
                streams[header.streamId]?.let(StreamResolution::Active)
                    ?: StreamResolution.Tombstone(
                        sendReset = claimTombstoneResetLocked(
                            streamId = header.streamId,
                            inboundReset = header.flags and YamuxFlags.RST != 0,
                        ),
                    )
            }
        }

        if (header.streamId and 1L != 0L) {
            throw YamuxProtocolFailure(YamuxProtocolError.INVALID_STREAM_PARITY)
        }
        var created: YamuxStream? = null
        var reject = false
        var sendResetOnReject = false
        var waiterToWake: AcceptWaiter? = null
        synchronized(stateLock) {
            if (ending) return StreamResolution.Stopping
            if (streams.containsKey(header.streamId)) {
                throw YamuxProtocolFailure(YamuxProtocolError.DUPLICATE_SYN)
            }
            if (tombstones.containsKey(header.streamId)) {
                throw YamuxProtocolFailure(YamuxProtocolError.STREAM_ID_REUSE)
            }
            val permit = streamPermits.tryReceive().getOrNull()
            if (
                normalGoAwayPhase == NormalGoAwayPhase.SENT ||
                (acceptWaiters.isEmpty() && acceptBacklog.size >= config.acceptBacklog) ||
                permit == null
            ) {
                if (permit != null) check(streamPermits.trySend(Unit).isSuccess)
                sendResetOnReject = header.flags and YamuxFlags.RST == 0
                addTombstoneLocked(header.streamId, resetClaimed = true)
                reject = true
            } else {
                activePermitCount.incrementAndGet()
                created = YamuxStream(
                    streamId = header.streamId,
                    initiator = YamuxStreamInitiator.REMOTE,
                    session = this,
                    config = config,
                ).also { stream ->
                    streams[header.streamId] = stream
                    val waiter = if (acceptWaiters.isEmpty()) null else acceptWaiters.removeFirst()
                    if (waiter == null) {
                        acceptBacklog.addLast(stream)
                    } else {
                        waiter.assigned = stream
                        waiterToWake = waiter
                    }
                }
            }
        }
        if (reject) return StreamResolution.Reject(sendResetOnReject)
        waiterToWake?.signal?.complete(Unit)
        return StreamResolution.Active(checkNotNull(created))
    }

    private suspend fun handleActiveStreamFrame(stream: YamuxStream, header: YamuxFrameHeader) {
        if (header.flags and YamuxFlags.RST != 0) {
            if (header.type == YamuxFrameType.DATA) {
                handleResetDataFrame(stream, header)
            } else {
                stream.receiveReset()
            }
            return
        }
        when (header.type) {
            YamuxFrameType.DATA -> handleDataFrame(stream, header)
            YamuxFrameType.WINDOW_UPDATE -> {
                val acknowledged = stream.receiveWindowUpdate(header.length, header.flags)
                if (acknowledged) removePendingAcknowledgement(stream)
            }

            else -> error("Session frames are handled separately")
        }
    }

    private suspend fun handleDataFrame(stream: YamuxStream, header: YamuxFrameHeader) {
        if (header.length > config.maxDataFrameSize.toLong()) {
            throw YamuxProtocolFailure(YamuxProtocolError.DATA_FRAME_TOO_LARGE)
        }
        val preparation = stream.prepareInboundData(header.length, header.flags)
        if (preparation.acknowledgedNow) removePendingAcknowledgement(stream)
        if (!preparation.shouldRead) {
            discardDataBody(header)
            return
        }
        if (!reserveReceiveBytes(header.length)) throw YamuxReceiveBudgetFailure()
        if (!stream.registerInboundReservation(header.length)) {
            releaseReceiveBytes(header.length)
            discardDataBody(header)
            return
        }
        val payload = try {
            YamuxFrameCodec.readDataSegments(
                transport = transport,
                length = header.length,
                maximumLength = config.maxDataFrameSize,
                segmentSize = config.receiveSegmentSize,
            )
        } catch (failure: CancellationException) {
            releaseCancelledReservation(stream, header.length)
            throw failure
        } catch (failure: Error) {
            releaseCancelledReservation(stream, header.length)
            throw failure
        } catch (failure: Exception) {
            releaseCancelledReservation(stream, header.length)
            throw failure
        }
        if (!stream.publishInboundData(payload, header.flags and YamuxFlags.FIN != 0)) {
            releaseCancelledReservation(stream, header.length)
        }
    }

    private suspend fun handleResetDataFrame(stream: YamuxStream, header: YamuxFrameHeader) {
        if (header.length > config.maxDataFrameSize.toLong()) {
            throw YamuxProtocolFailure(YamuxProtocolError.DATA_FRAME_TOO_LARGE)
        }
        stream.validateInboundResetData(header.length)
        if (!reserveReceiveBytes(header.length)) throw YamuxReceiveBudgetFailure()
        try {
            stream.receiveReset()
            discardDataBody(header)
        } finally {
            releaseReceiveBytes(header.length)
        }
    }

    private suspend fun discardDataBody(header: YamuxFrameHeader) {
        if (header.type != YamuxFrameType.DATA) return
        YamuxFrameCodec.discardData(
            transport = transport,
            length = header.length,
            maximumLength = config.maxDataFrameSize,
            segmentSize = config.receiveSegmentSize,
        )
    }

    private suspend fun discardRejectedStreamBody(header: YamuxFrameHeader) {
        if (header.type != YamuxFrameType.DATA) return
        if (header.length > YamuxProtocol.INITIAL_STREAM_WINDOW) {
            throw YamuxProtocolFailure(YamuxProtocolError.RECEIVE_WINDOW_EXCEEDED)
        }
        YamuxFrameCodec.discardData(
            transport = transport,
            length = header.length,
            maximumLength = YamuxProtocol.INITIAL_STREAM_WINDOW.toInt(),
            segmentSize = config.receiveSegmentSize,
        )
    }

    private suspend fun sendResetFrame(streamId: Long) {
        writer.write(resetHeader(streamId))
    }

    private fun resetHeader(streamId: Long): YamuxFrameHeader = YamuxFrameHeader(
        type = YamuxFrameType.WINDOW_UPDATE,
        flags = YamuxFlags.RST,
        streamId = streamId,
        length = 0L,
    )

    private suspend fun pingInternal(timeoutKind: YamuxTimeoutKind): Long = pingMutex.withLock {
        operationFailure()?.let { throw it }
        val id = nextPingId.getAndUpdate { current ->
            if (current == YamuxProtocol.MAX_UINT32) 0L else current + 1L
        }
        val waiter = PendingPing(id)
        synchronized(stateLock) {
            throwIfEndingLocked()
            check(pendingPing == null)
            pendingPing = waiter
        }
        try {
            writer.write(
                YamuxFrameHeader(
                    type = YamuxFrameType.PING,
                    flags = YamuxFlags.SYN,
                    streamId = 0L,
                    length = id,
                ),
            )
            val started = TimeSource.Monotonic.markNow()
            val response = withTimeoutOrNull(config.keepAliveTimeoutMillis) {
                select {
                    waiter.signal.onAwait { true }
                    operationStopSignal.onAwait { false }
                }
            }
            if (response == false) throw requiredOperationFailure()
            if (response == null) {
                val failure = YamuxTimeoutFailure(timeoutKind)
                terminateNow(failure)
                throw failure
            }
            started.elapsedNow().inWholeMilliseconds
        } catch (failure: CancellationException) {
            cancelPing(waiter)
            throw failure
        } catch (failure: Error) {
            cancelPing(waiter)
            throw failure
        } catch (failure: Exception) {
            cancelPing(waiter)
            throw failure
        }
    }

    private fun cancelPing(waiter: PendingPing) {
        synchronized(stateLock) {
            if (pendingPing === waiter) {
                pendingPing = null
            }
        }
    }

    private suspend fun runKeepAlive() {
        while (!ending) {
            delay(config.keepAliveIntervalMillis)
            if (ending) return
            try {
                pingInternal(YamuxTimeoutKind.KEEPALIVE)
            } catch (failure: CancellationException) {
                if (!ending) terminateNow(failure)
                return
            } catch (failure: Error) {
                terminateNow(failure)
                return
            } catch (failure: YamuxFailure) {
                if (!ending) terminateNow(failure)
                return
            } catch (failure: Exception) {
                if (!ending) terminateNow(safeFailure(failure, YamuxTransportOperation.READ))
                return
            }
        }
    }

    private suspend fun acquireOpenPermit() {
        val result = select<OpenPermitResult> {
            streamPermits.onReceive { OpenPermitResult.ACQUIRED }
            newStreamsClosedSignal.onAwait { OpenPermitResult.GOAWAY }
            operationStopSignal.onAwait { OpenPermitResult.TERMINATED }
        }
        when (result) {
            OpenPermitResult.ACQUIRED -> Unit
            OpenPermitResult.GOAWAY -> throw synchronized(stateLock) {
                newStreamFailureLocked() ?: YamuxSessionClosedFailure()
            }
            OpenPermitResult.TERMINATED -> throw requiredOperationFailure()
        }
    }

    private fun allocateLocalStreamIdLocked(): Long {
        val streamId = nextLocalStreamId
        if (streamId >= YamuxProtocol.MAX_UINT32 - 1L) throw YamuxStreamIdExhaustedFailure()
        nextLocalStreamId = streamId + 2L
        return streamId
    }

    private fun newStreamFailureLocked(): Throwable? = when {
        remoteGoAway -> YamuxRemoteGoAwayFailure(YamuxGoAwayCode.NORMAL)
        else -> null
    }

    private fun cancelOpen(stream: YamuxStream) {
        if (stream.failLocallyForReset(YamuxStreamResetFailure())) scheduleResetBestEffort(stream)
    }

    private fun cancelAcceptWaiter(waiter: AcceptWaiter) {
        var waiterToWake: AcceptWaiter? = null
        var streamToReset: YamuxStream? = null
        synchronized(stateLock) {
            if (waiter.cancelled || waiter.consumed) return
            waiter.cancelled = true
            acceptWaiters.remove(waiter)
            val assigned = waiter.assigned.also { waiter.assigned = null }
            if (assigned != null && !ending) {
                val replacement = if (acceptWaiters.isEmpty()) null else acceptWaiters.removeFirst()
                when {
                    replacement != null -> {
                        replacement.assigned = assigned
                        waiterToWake = replacement
                    }

                    acceptBacklog.size < config.acceptBacklog -> acceptBacklog.addFirst(assigned)
                    else -> streamToReset = assigned
                }
            }
        }
        waiterToWake?.signal?.complete(Unit)
        streamToReset?.let { stream ->
            if (stream.failLocallyForReset(YamuxStreamResetFailure())) scheduleResetBestEffort(stream)
        }
    }

    private fun removePendingAcknowledgement(stream: YamuxStream) {
        synchronized(stateLock) {
            if (streams[stream.streamId] === stream) pendingOpenAcknowledgements.remove(stream.streamId)
        }
    }

    private fun reserveReceiveBytes(count: Long): Boolean {
        while (true) {
            val current = totalReceiveBytes.get()
            if (count > config.maxBufferedReceiveBytes - current) return false
            if (totalReceiveBytes.compareAndSet(current, current + count)) return true
        }
    }

    private fun releaseCancelledReservation(stream: YamuxStream, length: Long) {
        val released = stream.cancelInboundReservation(length)
        if (released > 0L) releaseReceiveBytes(released)
    }

    private fun returnPermitToken() {
        streamPermits.trySend(Unit)
    }

    private fun releaseActivePermitToken() {
        activePermitCount.updateAndGet { current -> (current - 1).coerceAtLeast(0) }
        returnPermitToken()
    }

    private fun addTombstoneLocked(streamId: Long, resetClaimed: Boolean = false): Tombstone {
        tombstones[streamId]?.let { existing ->
            if (resetClaimed) existing.resetClaimed = true
            return existing
        }
        val tombstone = Tombstone(resetClaimed)
        tombstones[streamId] = tombstone
        while (tombstones.size > config.tombstoneCapacity) {
            val iterator = tombstones.entries.iterator()
            check(iterator.hasNext())
            iterator.next()
            iterator.remove()
        }
        return tombstone
    }

    private fun claimTombstoneResetLocked(streamId: Long, inboundReset: Boolean): Boolean {
        val tombstone = addTombstoneLocked(streamId)
        if (inboundReset || tombstone.resetClaimed) {
            if (inboundReset) tombstone.resetClaimed = true
            return false
        }
        tombstone.resetClaimed = true
        return true
    }

    private fun terminateWithGoAway(failure: Throwable, code: YamuxGoAwayCode) {
        beginEnding(failure, failure, termination = null, goAwayCode = code)
    }

    private fun terminateNow(failure: Throwable) {
        if (!beginEnding(failure, failure, termination = null)) {
            recordSecondaryFailure(failure)
        }
    }

    private fun terminateFromCallback(failure: Throwable) {
        if (!beginEnding(failure, failure, termination = null)) {
            recordSecondaryFailure(failure, includeOrdinary = true)
        }
    }

    private fun beginEnding(
        operationFailure: Throwable,
        finalFailure: Throwable?,
        termination: YamuxTermination?,
        goAwayCode: YamuxGoAwayCode? = null,
    ): Boolean {
        var pendingNormalGoAway: NormalGoAwayAttempt? = null
        var normalGoAwayCommitted = false
        var queuedResetRequests = emptyList<ResetRequest>()
        val began = synchronized(endingLock) {
            var changed = false
            synchronized(stateLock) {
                if (!ending) {
                    pendingNormalGoAway = normalGoAwayAttempt
                    normalGoAwayCommitted = normalGoAwayPhase == NormalGoAwayPhase.SENT
                    normalGoAwayAttempt = null
                    if (normalGoAwayPhase == NormalGoAwayPhase.SENDING) {
                        normalGoAwayPhase = NormalGoAwayPhase.NOT_SENT
                    }
                    operationFailureRef.set(operationFailure)
                    finalFailureRef.set(finalFailure)
                    terminationRef.set(termination)
                    ending = true
                    queuedResetRequests = resetRequestQueue.toList()
                    resetRequestQueue.clear()
                    writer.requestTermination(operationFailure)
                    changed = true
                }
            }
            changed
        }
        if (!began) return false
        pendingNormalGoAway?.complete(NormalGoAwayResult.Failure(operationFailure))
        queuedResetRequests.forEach { finishResetRequest(it, failure = null) }
        operationStopSignal.complete(Unit)
        newStreamsClosedSignal.complete(Unit)
        finalizerScope.launch {
            // Never run writer.join inline from a writer callback on an unconfined dispatcher.
            yield()
            finalizeEnding(
                goAwayCode = goAwayCode,
                normalGoAwayCommitted = normalGoAwayCommitted,
                pendingNormalGoAway = pendingNormalGoAway,
            )
        }
        return true
    }

    private suspend fun finalizeEnding(
        goAwayCode: YamuxGoAwayCode?,
        normalGoAwayCommitted: Boolean,
        pendingNormalGoAway: NormalGoAwayAttempt?,
    ) {
        try {
            withContext(NonCancellable) {
                val operationFailure = operationFailureRef.get() ?: YamuxSessionClosedFailure()
                val activeStreams = synchronized(stateLock) {
                    val snapshot = streams.values.toList()
                    streams.clear()
                    pendingOpenAcknowledgements.clear()
                    acceptBacklog.clear()
                    acceptWaiters.clear()
                    receiveUpdateQueue.clear()
                    tombstones.clear()
                    pendingPing = null
                    snapshot
                }
                activeStreams.forEach { stream ->
                    try {
                        stream.failFromSession(operationFailure)
                        if (stream.releasePermitOnce()) releaseActivePermitToken()
                    } catch (failure: CancellationException) {
                        recordSecondaryFailure(originalCancellation(failure))
                    } catch (failure: Error) {
                        recordSecondaryFailure(failure)
                    } catch (failure: Exception) {
                        recordSecondaryFailure(failure, includeOrdinary = true)
                    }
                }

                writer.beginTermination(operationFailure)?.let(::recordSecondaryFailure)

                if (goAwayCode != null) {
                    try {
                        withTimeoutOrNull(config.writeTimeoutMillis) {
                            val shouldSend = if (goAwayCode != YamuxGoAwayCode.NORMAL) {
                                true
                            } else {
                                val alreadyOwned = normalGoAwayCommitted ||
                                    pendingNormalGoAway?.ownership?.await() == true
                                if (alreadyOwned) pendingNormalGoAway?.physicalWriteDone?.await()
                                !alreadyOwned
                            }
                            if (shouldSend) {
                                writer.writeTerminalBestEffort(
                                    header = YamuxFrameHeader(
                                        type = YamuxFrameType.GOAWAY,
                                        flags = 0,
                                        streamId = 0L,
                                        length = goAwayCode.wireValue,
                                    ),
                                    timeoutMillis = config.writeTimeoutMillis,
                                )
                            }
                        }
                    } catch (failure: CancellationException) {
                        recordSecondaryFailure(originalCancellation(failure))
                    } catch (failure: Error) {
                        recordSecondaryFailure(failure)
                    } catch (_: Exception) {
                        // The selected termination remains authoritative for ordinary close I/O.
                    }
                }

                writer.shutdown(operationFailure)?.let(::recordSecondaryFailure)

                try {
                    transport.close()
                } catch (failure: CancellationException) {
                    recordSecondaryFailure(originalCancellation(failure))
                } catch (failure: Error) {
                    recordSecondaryFailure(failure)
                } catch (_: Exception) {
                    // Expected close I/O errors are secondary to the selected termination.
                }

                writer.joinOrAbort(forcedTerminationSignal)

                val completionHandle = synchronized(endingLock) {
                    parentCompletionHandle.also { parentCompletionHandle = null }
                }
                try {
                    completionHandle?.dispose()
                } catch (failure: CancellationException) {
                    recordSecondaryFailure(originalCancellation(failure))
                } catch (failure: Error) {
                    recordSecondaryFailure(failure)
                } catch (failure: Exception) {
                    recordSecondaryFailure(failure, includeOrdinary = true)
                }

                sessionJob.cancel()
                select {
                    sessionJob.onJoin { }
                    forcedTerminationSignal.onAwait { }
                }
                readerJob = null
                keepAliveJob = null
                receiveUpdateJob = null
                resetDispatcherJob = null
            }
        } catch (failure: CancellationException) {
            recordSecondaryFailure(originalCancellation(failure))
        } catch (failure: Error) {
            recordSecondaryFailure(failure)
        } catch (failure: Exception) {
            recordSecondaryFailure(failure, includeOrdinary = true)
        } finally {
            terminationDoneSignal.complete(Unit)
        }
    }

    private fun recordSecondaryFailure(failure: Throwable, includeOrdinary: Boolean = false) {
        val candidate = if (failure is CancellationException) originalCancellation(failure) else failure
        if (!includeOrdinary && candidate !is Error && candidate !is CancellationException) return
        while (true) {
            val current = finalFailureRef.get()
            if (preferYamuxFailure(current, candidate) === current) return
            if (finalFailureRef.compareAndSet(current, candidate)) return
        }
    }

    private fun throwIfEndingLocked() {
        if (ending) throw requiredOperationFailure()
    }

    private fun requiredOperationFailure(): Throwable =
        operationFailureRef.get() ?: YamuxSessionClosedFailure()

    private fun safeFailure(failure: Exception, operation: YamuxTransportOperation): Throwable =
        if (failure is YamuxFailure) failure else YamuxTransportFailure(operation)

    private fun originalCancellation(failure: CancellationException): CancellationException {
        var original = failure
        while (true) {
            val cause = original.cause as? CancellationException ?: return original
            if (cause === original) return original
            original = cause
        }
    }

    override fun toString(): String {
        val diagnostic = diagnostics()
        return "YamuxClientSession(activeStreams=${diagnostic.activeStreams}, " +
            "pendingOpenCalls=${diagnostic.pendingOpenCalls}, " +
            "acceptBacklog=${diagnostic.acceptBacklog}, ending=${diagnostic.ending}, " +
            "terminated=${diagnostic.terminated})"
    }

    private data class PendingPing(
        val id: Long,
        val signal: CompletableDeferred<Unit> = CompletableDeferred(),
    )

    private class AcceptWaiter(
        val signal: CompletableDeferred<Unit> = CompletableDeferred(),
        var assigned: YamuxStream? = null,
        var consumed: Boolean = false,
        var cancelled: Boolean = false,
    )

    private enum class NormalGoAwayPhase {
        NOT_SENT,
        SENDING,
        SENT,
    }

    private enum class ResetAdmission {
        ADMITTED,
        STOPPED,
        OVERFLOW,
    }

    private sealed interface NormalGoAwayResult {
        data object Success : NormalGoAwayResult

        data object Retry : NormalGoAwayResult

        data class Failure(val failure: Throwable) : NormalGoAwayResult
    }

    private class NormalGoAwayAttempt {
        val result = CompletableDeferred<NormalGoAwayResult>()
        val ownership = CompletableDeferred<Boolean>()
        val physicalWriteDone = CompletableDeferred<Unit>()
        private val failureRef = AtomicReference<Throwable?>()
        private val writerJobRef = AtomicReference<Job?>()

        fun complete(result: NormalGoAwayResult) {
            if (result is NormalGoAwayResult.Failure) failureRef.compareAndSet(null, result.failure)
            this.result.complete(result)
        }

        fun completeOwnership(owned: Boolean) {
            ownership.complete(owned)
        }

        fun completePhysicalWrite() {
            physicalWriteDone.complete(Unit)
        }

        fun attachWriterJob(job: Job) {
            check(writerJobRef.compareAndSet(null, job))
        }

        fun cancelWriter(failure: CancellationException) {
            writerJobRef.get()?.cancel(failure)
        }

        fun failure(): Throwable? = failureRef.get()
    }

    private class ResetRequest(
        val stream: YamuxStream,
        val completion: CompletableDeferred<Throwable?> = CompletableDeferred(),
        val finished: AtomicBoolean = AtomicBoolean(false),
        val counted: AtomicBoolean = AtomicBoolean(false),
    ) {
        val queuedAt = TimeSource.Monotonic.markNow()
    }

    private data class Tombstone(var resetClaimed: Boolean)

    private data class DiagnosticsState(
        val activeStreams: Int,
        val pendingOpenAcknowledgements: Int,
        val acceptBacklog: Int,
        val acceptWaiters: Int,
        val pendingReceiveUpdates: Int,
        val tombstones: Int,
        val pendingPing: Boolean,
    )

    private enum class OpenPermitResult {
        ACQUIRED,
        GOAWAY,
        TERMINATED,
    }

    private sealed interface StreamResolution {
        data class Active(val stream: YamuxStream) : StreamResolution

        data class Reject(val sendReset: Boolean) : StreamResolution

        data class Tombstone(val sendReset: Boolean) : StreamResolution

        data object Stopping : StreamResolution
    }
}

private fun invokeYamuxCallback(callback: () -> Unit): Throwable? = try {
    callback()
    null
} catch (failure: Throwable) {
    failure
}
