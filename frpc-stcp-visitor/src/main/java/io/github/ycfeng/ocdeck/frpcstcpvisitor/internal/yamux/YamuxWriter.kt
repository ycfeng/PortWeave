package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux

import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull

internal class YamuxWriteAbort(
    val signal: Deferred<Unit>,
    val failure: () -> Throwable,
)

internal fun preferYamuxFailure(first: Throwable?, second: Throwable?): Throwable? = when {
    first == null -> second
    second == null -> first
    yamuxFailurePriority(second) > yamuxFailurePriority(first) -> second
    else -> first
}

private fun yamuxFailurePriority(failure: Throwable): Int = when (failure) {
    is Error -> 3
    is CancellationException -> 2
    else -> 1
}

internal class YamuxFrameWriter(
    private val transport: YamuxPhysicalTransport,
    private val config: YamuxConfig,
    private val scope: CoroutineScope,
    private val onFatalFailure: (Throwable) -> Unit,
    private val onCallbackFailure: (Throwable) -> Unit = onFatalFailure,
    private val beforeMailboxSend: suspend (YamuxFrameHeader) -> Unit = {},
) {
    private val normalMailbox = Channel<Request>(config.writerMailboxCapacity)
    private val controlMailbox = Channel<Request>(config.maxStreams)
    private val terminalMailbox = Channel<Request>(1)
    private val queuedCount = AtomicInteger(0)
    private val activeRequest = AtomicReference<Request?>()
    private val ownershipLock = Any()
    private val terminationFailure = AtomicReference<Throwable?>()
    private val regularStopped = AtomicBoolean(false)
    private val fullyStopped = AtomicBoolean(false)
    private val shutdownFailure = AtomicReference<Throwable?>()
    private val byteBudgetLock = Any()
    private val normalByteBudgetWaiters = ArrayDeque<NormalByteBudgetWaiter>()
    private var normalQueuedBytes = 0L
    private var priorityQueuedBytes = 0L
    private var consecutiveControlWrites = 0
    private val writerJob: Job = scope.launch { runWriter() }

    val pendingFrameCount: Int
        get() = queuedCount.get() + if (activeRequest.get() == null) 0 else 1

    val pendingByteCount: Long
        get() = synchronized(byteBudgetLock) { normalQueuedBytes + priorityQueuedBytes }

    suspend fun write(
        header: YamuxFrameHeader,
        body: ByteArray? = null,
        bodyOffset: Int = 0,
        bodyLength: Int = body?.size?.minus(bodyOffset) ?: 0,
        abort: YamuxWriteAbort? = null,
        onWriteStarted: () -> Unit = {},
        onNotStarted: () -> Unit = {},
    ) {
        val priority = if (header.type == YamuxFrameType.DATA) {
            RequestPriority.NORMAL
        } else {
            RequestPriority.CONTROL
        }
        val request = prepareRequest(
            header = header,
            body = body,
            bodyOffset = bodyOffset,
            bodyLength = bodyLength,
            priority = priority,
            abort = abort,
            onWriteStarted = onWriteStarted,
            onNotStarted = onNotStarted,
        )
        enqueue(request, abort, awaitCompletion = true)
    }

    suspend fun enqueueControl(
        header: YamuxFrameHeader,
        abort: YamuxWriteAbort? = null,
        onWriteStarted: () -> Unit = {},
        onNotStarted: () -> Unit = {},
    ) {
        require(header.type != YamuxFrameType.DATA)
        val request = prepareRequest(
            header = header,
            body = null,
            bodyOffset = 0,
            bodyLength = 0,
            priority = RequestPriority.CONTROL,
            abort = abort,
            onWriteStarted = onWriteStarted,
            onNotStarted = onNotStarted,
        )
        enqueue(request, abort, awaitCompletion = false)
    }

    suspend fun writeBestEffort(header: YamuxFrameHeader, body: ByteArray? = null): Boolean = try {
        write(header, body)
        true
    } catch (failure: CancellationException) {
        throw failure
    } catch (failure: Error) {
        throw failure
    } catch (_: Exception) {
        false
    }

    suspend fun writeTerminalBestEffort(
        header: YamuxFrameHeader,
        timeoutMillis: Long,
    ): Boolean {
        require(header.type == YamuxFrameType.GOAWAY)
        require(timeoutMillis > 0L)
        val request = try {
            prepareRequest(
                header = header,
                body = null,
                bodyOffset = 0,
                bodyLength = 0,
                priority = RequestPriority.TERMINAL,
                abort = null,
                onWriteStarted = {},
                onNotStarted = {},
            )
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (_: Exception) {
            return false
        }

        request.markQueueCounted()
        queuedCount.incrementAndGet()
        if (terminalMailbox.trySend(request).isFailure) {
            request.releaseQueueCount(::decrementQueuedCount)
            val resolution = request.cancelBeforeStart(YamuxSessionClosedFailure())
            reportCallbackFailure(resolution)
            return false
        }

        val completed = try {
            withTimeoutOrNull(timeoutMillis) {
                request.awaitResult()
                true
            }
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (_: Exception) {
            return false
        }
        if (completed == true) return true

        val resolution = request.cancelBeforeStart(YamuxTimeoutFailure(YamuxTimeoutKind.WRITE))
        if (resolution != null) request.releaseQueueCount(::decrementQueuedCount)
        reportCallbackFailure(resolution)
        return false
    }

    fun beginTermination(failure: Throwable): Throwable? {
        requestTermination(failure)
        shutdownFailure.compareAndSet(null, failure)
        if (!regularStopped.compareAndSet(false, true)) return null
        normalMailbox.close()
        controlMailbox.close()
        wakeNormalByteBudgetWaiters()
        return preferYamuxFailure(
            drainMailbox(controlMailbox, failure),
            drainMailbox(normalMailbox, failure),
        )
    }

    fun requestTermination(failure: Throwable) {
        synchronized(ownershipLock) {
            terminationFailure.compareAndSet(null, failure)
        }
    }

    fun shutdown(failure: Throwable): Throwable? {
        var selected = beginTermination(failure)
        if (fullyStopped.compareAndSet(false, true)) {
            terminalMailbox.close()
            selected = preferYamuxFailure(selected, drainMailbox(terminalMailbox, failure))
            wakeNormalByteBudgetWaiters()
            writerJob.cancel()
        }
        return selected
    }

    suspend fun join() {
        check(currentCoroutineContext()[Job] !== writerJob)
        writerJob.join()
    }

    suspend fun joinOrAbort(abort: Deferred<Unit>): Boolean {
        check(currentCoroutineContext()[Job] !== writerJob)
        return select {
            writerJob.onJoin { true }
            abort.onAwait { false }
        }
    }

    suspend fun shutdownAndJoin(failure: Throwable): Throwable? {
        val selected = shutdown(failure)
        join()
        return selected
    }

    private suspend fun prepareRequest(
        header: YamuxFrameHeader,
        body: ByteArray?,
        bodyOffset: Int,
        bodyLength: Int,
        priority: RequestPriority,
        abort: YamuxWriteAbort?,
        onWriteStarted: () -> Unit,
        onNotStarted: () -> Unit,
    ): Request {
        val specification = try {
            validateBody(header, body, bodyOffset, bodyLength)
        } catch (failure: Throwable) {
            throw failBeforeRequest(failure, onNotStarted)
        }
        try {
            reserveBytes(specification.wireBytes, priority, abort)
        } catch (failure: Throwable) {
            throw failBeforeRequest(failure, onNotStarted)
        }
        return try {
            Request(
                header = header,
                frame = YamuxFrameCodec.encodeFrame(header, body, bodyOffset, bodyLength),
                wireBytes = specification.wireBytes,
                priority = priority,
                onWriteStarted = onWriteStarted,
                onNotStarted = onNotStarted,
                releaseBytes = ::releaseBytes,
                onCallbackFailure = ::onRequestCallbackFailure,
            )
        } catch (failure: Throwable) {
            releaseBytes(priority, specification.wireBytes)
            throw failBeforeRequest(failure, onNotStarted)
        }
    }

    private suspend fun enqueue(
        request: Request,
        abort: YamuxWriteAbort?,
        awaitCompletion: Boolean,
    ) {
        if (abort != null) {
            request.attachAbort(abort)
            if (!request.isNew()) request.throwFailure()
        }
        try {
            beforeMailboxSend(request.header)
        } catch (failure: CancellationException) {
            interruptBeforeOrDuringWrite(request, failure)
        } catch (failure: Error) {
            interruptBeforeOrDuringWrite(request, failure)
        } catch (failure: Exception) {
            interruptBeforeOrDuringWrite(request, failure)
        }
        if (!request.isNew()) request.throwFailure()

        request.markQueueCounted()
        queuedCount.incrementAndGet()
        try {
            val mailbox = when (request.priority) {
                RequestPriority.NORMAL -> normalMailbox
                RequestPriority.CONTROL -> controlMailbox
                RequestPriority.TERMINAL -> terminalMailbox
            }
            if (abort == null) {
                mailbox.send(request)
            } else {
                val admitted = select {
                    mailbox.onSend(request) { true }
                    abort.signal.onAwait { false }
                }
                if (!admitted) interruptBeforeOrDuringWrite(request, abortFailure(abort))
            }
        } catch (failure: CancellationException) {
            interruptBeforeOrDuringWrite(request, failure)
        } catch (failure: Error) {
            interruptBeforeOrDuringWrite(request, failure)
        } catch (_: Exception) {
            interruptBeforeOrDuringWrite(request, YamuxSessionClosedFailure())
        }

        if (awaitCompletion) awaitRequest(request, abort)
    }

    private suspend fun awaitRequest(request: Request, abort: YamuxWriteAbort?) {
        try {
            if (abort == null) {
                request.awaitResult()
                return
            }
            val completed = select {
                request.completion.onAwait { true }
                abort.signal.onAwait { false }
            }
            if (!completed) interruptBeforeOrDuringWrite(request, abortFailure(abort))
            request.throwFailure()
        } catch (failure: CancellationException) {
            interruptBeforeOrDuringWrite(request, failure)
        }
    }

    private suspend fun interruptBeforeOrDuringWrite(request: Request, cause: Throwable): Nothing {
        val cancelled = request.cancelBeforeStart(cause)
        if (cancelled != null) {
            request.releaseQueueCount(::decrementQueuedCount)
            reportCallbackFailure(cancelled)
            throw checkNotNull(cancelled.failure)
        }

        // Once physical ownership is acquired, state is committed and only session termination may
        // interrupt the transport. The caller still receives the highest-priority observed failure.
        withContext(NonCancellable) {
            request.completion.await()
        }
        throw checkNotNull(preferYamuxFailure(cause, request.failure()))
    }

    private suspend fun runWriter() {
        while (true) {
            val request = try {
                receiveNextRequest()
            } catch (failure: CancellationException) {
                if (!fullyStopped.get()) failWriter(failure)
                return
            } catch (failure: Error) {
                failWriter(failure)
                return
            } catch (_: Exception) {
                if (!fullyStopped.get()) failWriter(YamuxTransportFailure(YamuxTransportOperation.WRITE))
                return
            }
            request.releaseQueueCount(::decrementQueuedCount)
            if (!request.isNew()) continue

            var claimFailure: Throwable? = null
            val claimed = synchronized(ownershipLock) {
                claimFailure = terminationFailure.get()
                (claimFailure == null || request.priority == RequestPriority.TERMINAL) &&
                    request.claimPhysicalWrite()
            }
            if (!claimed) {
                claimFailure?.let { failure ->
                    val resolution = request.cancelBeforeStart(failure)
                    reportCallbackFailure(resolution)
                }
                continue
            }

            activeRequest.set(request)
            val ownershipFailure = request.commitPhysicalWriteOwnership()
            val physicalFailure = ownershipFailure ?: writeRequest(request)
            activeRequest.compareAndSet(request, null)
            val resolution = request.finish(physicalFailure)
            if (resolution.failure != null) {
                if (
                    !fullyStopped.get() ||
                    resolution.failure is Error ||
                    ownershipFailure != null ||
                    resolution.callbackFailure != null
                ) {
                    failWriter(resolution.failure)
                }
                return
            }
        }
    }

    private suspend fun receiveNextRequest(): Request {
        while (true) {
            terminalMailbox.tryReceive().getOrNull()?.let { return it }
            if (terminationFailure.get() != null || regularStopped.get()) return terminalMailbox.receive()
            val preferNormal = consecutiveControlWrites >= MAX_CONTROL_BURST
            val immediate = if (preferNormal) {
                normalMailbox.tryReceive().getOrNull()
                    ?: controlMailbox.tryReceive().getOrNull()
            } else {
                controlMailbox.tryReceive().getOrNull()
                    ?: normalMailbox.tryReceive().getOrNull()
            }
            if (immediate != null) return recordRegularSelection(immediate)

            val selected = if (preferNormal) {
                select<Request?> {
                    terminalMailbox.onReceiveCatching { it.getOrNull() }
                    normalMailbox.onReceiveCatching { it.getOrNull() }
                    controlMailbox.onReceiveCatching { it.getOrNull() }
                }
            } else {
                select<Request?> {
                    terminalMailbox.onReceiveCatching { it.getOrNull() }
                    controlMailbox.onReceiveCatching { it.getOrNull() }
                    normalMailbox.onReceiveCatching { it.getOrNull() }
                }
            }
            if (selected != null) {
                return if (selected.priority == RequestPriority.TERMINAL) {
                    selected
                } else {
                    recordRegularSelection(selected)
                }
            }
        }
    }

    private fun recordRegularSelection(request: Request): Request {
        if (request.priority == RequestPriority.CONTROL) {
            consecutiveControlWrites = (consecutiveControlWrites + 1).coerceAtMost(MAX_CONTROL_BURST)
        } else {
            consecutiveControlWrites = 0
        }
        return request
    }

    private suspend fun writeRequest(request: Request): Throwable? = try {
        val completed = withTimeoutOrNull(config.writeTimeoutMillis) {
            transport.writeFully(request.frame())
            true
        }
        if (completed == null) YamuxTimeoutFailure(YamuxTimeoutKind.WRITE) else null
    } catch (failure: CancellationException) {
        if (fullyStopped.get()) shutdownFailure.get() ?: failure else failure
    } catch (failure: Error) {
        failure
    } catch (failure: Exception) {
        if (failure is YamuxFailure) failure else YamuxTransportFailure(YamuxTransportOperation.WRITE)
    }

    private fun failWriter(failure: Throwable) {
        val drained = stopAcceptingAll(failure)
        onFatalFailure(checkNotNull(preferYamuxFailure(failure, drained)))
    }

    private fun stopAcceptingAll(failure: Throwable): Throwable? {
        var selected = beginTermination(failure)
        if (fullyStopped.compareAndSet(false, true)) {
            terminalMailbox.close()
            selected = preferYamuxFailure(selected, drainMailbox(terminalMailbox, failure))
            wakeNormalByteBudgetWaiters()
        }
        return selected
    }

    private fun drainMailbox(mailbox: Channel<Request>, cause: Throwable): Throwable? {
        var selected: Throwable? = null
        while (true) {
            val request = mailbox.tryReceive().getOrNull() ?: break
            request.releaseQueueCount(::decrementQueuedCount)
            val resolution = request.cancelBeforeStart(cause) ?: continue
            resolution.callbackFailure?.let(onCallbackFailure)
            selected = preferYamuxFailure(selected, resolution.failure)
        }
        return selected
    }

    private suspend fun reserveBytes(
        count: Long,
        priority: RequestPriority,
        abort: YamuxWriteAbort?,
    ) {
        if (priority != RequestPriority.NORMAL) {
            synchronized(byteBudgetLock) {
                if (fullyStopped.get() || priority == RequestPriority.CONTROL && regularStopped.get()) {
                    throw YamuxSessionClosedFailure()
                }
                priorityQueuedBytes += count
            }
            return
        }
        if (count > config.writerQueuedByteCapacity) {
            throw YamuxResourceLimitFailure(YamuxResourceKind.WRITER_BYTES)
        }
        val waiter = synchronized(byteBudgetLock) {
            if (regularStopped.get() || fullyStopped.get()) throw YamuxSessionClosedFailure()
            if (
                normalByteBudgetWaiters.isEmpty() &&
                count <= config.writerQueuedByteCapacity - normalQueuedBytes
            ) {
                normalQueuedBytes += count
                null
            } else {
                if (normalByteBudgetWaiters.size >= config.maxStreams) {
                    throw YamuxResourceLimitFailure(YamuxResourceKind.WRITER_FRAMES)
                }
                NormalByteBudgetWaiter(count).also(normalByteBudgetWaiters::addLast)
            }
        } ?: return

        try {
            val granted = if (abort == null) {
                waiter.signal.await()
                true
            } else {
                select {
                    waiter.signal.onAwait { true }
                    abort.signal.onAwait { false }
                }
            }
            if (!granted) throw abortFailure(abort!!)
            synchronized(byteBudgetLock) {
                if (!waiter.granted || regularStopped.get() || fullyStopped.get()) {
                    throw YamuxSessionClosedFailure()
                }
            }
        } catch (failure: CancellationException) {
            cancelNormalByteBudgetWaiter(waiter)
            throw failure
        } catch (failure: Error) {
            cancelNormalByteBudgetWaiter(waiter)
            throw failure
        } catch (failure: Exception) {
            cancelNormalByteBudgetWaiter(waiter)
            throw failure
        }
    }

    private fun releaseBytes(priority: RequestPriority, count: Long) {
        val waiters = synchronized(byteBudgetLock) {
            if (priority == RequestPriority.NORMAL) {
                normalQueuedBytes = (normalQueuedBytes - count).coerceAtLeast(0L)
                grantNormalByteBudgetWaitersLocked()
            } else {
                priorityQueuedBytes = (priorityQueuedBytes - count).coerceAtLeast(0L)
                emptyList()
            }
        }
        waiters.forEach { it.complete(Unit) }
    }

    private fun cancelNormalByteBudgetWaiter(waiter: NormalByteBudgetWaiter) {
        val granted = synchronized(byteBudgetLock) {
            if (waiter.cancelled) return
            waiter.cancelled = true
            if (waiter.granted) {
                waiter.granted = false
                normalQueuedBytes = (normalQueuedBytes - waiter.count).coerceAtLeast(0L)
            } else {
                normalByteBudgetWaiters.remove(waiter)
            }
            grantNormalByteBudgetWaitersLocked()
        }
        granted.forEach { it.complete(Unit) }
    }

    private fun wakeNormalByteBudgetWaiters() {
        val waiters = synchronized(byteBudgetLock) {
            normalByteBudgetWaiters.map { waiter ->
                waiter.cancelled = true
                waiter.signal
            }.also { normalByteBudgetWaiters.clear() }
        }
        waiters.forEach { it.complete(Unit) }
    }

    private fun grantNormalByteBudgetWaitersLocked(): List<CompletableDeferred<Unit>> = buildList {
        while (normalByteBudgetWaiters.isNotEmpty()) {
            val waiter = normalByteBudgetWaiters.first()
            if (waiter.count > config.writerQueuedByteCapacity - normalQueuedBytes) break
            normalByteBudgetWaiters.removeFirst()
            if (waiter.cancelled) continue
            waiter.granted = true
            normalQueuedBytes += waiter.count
            add(waiter.signal)
        }
    }

    private fun failBeforeRequest(cause: Throwable, rollback: () -> Unit): Throwable {
        val callbackFailure = invokeCallback(rollback)
        if (callbackFailure != null) onCallbackFailure(callbackFailure)
        return checkNotNull(preferYamuxFailure(cause, callbackFailure))
    }

    private fun onRequestCallbackFailure(failure: Throwable) {
        onCallbackFailure(failure)
    }

    private fun reportCallbackFailure(resolution: RequestResolution?) {
        resolution?.callbackFailure?.let(onCallbackFailure)
    }

    private fun abortFailure(abort: YamuxWriteAbort): Throwable = try {
        abort.failure()
    } catch (failure: Throwable) {
        failure
    }

    private fun validateBody(
        header: YamuxFrameHeader,
        body: ByteArray?,
        bodyOffset: Int,
        bodyLength: Int,
    ): RequestSpecification {
        YamuxFrameCodec.validateHeader(header)
        if (header.type == YamuxFrameType.DATA) {
            require(header.length <= Int.MAX_VALUE.toLong())
            require(body != null || bodyLength == 0)
            requireRange(body?.size ?: 0, bodyOffset, bodyLength)
            require(bodyLength == header.length.toInt())
            require(bodyLength <= config.outboundDataFrameSize)
        } else {
            require(body == null)
            require(bodyOffset == 0 && bodyLength == 0)
        }
        return RequestSpecification(YamuxProtocol.HEADER_SIZE.toLong() + bodyLength.toLong())
    }

    private fun decrementQueuedCount() {
        queuedCount.updateAndGet { current -> (current - 1).coerceAtLeast(0) }
    }

    private fun requireRange(size: Int, offset: Int, length: Int) {
        require(offset >= 0 && length >= 0 && offset <= size - length)
    }

    private data class RequestSpecification(val wireBytes: Long)

    private class NormalByteBudgetWaiter(
        val count: Long,
        val signal: CompletableDeferred<Unit> = CompletableDeferred(),
        var granted: Boolean = false,
        var cancelled: Boolean = false,
    )

    private enum class RequestPriority {
        NORMAL,
        CONTROL,
        TERMINAL,
    }

    private data class RequestResolution(
        val failure: Throwable?,
        val callbackFailure: Throwable?,
    )

    private class Request(
        val header: YamuxFrameHeader,
        frame: ByteArray,
        private val wireBytes: Long,
        val priority: RequestPriority,
        private val onWriteStarted: () -> Unit,
        private val onNotStarted: () -> Unit,
        private val releaseBytes: (RequestPriority, Long) -> Unit,
        private val onCallbackFailure: (Throwable) -> Unit,
    ) {
        private val state = AtomicInteger(NEW)
        private val failureRef = AtomicReference<Throwable?>()
        private val frameRef = AtomicReference(frame)
        private val queueCounted = AtomicBoolean(false)
        private val released = AtomicBoolean(false)
        private val abortHandle = AtomicReference<DisposableHandle?>()
        val completion = CompletableDeferred<Unit>()

        fun markQueueCounted() {
            queueCounted.set(true)
        }

        fun releaseQueueCount(release: () -> Unit) {
            if (queueCounted.compareAndSet(true, false)) release()
        }

        fun attachAbort(abort: YamuxWriteAbort) {
            val handle = abort.signal.invokeOnCompletion {
                val cause = try {
                    abort.failure()
                } catch (failure: Throwable) {
                    failure
                }
                cancelBeforeStart(cause)?.callbackFailure?.let(onCallbackFailure)
            }
            if (!abortHandle.compareAndSet(null, handle)) {
                disposeHandle(handle)
            } else if (state.get() != NEW) {
                abortHandle.getAndSet(null)?.let(::disposeHandle)
            }
        }

        fun frame(): ByteArray = checkNotNull(frameRef.get())

        fun isNew(): Boolean = state.get() == NEW

        fun claimPhysicalWrite(): Boolean = state.compareAndSet(NEW, STARTED)

        fun commitPhysicalWriteOwnership(): Throwable? = invokeCallback(onWriteStarted).also { failure ->
            if (failure != null) onCallbackFailure(failure)
        }

        fun cancelBeforeStart(cause: Throwable): RequestResolution? {
            if (!state.compareAndSet(NEW, CANCELLED)) return null
            return resolve(cause, onNotStarted)
        }

        fun finish(cause: Throwable?): RequestResolution {
            if (!state.compareAndSet(STARTED, FINISHED)) {
                return RequestResolution(cause, callbackFailure = null)
            }
            return resolve(cause, callback = null)
        }

        suspend fun awaitResult() {
            completion.await()
            throwFailure()
        }

        fun failure(): Throwable? = failureRef.get()

        fun throwFailure() {
            failure()?.let { throw it }
        }

        private fun disposeHandle(handle: DisposableHandle) {
            try {
                handle.dispose()
            } catch (failure: Throwable) {
                onCallbackFailure(failure)
            }
        }

        private fun resolve(cause: Throwable?, callback: (() -> Unit)?): RequestResolution {
            var callbackFailure: Throwable? = null
            var cleanupFailure: Throwable? = null
            try {
                callbackFailure = callback?.let(::invokeCallback)
            } finally {
                try {
                    abortHandle.getAndSet(null)?.dispose()
                } catch (failure: Throwable) {
                    cleanupFailure = preferYamuxFailure(cleanupFailure, failure)
                }
                frameRef.set(null)
                if (released.compareAndSet(false, true)) {
                    try {
                        releaseBytes(priority, wireBytes)
                    } catch (failure: Throwable) {
                        cleanupFailure = preferYamuxFailure(cleanupFailure, failure)
                    }
                }
            }
            val resolutionFailure = preferYamuxFailure(callbackFailure, cleanupFailure)
            val effectiveFailure = preferYamuxFailure(cause, resolutionFailure)
            failureRef.set(effectiveFailure)
            completion.complete(Unit)
            return RequestResolution(effectiveFailure, resolutionFailure)
        }

        private companion object {
            const val NEW = 0
            const val STARTED = 1
            const val FINISHED = 2
            const val CANCELLED = 3
        }
    }

    internal companion object {
        const val MAX_CONTROL_BURST = 8
    }
}

private fun invokeCallback(callback: () -> Unit): Throwable? = try {
    callback()
    null
} catch (failure: Throwable) {
    failure
}
