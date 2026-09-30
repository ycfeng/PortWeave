package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport

import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext

internal fun interface BlockingIoThreadGuard {
    fun check()
}

internal object NoMainThreadBlockingIo : BlockingIoThreadGuard {
    override fun check() {
        check(Thread.currentThread().name != "main") {
            "FRP blocking stream adapters cannot run on the main thread"
        }
    }
}

internal class YamuxStreamBlockingIo(
    private val stream: FrpMuxStream,
    parentScope: CoroutineScope,
    private val codecThreadGuard: BlockingIoThreadGuard,
) : AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val resetFailure = AtomicReference<Throwable?>()
    private val resetDone = CompletableDeferred<Unit>()
    private val cleanupJob = SupervisorJob()
    private val cleanupScope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) +
            cleanupJob +
            CoroutineName("YamuxStreamBlockingIoCleanup"),
    )

    val input: InputStream = object : InputStream() {
        override fun read(): Int {
            val single = ByteArray(1)
            return try {
                val count = read(single, 0, 1)
                if (count < 0) -1 else single[0].toInt() and 0xff
            } finally {
                single.fill(0)
            }
        }

        override fun read(destination: ByteArray, offset: Int, length: Int): Int {
            requireRange(destination.size, offset, length)
            if (length == 0) return 0
            codecThreadGuard.check()
            checkOpen()
            return runBlocking {
                stream.read(destination, offset, length)
            }
        }

        override fun close() {
            this@YamuxStreamBlockingIo.close()
        }

        override fun toString(): String = "YamuxBlockingInputStream(closed=${closed.get()})"
    }

    val output: OutputStream = object : OutputStream() {
        override fun write(value: Int) {
            val single = byteArrayOf(value.toByte())
            try {
                write(single, 0, 1)
            } finally {
                single.fill(0)
            }
        }

        override fun write(source: ByteArray, offset: Int, length: Int) {
            requireRange(source.size, offset, length)
            if (length == 0) return
            codecThreadGuard.check()
            checkOpen()
            runBlocking {
                stream.write(source, offset, length)
            }
        }

        override fun flush() = Unit

        override fun close() {
            this@YamuxStreamBlockingIo.close()
        }

        override fun toString(): String = "YamuxBlockingOutputStream(closed=${closed.get()})"
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        cleanupScope.launch(start = CoroutineStart.UNDISPATCHED) {
            try {
                withContext(NonCancellable) {
                    stream.reset()
                }
            } catch (failure: CancellationException) {
                resetFailure.compareAndSet(null, failure)
            } catch (failure: Error) {
                resetFailure.compareAndSet(null, failure)
            } catch (_: Exception) {
                // Reset is best effort after the logical stream is locally invalidated.
            } finally {
                resetDone.complete(Unit)
                cleanupJob.cancel()
            }
        }
    }

    suspend fun awaitReset() {
        if (!closed.get()) return
        resetDone.await()
        resetFailure.get()?.let { throw it }
    }

    val isClosed: Boolean
        get() = closed.get()

    override fun toString(): String = "YamuxStreamBlockingIo(closed=${closed.get()})"

    private fun requireRange(size: Int, offset: Int, length: Int) {
        require(offset >= 0 && length >= 0 && offset <= size - length)
    }

    private fun checkOpen() {
        if (closed.get()) throw FrpTransportException(FrpTransportFailure.CLOSED)
    }

}
