package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux

import java.io.ByteArrayOutputStream
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

internal class YamuxTestTransport(
    private val writeGate: Deferred<Unit>? = null,
    private val writeGateAfterStarts: Int = 0,
    private val yieldBetweenWrittenBytes: Boolean = false,
    private val writeFailure: Throwable? = null,
    private val writeFailureOnStart: Int? = null,
    private val closeFailure: Throwable? = null,
    private val beforeWrite: ((Int, ByteArray, Int, Int) -> Unit)? = null,
) : YamuxPhysicalTransport {
    init {
        require(writeGateAfterStarts >= 0)
        require(writeFailureOnStart == null || writeFailureOnStart > 0)
    }

    private val inbound = Channel<Inbound>(Channel.UNLIMITED)
    private val outputSignal = Channel<Unit>(Channel.CONFLATED)
    private val writeStartSignal = Channel<Unit>(Channel.CONFLATED)
    private val outputLock = Any()
    private val output = ByteArrayOutputStream()
    private val closed = AtomicBoolean(false)
    private val writeStarts = AtomicInteger(0)
    private val activeReads = AtomicInteger(0)
    private val activeWrites = AtomicInteger(0)

    private var currentInput: ByteArray? = null
    private var currentOffset = 0

    override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int {
        require(offset >= 0 && length >= 0 && offset <= destination.size - length)
        if (length == 0) return 0
        activeReads.incrementAndGet()
        try {
            while (true) {
                val current = currentInput
                if (current != null) {
                    val count = minOf(length, current.size - currentOffset)
                    current.copyInto(destination, offset, currentOffset, currentOffset + count)
                    currentOffset += count
                    if (currentOffset == current.size) {
                        currentInput = null
                        currentOffset = 0
                    }
                    return count
                }
                if (closed.get()) return -1
                when (val item = inbound.receive()) {
                    is Inbound.Bytes -> {
                        currentInput = item.value
                        currentOffset = 0
                    }

                    is Inbound.Failure -> throw item.value
                    Inbound.End -> return -1
                }
            }
        } finally {
            activeReads.decrementAndGet()
        }
    }

    override suspend fun writeFully(source: ByteArray, offset: Int, length: Int) {
        require(offset >= 0 && length >= 0 && offset <= source.size - length)
        val writeStart = writeStarts.incrementAndGet()
        writeStartSignal.trySend(Unit)
        activeWrites.incrementAndGet()
        try {
            beforeWrite?.invoke(writeStart, source, offset, length)
            if (writeStart > writeGateAfterStarts) writeGate?.await()
            if (writeFailureOnStart == null || writeFailureOnStart == writeStart) {
                writeFailure?.let { throw it }
            }
            if (yieldBetweenWrittenBytes) {
                repeat(length) { index ->
                    synchronized(outputLock) {
                        output.write(source[offset + index].toInt())
                    }
                    outputSignal.trySend(Unit)
                    yield()
                }
            } else {
                synchronized(outputLock) {
                    output.write(source, offset, length)
                }
                outputSignal.trySend(Unit)
            }
        } finally {
            activeWrites.decrementAndGet()
        }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) inbound.trySend(Inbound.End)
        closeFailure?.let { throw it }
    }

    fun feed(bytes: ByteArray, chunkSizes: List<Int> = listOf(bytes.size.coerceAtLeast(1))) {
        check(!closed.get())
        require(chunkSizes.isNotEmpty() && chunkSizes.all { it > 0 })
        var position = 0
        var chunkIndex = 0
        while (position < bytes.size) {
            val count = minOf(chunkSizes[chunkIndex % chunkSizes.size], bytes.size - position)
            check(inbound.trySend(Inbound.Bytes(bytes.copyOfRange(position, position + count))).isSuccess)
            position += count
            chunkIndex++
        }
    }

    fun finishInput() {
        check(inbound.trySend(Inbound.End).isSuccess)
    }

    fun failInput(failure: Throwable) {
        check(inbound.trySend(Inbound.Failure(failure)).isSuccess)
    }

    fun writtenBytes(): ByteArray = synchronized(outputLock) { output.toByteArray() }

    suspend fun awaitWrittenSize(expectedSize: Int) {
        withTimeout(TEST_WAIT_MILLIS) {
            while (writtenBytes().size < expectedSize) outputSignal.receive()
        }
    }

    suspend fun awaitWriteStarts(expectedCount: Int) {
        withTimeout(TEST_WAIT_MILLIS) {
            while (writeStarts.get() < expectedCount) writeStartSignal.receive()
        }
    }

    val isClosed: Boolean
        get() = closed.get()

    val activeReadCount: Int
        get() = activeReads.get()

    val activeWriteCount: Int
        get() = activeWrites.get()

    val writeStartCount: Int
        get() = writeStarts.get()

    private sealed interface Inbound {
        data class Bytes(val value: ByteArray) : Inbound

        data class Failure(val value: Throwable) : Inbound

        data object End : Inbound
    }

    private companion object {
        const val TEST_WAIT_MILLIS = 5_000L
    }
}

internal data class YamuxTestFrame(
    val header: YamuxFrameHeader,
    val body: ByteArray,
) {
    override fun toString(): String = "YamuxTestFrame(header=$header, bodyLength=${body.size})"
}

internal fun yamuxFrameBytes(
    type: YamuxFrameType,
    flags: Int = 0,
    streamId: Long = 0L,
    length: Long = 0L,
    body: ByteArray? = null,
): ByteArray {
    val header = YamuxFrameCodec.encodeHeader(YamuxFrameHeader(type, flags, streamId, length))
    return if (body == null) header else header + body
}

internal suspend fun parseYamuxTrace(
    bytes: ByteArray,
    chunkSizes: List<Int> = listOf(bytes.size.coerceAtLeast(1)),
    maximumDataFrameSize: Int = MAX_FIXTURE_DATA_FRAME,
): List<YamuxTestFrame> {
    require(bytes.size <= MAX_FIXTURE_BYTES)
    require(maximumDataFrameSize > 0)
    val transport = YamuxTestTransport()
    transport.feed(bytes, chunkSizes)
    transport.finishInput()
    val headerScratch = ByteArray(YamuxProtocol.HEADER_SIZE)
    return buildList {
        while (true) {
            val header = YamuxFrameCodec.readHeaderOrNull(transport, headerScratch) ?: break
            val body = if (header.type == YamuxFrameType.DATA) {
                val payload = YamuxFrameCodec.readDataSegments(
                    transport = transport,
                    length = header.length,
                    maximumLength = maximumDataFrameSize,
                    segmentSize = FIXTURE_SEGMENT_SIZE,
                )
                ByteArray(payload.length).also { destination ->
                    var position = 0
                    payload.segments.forEach { segment ->
                        segment.copyInto(destination, position)
                        position += segment.size
                    }
                }
            } else {
                ByteArray(0)
            }
            add(YamuxTestFrame(header, body))
        }
    }
}

internal fun encodeYamuxTrace(frames: List<YamuxTestFrame>): ByteArray =
    ByteArrayOutputStream().also { output ->
        frames.forEach { frame ->
            output.write(YamuxFrameCodec.encodeHeader(frame.header))
            output.write(frame.body)
        }
    }.toByteArray()

internal fun loadYamuxFixture(path: String): ByteArray {
    val resource = "$YAMUX_CONTRACT_ROOT/$path"
    val stream = YamuxTestFrame::class.java.classLoader?.getResourceAsStream(resource)
        ?: throw AssertionError("Classpath resource is missing: $resource")
    return stream.use { it.readNBytes(MAX_FIXTURE_BYTES + 1) }.also { bytes ->
        check(bytes.size <= MAX_FIXTURE_BYTES)
    }
}

internal fun applyYamuxManifestMutation(id: String): ByteArray {
    val manifestBytes = loadContractResource("manifest.json")
    val manifest = FIXTURE_JSON.decodeFromString<YamuxContractManifest>(
        manifestBytes.decodeToString(throwOnInvalidSequence = true),
    )
    val mutation = manifest.mutations.singleOrNull { it.id == id }
        ?: throw AssertionError("Manifest mutation is missing: $id")
    require(mutation.source.startsWith("yamux/"))
    val source = loadYamuxFixture(mutation.source.removePrefix("yamux/"))
    return source.clone().also { mutated ->
        when (mutation.operation) {
            "set-byte" -> {
                val offset = requireNotNull(mutation.offset)
                require(offset in mutated.indices)
                mutated[offset] = requireNotNull(mutation.byteValue).toByte()
            }

            "xor-byte" -> {
                val offset = requireNotNull(mutation.offset)
                require(offset in mutated.indices)
                mutated[offset] = (mutated[offset].toInt() xor requireNotNull(mutation.mask)).toByte()
            }

            else -> throw AssertionError("Unsupported Yamux mutation operation: ${mutation.operation}")
        }
    }
}

private fun loadContractResource(path: String): ByteArray {
    val resource = "$CONTRACT_ROOT/$path"
    val stream = YamuxTestFrame::class.java.classLoader?.getResourceAsStream(resource)
        ?: throw AssertionError("Classpath resource is missing: $resource")
    return stream.use { it.readNBytes(MAX_FIXTURE_BYTES + 1) }.also { bytes ->
        check(bytes.size <= MAX_FIXTURE_BYTES)
    }
}

internal const val YAMUX_INITIAL_WINDOW = 262_144L
internal const val YAMUX_FRP_WINDOW = 6_291_456L
private const val CONTRACT_ROOT = "io/github/ycfeng/ocdeck/frpcstcpvisitor/contract/v1"
private const val YAMUX_CONTRACT_ROOT = "$CONTRACT_ROOT/yamux"
private const val MAX_FIXTURE_BYTES = 1 shl 20
private const val MAX_FIXTURE_DATA_FRAME = 6_291_456
private const val FIXTURE_SEGMENT_SIZE = 4_096

private val FIXTURE_JSON = Json { ignoreUnknownKeys = true }

@Serializable
private data class YamuxContractManifest(
    val mutations: List<YamuxContractMutation>,
)

@Serializable
private data class YamuxContractMutation(
    val id: String,
    val source: String,
    val operation: String,
    val offset: Int? = null,
    val byteValue: Int? = null,
    val mask: Int? = null,
)
