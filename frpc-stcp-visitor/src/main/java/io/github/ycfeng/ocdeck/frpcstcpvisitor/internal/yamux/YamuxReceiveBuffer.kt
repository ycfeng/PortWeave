package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux

import java.util.ArrayDeque

internal class YamuxReceiveBuffer {
    private val blocks = ArrayDeque<ByteArray>()
    private var headOffset = 0
    private var tailLength = 0
    private var size = 0L

    val byteCount: Long
        get() = size

    val blockCount: Int
        get() = blocks.size

    fun appendFrame(payload: YamuxPayloadSegments) {
        require(payload.length >= 0)
        require(payload.segments.sumOf(ByteArray::size) == payload.length)
        require(payload.segments.all(ByteArray::isNotEmpty))
        payload.segments.forEach { source ->
            var sourceOffset = 0
            while (sourceOffset < source.size) {
                if (blocks.isEmpty() || tailLength == BLOCK_SIZE) {
                    blocks.addLast(ByteArray(BLOCK_SIZE))
                    tailLength = 0
                }
                val count = minOf(source.size - sourceOffset, BLOCK_SIZE - tailLength)
                source.copyInto(
                    destination = blocks.last(),
                    destinationOffset = tailLength,
                    startIndex = sourceOffset,
                    endIndex = sourceOffset + count,
                )
                sourceOffset += count
                tailLength += count
            }
        }
        size += payload.length.toLong()
    }

    fun read(destination: ByteArray, offset: Int, length: Int): Int {
        require(offset >= 0 && length >= 0 && offset <= destination.size - length)
        if (length == 0 || size == 0L) return 0
        var outputOffset = offset
        var remaining = minOf(length.toLong(), size).toInt()
        val result = remaining
        while (remaining > 0) {
            val block = blocks.first()
            val blockEnd = if (blocks.size == 1) tailLength else BLOCK_SIZE
            val count = minOf(remaining, blockEnd - headOffset)
            block.copyInto(
                destination = destination,
                destinationOffset = outputOffset,
                startIndex = headOffset,
                endIndex = headOffset + count,
            )
            headOffset += count
            outputOffset += count
            remaining -= count
            size -= count.toLong()
            if (headOffset == blockEnd) {
                blocks.removeFirst()
                headOffset = 0
                if (blocks.isEmpty()) tailLength = 0
            }
        }
        return result
    }

    fun clear(): Long {
        val released = size
        blocks.clear()
        headOffset = 0
        tailLength = 0
        size = 0L
        return released
    }

    override fun toString(): String =
        "YamuxReceiveBuffer(blockCount=${blocks.size}, byteCount=$size)"

    internal companion object {
        const val BLOCK_SIZE = 16 * 1024
    }
}
