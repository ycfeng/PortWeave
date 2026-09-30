package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpMuxStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.CancellationException

internal class ClearingByteArrayOutputStream(initialCapacity: Int) : ByteArrayOutputStream(initialCapacity) {
    fun copyAndClear(): ByteArray = try {
        toByteArray()
    } finally {
        clear()
    }

    fun clear() {
        buf.fill(0)
        reset()
    }
}

internal fun readByteSafely(input: InputStream): Int =
    try {
        input.read()
    } catch (exception: CancellationException) {
        throw exception
    } catch (exception: FrpSafeIOException) {
        throw exception
    } catch (exception: Exception) {
        throw FrpProtocolException(FrpProtocolFailure.IO_FAILURE)
    }

internal fun readFullySafely(
    input: InputStream,
    destination: ByteArray,
    offset: Int = 0,
    length: Int = destination.size - offset,
    truncatedFailure: FrpProtocolFailure,
) {
    var position = offset
    val end = offset + length
    while (position < end) {
        val count = try {
            input.read(destination, position, end - position)
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: FrpSafeIOException) {
            throw exception
        } catch (exception: Exception) {
            throw FrpProtocolException(FrpProtocolFailure.IO_FAILURE)
        }
        if (count < 0) {
            throw FrpProtocolException(truncatedFailure)
        }
        if (count == 0) {
            val single = readByteSafely(input)
            if (single < 0) {
                throw FrpProtocolException(truncatedFailure)
            }
            destination[position++] = single.toByte()
        } else {
            position += count
        }
    }
}

internal fun writeSafely(output: OutputStream, bytes: ByteArray) {
    try {
        output.write(bytes)
    } catch (exception: CancellationException) {
        throw exception
    } catch (exception: FrpSafeIOException) {
        throw exception
    } catch (exception: Exception) {
        throw FrpProtocolException(FrpProtocolFailure.WRITE_FAILED)
    }
}

internal fun writeSafely(output: OutputStream, value: Int) {
    try {
        output.write(value)
    } catch (exception: CancellationException) {
        throw exception
    } catch (exception: FrpSafeIOException) {
        throw exception
    } catch (exception: Exception) {
        throw FrpProtocolException(FrpProtocolFailure.WRITE_FAILED)
    }
}

internal suspend fun readByteSafely(input: FrpMuxStream): Int {
    val single = ByteArray(1)
    return try {
        val count = try {
            input.read(single, 0, 1)
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: FrpSafeIOException) {
            throw exception
        } catch (exception: Exception) {
            throw FrpProtocolException(FrpProtocolFailure.IO_FAILURE)
        }
        when {
            count < 0 -> -1
            count == 0 -> throw FrpProtocolException(FrpProtocolFailure.IO_FAILURE)
            else -> single[0].toInt() and 0xff
        }
    } finally {
        single.fill(0)
    }
}

internal suspend fun readFullySafely(
    input: FrpMuxStream,
    destination: ByteArray,
    offset: Int = 0,
    length: Int = destination.size - offset,
    truncatedFailure: FrpProtocolFailure,
) {
    var position = offset
    val end = offset + length
    while (position < end) {
        val count = try {
            input.read(destination, position, end - position)
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: FrpSafeIOException) {
            throw exception
        } catch (exception: Exception) {
            throw FrpProtocolException(FrpProtocolFailure.IO_FAILURE)
        }
        if (count < 0) throw FrpProtocolException(truncatedFailure)
        if (count == 0) throw FrpProtocolException(FrpProtocolFailure.IO_FAILURE)
        position += count
    }
}

internal suspend fun writeSafely(output: FrpMuxStream, bytes: ByteArray) {
    try {
        output.write(bytes)
    } catch (exception: CancellationException) {
        throw exception
    } catch (exception: FrpSafeIOException) {
        throw exception
    } catch (exception: Exception) {
        throw FrpProtocolException(FrpProtocolFailure.WRITE_FAILED)
    }
}
