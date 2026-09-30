package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpSafeIOException
import java.io.InputStream
import java.io.OutputStream
import java.security.SecureRandom
import java.util.concurrent.CancellationException
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec
import org.bouncycastle.crypto.InvalidCipherTextException
import org.bouncycastle.crypto.modes.ChaCha20Poly1305
import org.bouncycastle.crypto.params.AEADParameters
import org.bouncycastle.crypto.params.KeyParameter

internal object FrpAeadRecordStreams {
    const val MAX_PLAINTEXT_LENGTH = 65_536
    const val TAG_LENGTH = 16
    const val MIN_CIPHERTEXT_LENGTH = TAG_LENGTH
    const val MAX_CIPHERTEXT_LENGTH = MAX_PLAINTEXT_LENGTH + TAG_LENGTH
    const val AES_GCM_MAX_RECORDS = 1L shl 32

    fun encrypting(
        output: OutputStream,
        key: ByteArray,
        algorithm: FrpAeadAlgorithm,
        secureRandom: SecureRandom = SecureRandom(),
    ): FrpAeadRecordOutputStream {
        val nonce = ByteArray(algorithm.nonceSize)
        try {
            try {
                secureRandom.nextBytes(nonce)
            } catch (exception: CancellationException) {
                throw exception
            } catch (exception: Exception) {
                throw FrpCryptoException(FrpCryptoFailure.IO_FAILURE)
            }
            return FrpAeadRecordOutputStream(output, key, algorithm, nonce)
        } finally {
            nonce.fill(0)
        }
    }

    fun decrypting(
        input: InputStream,
        key: ByteArray,
        algorithm: FrpAeadAlgorithm,
    ): FrpAeadRecordInputStream = FrpAeadRecordInputStream(input, key, algorithm)
}

internal object FrpCryptoTestHooks {
    @Volatile
    var onTemporaryBufferCleared: ((String, Int, Boolean, Boolean) -> Unit)? = null

    fun clearTemporaryBuffer(label: String, buffer: ByteArray) {
        val observer = onTemporaryBufferCleared
        val containedData = observer != null && buffer.any { it != 0.toByte() }
        buffer.fill(0)
        observer?.invoke(label, buffer.size, containedData, buffer.all { it == 0.toByte() })
    }
}

internal fun retainOrTrimTemporaryCryptoOutput(
    label: String,
    output: ByteArray,
    count: Int,
): ByteArray {
    if (count == output.size) {
        return output
    }
    return try {
        if (count < 0 || count > output.size) {
            throw IllegalStateException("invalid crypto output length")
        }
        output.copyOf(count)
    } finally {
        FrpCryptoTestHooks.clearTemporaryBuffer(label, output)
    }
}

internal class FrpAeadRecordOutputStream(
    private val output: OutputStream,
    sourceKey: ByteArray,
    private val algorithm: FrpAeadAlgorithm,
    sourceStreamNonce: ByteArray,
    initialRecordCount: Long = 0,
) : OutputStream() {
    private val ioLock = Any()
    private val lifecycle = FrpCryptoStreamLifecycle(FrpCryptoFailure.WRITE_FAILED)
    private val key: ByteArray
    private val streamNonce: ByteArray
    private val recordNonce: ByteArray
    @Volatile
    private var recordCount = initialRecordCount
    @Volatile
    private var nonceSent = false

    init {
        if (sourceKey.size != FrpV2Crypto.KEY_SIZE) {
            throw FrpCryptoException(FrpCryptoFailure.INVALID_KEY)
        }
        if (sourceStreamNonce.size != algorithm.nonceSize || initialRecordCount < 0) {
            throw FrpCryptoException(FrpCryptoFailure.INVALID_NONCE)
        }
        val keyCopy = sourceKey.copyOf()
        val streamNonceCopy = try {
            sourceStreamNonce.copyOf()
        } catch (failure: Throwable) {
            keyCopy.fill(0)
            throw failure
        }
        val recordNonceCopy = try {
            sourceStreamNonce.copyOf()
        } catch (failure: Throwable) {
            keyCopy.fill(0)
            streamNonceCopy.fill(0)
            throw failure
        }
        key = keyCopy
        streamNonce = streamNonceCopy
        recordNonce = recordNonceCopy
    }

    override fun write(value: Int) {
        val single = byteArrayOf(value.toByte())
        try {
            write(single, 0, 1)
        } finally {
            single.fill(0)
        }
    }

    override fun write(bytes: ByteArray, offset: Int, length: Int) {
        if (offset < 0 || length < 0 || offset > bytes.size - length) {
            throw IndexOutOfBoundsException()
        }
        if (length == 0) {
            runCryptoStreamOperation(lifecycle, ioLock) { Unit }
            return
        }
        runCryptoStreamOperation(lifecycle, ioLock) {
            var position = offset
            val end = offset + length
            while (position < end) {
                val chunkLength = minOf(end - position, FrpAeadRecordStreams.MAX_PLAINTEXT_LENGTH)
                writeRecord(bytes, position, chunkLength)
                position += chunkLength
            }
        }
    }

    override fun flush() {
        runCryptoStreamOperation(lifecycle, ioLock) {
            output.flush()
        }
    }

    override fun close() {
        when (lifecycle.beginClose()) {
            FrpStreamCloseRole.ALREADY_CLOSED -> return
            FrpStreamCloseRole.WAITER -> {
                lifecycle.awaitClosed()?.let { throw it }
                return
            }
            FrpStreamCloseRole.OWNER -> Unit
        }
        var primaryFailure = lifecycle.recordedFailure()
        try {
            try {
                output.close()
            } catch (failure: Throwable) {
                primaryFailure = combineCryptoFailures(
                    primaryFailure,
                    failure,
                    FrpCryptoFailure.WRITE_FAILED,
                )
            }
            primaryFailure = combineCryptoFailures(
                primaryFailure,
                lifecycle.awaitOperations(),
                FrpCryptoFailure.WRITE_FAILED,
            )
            primaryFailure = combineCryptoFailures(
                primaryFailure,
                lifecycle.recordedFailure(),
                FrpCryptoFailure.WRITE_FAILED,
            )
        } finally {
            synchronized(ioLock) {
                key.fill(0)
                streamNonce.fill(0)
                recordNonce.fill(0)
            }
            lifecycle.finishClose()
        }
        primaryFailure?.let { throw it }
    }

    override fun toString(): String =
        "FrpAeadRecordOutputStream(algorithm=${algorithm.wireName}, nonceSent=$nonceSent, " +
            "recordCount=$recordCount, failed=${lifecycle.isFailed()})"

    private fun writeRecord(bytes: ByteArray, offset: Int, length: Int) {
        if (algorithm == FrpAeadAlgorithm.AES_256_GCM &&
            recordCount >= FrpAeadRecordStreams.AES_GCM_MAX_RECORDS
        ) {
            throw FrpCryptoException(FrpCryptoFailure.RECORD_LIMIT)
        }
        if (!nonceSent) {
            output.write(streamNonce)
            nonceSent = true
        }

        val ciphertextLength = length + FrpAeadRecordStreams.TAG_LENGTH
        val header = encodeLength(ciphertextLength)
        val aad = ByteArray(streamNonce.size + header.size)
        streamNonce.copyInto(aad)
        header.copyInto(aad, destinationOffset = streamNonce.size)
        val plaintext = bytes.copyOfRange(offset, offset + length)
        val ciphertext = try {
            encryptRecord(algorithm, key, recordNonce, aad, plaintext)
        } finally {
            plaintext.fill(0)
            aad.fill(0)
        }
        try {
            if (!incrementNonce(recordNonce)) {
                throw FrpCryptoException(FrpCryptoFailure.NONCE_EXHAUSTED)
            }
            recordCount++
            output.write(header)
            output.write(ciphertext)
        } finally {
            ciphertext.fill(0)
        }
    }
}

internal class FrpAeadRecordInputStream(
    private val input: InputStream,
    sourceKey: ByteArray,
    private val algorithm: FrpAeadAlgorithm,
    initialRecordCount: Long = 0,
) : InputStream() {
    private val ioLock = Any()
    private val lifecycle = FrpCryptoStreamLifecycle(FrpCryptoFailure.IO_FAILURE)
    private val key: ByteArray
    @Volatile
    private var streamNonce: ByteArray? = null
    private var recordNonce: ByteArray? = null
    @Volatile
    private var recordCount = initialRecordCount
    private var pending = ByteArray(0)
    private var pendingOffset = 0
    private var eof = false

    init {
        if (sourceKey.size != FrpV2Crypto.KEY_SIZE) {
            throw FrpCryptoException(FrpCryptoFailure.INVALID_KEY)
        }
        if (initialRecordCount < 0) {
            throw FrpCryptoException(FrpCryptoFailure.INVALID_NONCE)
        }
        key = sourceKey.copyOf()
    }

    override fun read(): Int {
        val single = ByteArray(1)
        return try {
            val count = read(single, 0, 1)
            if (count < 0) -1 else single[0].toInt() and 0xff
        } finally {
            single.fill(0)
        }
    }

    override fun read(bytes: ByteArray, offset: Int, length: Int): Int {
        if (offset < 0 || length < 0 || offset > bytes.size - length) {
            throw IndexOutOfBoundsException()
        }
        return runCryptoStreamOperation(lifecycle, ioLock) {
            try {
                if (length == 0) {
                    0
                } else if (eof) {
                    -1
                } else {
                    while (pendingOffset >= pending.size) {
                        pending.fill(0)
                        pending = ByteArray(0)
                        pendingOffset = 0
                        if (!readRecord()) {
                            eof = true
                            clearSensitiveState()
                            return@runCryptoStreamOperation -1
                        }
                    }
                    val count = minOf(length, pending.size - pendingOffset)
                    pending.copyInto(
                        bytes,
                        destinationOffset = offset,
                        startIndex = pendingOffset,
                        endIndex = pendingOffset + count,
                    )
                    pendingOffset += count
                    count
                }
            } catch (failure: Throwable) {
                clearSensitiveState()
                throw failure
            }
        }
    }

    override fun close() {
        when (lifecycle.beginClose()) {
            FrpStreamCloseRole.ALREADY_CLOSED -> return
            FrpStreamCloseRole.WAITER -> {
                lifecycle.awaitClosed()?.let { throw it }
                return
            }
            FrpStreamCloseRole.OWNER -> Unit
        }
        val failureBeforeClose = lifecycle.recordedFailure()
        var closeFailure: Throwable? = null
        try {
            try {
                input.close()
            } catch (failure: Throwable) {
                closeFailure = combineCryptoFailures(
                    closeFailure,
                    failure,
                    FrpCryptoFailure.IO_FAILURE,
                )
            }
            closeFailure = combineCryptoFailures(
                closeFailure,
                lifecycle.awaitOperations(),
                FrpCryptoFailure.IO_FAILURE,
            )
            if (failureBeforeClose == null) {
                // Earlier read failures were already delivered; rethrowing them from use-close self-suppresses.
                closeFailure = combineCryptoFailures(
                    closeFailure,
                    lifecycle.recordedFailure(),
                    FrpCryptoFailure.IO_FAILURE,
                )
            }
        } finally {
            synchronized(ioLock) {
                clearSensitiveState()
            }
            lifecycle.finishClose()
        }
        closeFailure?.let { throw it }
    }

    override fun toString(): String =
        "FrpAeadRecordInputStream(algorithm=${algorithm.wireName}, initialized=${streamNonce != null}, " +
            "recordCount=$recordCount, failed=${lifecycle.isFailed()})"

    private fun readRecord(): Boolean {
        if (!readStreamNonce()) {
            return false
        }
        val header = ByteArray(RECORD_HEADER_SIZE)
        val first = readRawByte()
        if (first < 0) {
            return false
        }
        header[0] = first.toByte()
        readRawFully(header, 1, header.size - 1, FrpCryptoFailure.TRUNCATED_HEADER)
        if (algorithm == FrpAeadAlgorithm.AES_256_GCM &&
            recordCount >= FrpAeadRecordStreams.AES_GCM_MAX_RECORDS
        ) {
            throw FrpCryptoException(FrpCryptoFailure.RECORD_LIMIT)
        }

        val ciphertextLength = decodeLength(header)
        if (ciphertextLength < FrpAeadRecordStreams.MIN_CIPHERTEXT_LENGTH.toLong() ||
            ciphertextLength > FrpAeadRecordStreams.MAX_CIPHERTEXT_LENGTH.toLong()
        ) {
            throw FrpCryptoException(FrpCryptoFailure.INVALID_LENGTH)
        }
        val ciphertext = ByteArray(ciphertextLength.toInt())
        try {
            readRawFully(ciphertext, 0, ciphertext.size, FrpCryptoFailure.TRUNCATED_RECORD)
        } catch (failure: Throwable) {
            ciphertext.fill(0)
            throw failure
        }
        var plaintext: ByteArray? = null
        var plaintextRetained = false
        try {
            val nonce = recordNonce ?: throw FrpCryptoException(FrpCryptoFailure.INVALID_NONCE)
            val activeStreamNonce = streamNonce ?: throw FrpCryptoException(FrpCryptoFailure.INVALID_NONCE)
            val aad = ByteArray(activeStreamNonce.size + header.size)
            val decrypted = try {
                activeStreamNonce.copyInto(aad)
                header.copyInto(aad, destinationOffset = activeStreamNonce.size)
                decryptRecord(algorithm, key, nonce, aad, ciphertext)
            } finally {
                aad.fill(0)
            }
            plaintext = decrypted
            if (!incrementNonce(nonce)) {
                throw FrpCryptoException(FrpCryptoFailure.NONCE_EXHAUSTED)
            }
            recordCount++
            pending = decrypted
            plaintextRetained = true
            return true
        } finally {
            ciphertext.fill(0)
            if (!plaintextRetained) {
                plaintext?.fill(0)
            }
        }
    }

    private fun readStreamNonce(): Boolean {
        if (streamNonce != null) {
            return true
        }
        val nonce = ByteArray(algorithm.nonceSize)
        var retained = false
        try {
            val first = readRawByte()
            if (first < 0) {
                return false
            }
            nonce[0] = first.toByte()
            readRawFully(nonce, 1, nonce.size - 1, FrpCryptoFailure.TRUNCATED_NONCE)
            streamNonce = nonce.copyOf()
            recordNonce = nonce
            retained = true
            return true
        } finally {
            if (!retained) {
                nonce.fill(0)
            }
        }
    }

    private fun readRawByte(): Int =
        try {
            input.read()
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: FrpSafeIOException) {
            throw exception
        } catch (exception: Exception) {
            throw FrpCryptoException(FrpCryptoFailure.IO_FAILURE)
        }

    private fun readRawFully(
        destination: ByteArray,
        offset: Int,
        length: Int,
        failure: FrpCryptoFailure,
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
                throw FrpCryptoException(FrpCryptoFailure.IO_FAILURE)
            }
            if (count < 0) {
                throw FrpCryptoException(failure)
            }
            if (count == 0) {
                val single = readRawByte()
                if (single < 0) {
                    throw FrpCryptoException(failure)
                }
                destination[position++] = single.toByte()
            } else {
                position += count
            }
        }
    }

    private fun clearSensitiveState() {
        key.fill(0)
        pending.fill(0)
        pending = ByteArray(0)
        pendingOffset = 0
        streamNonce?.fill(0)
        streamNonce = null
        recordNonce?.fill(0)
        recordNonce = null
    }
}

private fun encodeLength(length: Int): ByteArray = byteArrayOf(
    (length ushr 24).toByte(),
    (length ushr 16).toByte(),
    (length ushr 8).toByte(),
    length.toByte(),
)

private fun decodeLength(header: ByteArray): Long =
    ((header[0].toLong() and 0xffL) shl 24) or
        ((header[1].toLong() and 0xffL) shl 16) or
        ((header[2].toLong() and 0xffL) shl 8) or
        (header[3].toLong() and 0xffL)

private fun incrementNonce(nonce: ByteArray): Boolean {
    for (index in nonce.indices.reversed()) {
        nonce[index]++
        if (nonce[index].toInt() != 0) {
            return true
        }
    }
    return false
}

private fun encryptRecord(
    algorithm: FrpAeadAlgorithm,
    key: ByteArray,
    nonce: ByteArray,
    aad: ByteArray,
    plaintext: ByteArray,
): ByteArray = when (algorithm) {
    FrpAeadAlgorithm.AES_256_GCM -> aesGcm(true, key, nonce, aad, plaintext)
    FrpAeadAlgorithm.XCHACHA20_POLY1305 -> xChaCha20Poly1305(true, key, nonce, aad, plaintext)
}

private fun decryptRecord(
    algorithm: FrpAeadAlgorithm,
    key: ByteArray,
    nonce: ByteArray,
    aad: ByteArray,
    ciphertext: ByteArray,
): ByteArray = when (algorithm) {
    FrpAeadAlgorithm.AES_256_GCM -> aesGcm(false, key, nonce, aad, ciphertext)
    FrpAeadAlgorithm.XCHACHA20_POLY1305 -> xChaCha20Poly1305(false, key, nonce, aad, ciphertext)
}

private fun aesGcm(
    encrypt: Boolean,
    key: ByteArray,
    nonce: ByteArray,
    aad: ByteArray,
    input: ByteArray,
): ByteArray {
    var temporaryOutput: ByteArray? = null
    var returnTemporaryOutput = false
    return try {
        val cipher = Cipher.getInstance("AES/GCM/NoPadding").apply {
            init(
                if (encrypt) Cipher.ENCRYPT_MODE else Cipher.DECRYPT_MODE,
                SecretKeySpec(key, "AES"),
                GCMParameterSpec(FrpAeadRecordStreams.TAG_LENGTH * 8, nonce),
            )
            updateAAD(aad)
        }
        val output = ByteArray(cipher.getOutputSize(input.size))
        temporaryOutput = output
        val count = cipher.doFinal(input, 0, input.size, output, 0)
        val result = try {
            retainOrTrimTemporaryCryptoOutput("aes-gcm-output", output, count)
        } finally {
            if (count != output.size) {
                temporaryOutput = null
            }
        }
        returnTemporaryOutput = result === output
        result
    } catch (exception: CancellationException) {
        throw exception
    } catch (exception: Exception) {
        throw FrpCryptoException(
            if (encrypt) FrpCryptoFailure.UNSUPPORTED_ALGORITHM else FrpCryptoFailure.INTEGRITY_FAILURE,
        )
    } finally {
        if (!returnTemporaryOutput) {
            temporaryOutput?.let { FrpCryptoTestHooks.clearTemporaryBuffer("aes-gcm-output", it) }
        }
    }
}

private fun xChaCha20Poly1305(
    encrypt: Boolean,
    key: ByteArray,
    nonce: ByteArray,
    aad: ByteArray,
    input: ByteArray,
): ByteArray {
    if (key.size != FrpV2Crypto.KEY_SIZE || nonce.size != FrpAeadAlgorithm.XCHACHA20_POLY1305.nonceSize) {
        throw FrpCryptoException(FrpCryptoFailure.INVALID_NONCE)
    }
    val noncePrefix = nonce.copyOfRange(0, 16)
    val subKey = try {
        hChaCha20(key, noncePrefix)
    } finally {
        noncePrefix.fill(0)
    }
    var ietfNonce: ByteArray? = null
    var temporaryOutput: ByteArray? = null
    var returnTemporaryOutput = false
    return try {
        val activeNonce = ByteArray(12)
        ietfNonce = activeNonce
        nonce.copyInto(activeNonce, destinationOffset = 4, startIndex = 16, endIndex = 24)
        val cipher = ChaCha20Poly1305()
        cipher.init(encrypt, AEADParameters(KeyParameter(subKey), 128, activeNonce, aad))
        val output = ByteArray(cipher.getOutputSize(input.size))
        temporaryOutput = output
        var count = cipher.processBytes(input, 0, input.size, output, 0)
        count += cipher.doFinal(output, count)
        val result = try {
            retainOrTrimTemporaryCryptoOutput("xchacha-output", output, count)
        } finally {
            if (count != output.size) {
                temporaryOutput = null
            }
        }
        returnTemporaryOutput = result === output
        result
    } catch (exception: CancellationException) {
        throw exception
    } catch (exception: InvalidCipherTextException) {
        throw FrpCryptoException(FrpCryptoFailure.INTEGRITY_FAILURE)
    } catch (exception: Exception) {
        throw FrpCryptoException(
            if (encrypt) FrpCryptoFailure.UNSUPPORTED_ALGORITHM else FrpCryptoFailure.INTEGRITY_FAILURE,
        )
    } finally {
        if (!returnTemporaryOutput) {
            temporaryOutput?.let { FrpCryptoTestHooks.clearTemporaryBuffer("xchacha-output", it) }
        }
        subKey.fill(0)
        ietfNonce?.fill(0)
    }
}

private fun hChaCha20(key: ByteArray, nonce: ByteArray): ByteArray {
    if (key.size != 32 || nonce.size != 16) {
        throw FrpCryptoException(FrpCryptoFailure.INVALID_NONCE)
    }
    val state = IntArray(16)
    try {
        state[0] = 0x61707865
        state[1] = 0x3320646e
        state[2] = 0x79622d32
        state[3] = 0x6b206574
        for (index in 0 until 8) {
            state[4 + index] = littleEndianInt(key, index * 4)
        }
        for (index in 0 until 4) {
            state[12 + index] = littleEndianInt(nonce, index * 4)
        }
        repeat(10) {
            quarterRound(state, 0, 4, 8, 12)
            quarterRound(state, 1, 5, 9, 13)
            quarterRound(state, 2, 6, 10, 14)
            quarterRound(state, 3, 7, 11, 15)
            quarterRound(state, 0, 5, 10, 15)
            quarterRound(state, 1, 6, 11, 12)
            quarterRound(state, 2, 7, 8, 13)
            quarterRound(state, 3, 4, 9, 14)
        }
        val output = ByteArray(32)
        val words = intArrayOf(
            state[0],
            state[1],
            state[2],
            state[3],
            state[12],
            state[13],
            state[14],
            state[15],
        )
        var outputComplete = false
        try {
            words.forEachIndexed { index, value -> putLittleEndianInt(value, output, index * 4) }
            outputComplete = true
            return output
        } finally {
            words.fill(0)
            if (!outputComplete) {
                output.fill(0)
            }
        }
    } finally {
        state.fill(0)
    }
}

private fun quarterRound(state: IntArray, a: Int, b: Int, c: Int, d: Int) {
    state[a] += state[b]
    state[d] = Integer.rotateLeft(state[d] xor state[a], 16)
    state[c] += state[d]
    state[b] = Integer.rotateLeft(state[b] xor state[c], 12)
    state[a] += state[b]
    state[d] = Integer.rotateLeft(state[d] xor state[a], 8)
    state[c] += state[d]
    state[b] = Integer.rotateLeft(state[b] xor state[c], 7)
}

private fun littleEndianInt(bytes: ByteArray, offset: Int): Int =
    (bytes[offset].toInt() and 0xff) or
        ((bytes[offset + 1].toInt() and 0xff) shl 8) or
        ((bytes[offset + 2].toInt() and 0xff) shl 16) or
        ((bytes[offset + 3].toInt() and 0xff) shl 24)

private fun putLittleEndianInt(value: Int, destination: ByteArray, offset: Int) {
    destination[offset] = value.toByte()
    destination[offset + 1] = (value ushr 8).toByte()
    destination[offset + 2] = (value ushr 16).toByte()
    destination[offset + 3] = (value ushr 24).toByte()
}

private const val RECORD_HEADER_SIZE = 4
