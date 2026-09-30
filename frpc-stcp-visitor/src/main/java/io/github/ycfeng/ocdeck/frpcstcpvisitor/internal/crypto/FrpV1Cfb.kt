package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto

import android.annotation.SuppressLint
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpSafeIOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.concurrent.CancellationException
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.bouncycastle.crypto.digests.SHA1Digest
import org.bouncycastle.crypto.generators.PKCS5S2ParametersGenerator
import org.bouncycastle.crypto.params.KeyParameter

internal object FrpV1Cfb {
    const val IV_SIZE = 16

    fun encrypting(
        output: OutputStream,
        password: String,
        secureRandom: SecureRandom = SecureRandom(),
    ): FrpV1CfbOutputStream {
        val iv = ByteArray(IV_SIZE)
        try {
            try {
                secureRandom.nextBytes(iv)
            } catch (exception: CancellationException) {
                throw exception
            } catch (exception: Exception) {
                throw FrpCryptoException(FrpCryptoFailure.IO_FAILURE)
            }
            return FrpV1CfbOutputStream(output, password, iv)
        } finally {
            iv.fill(0)
        }
    }

    fun decrypting(input: InputStream, password: String): FrpV1CfbInputStream =
        FrpV1CfbInputStream(input, password)

    internal fun deriveKey(password: String): ByteArray {
        val passwordBytes = password.toByteArray(StandardCharsets.UTF_8)
        return try {
            val generator = PKCS5S2ParametersGenerator(SHA1Digest())
            generator.init(passwordBytes, SALT, ITERATIONS)
            (generator.generateDerivedParameters(KEY_BITS) as KeyParameter).key.copyOf()
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: Exception) {
            throw FrpCryptoException(FrpCryptoFailure.INVALID_KEY)
        } finally {
            passwordBytes.fill(0)
        }
    }

    internal fun newCipher(mode: Int, key: ByteArray, iv: ByteArray): FrpV1CfbCipher {
        if (key.size != IV_SIZE || iv.size != IV_SIZE) {
            throw FrpCryptoException(FrpCryptoFailure.INVALID_KEY)
        }
        return try {
            val encrypt = when (mode) {
                Cipher.ENCRYPT_MODE -> true
                Cipher.DECRYPT_MODE -> false
                else -> throw FrpCryptoException(FrpCryptoFailure.UNSUPPORTED_ALGORITHM)
            }
            FrpV1CfbCipher(key, iv, encrypt)
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: FrpCryptoException) {
            throw exception
        } catch (exception: Exception) {
            throw FrpCryptoException(FrpCryptoFailure.UNSUPPORTED_ALGORITHM)
        }
    }

    private const val ITERATIONS = 64
    private const val KEY_BITS = 128
    private val SALT = "frp".toByteArray(StandardCharsets.US_ASCII)
}

internal class FrpV1CfbCipher(
    key: ByteArray,
    sourceIv: ByteArray,
    private val encrypt: Boolean,
) {
    private val feedback = sourceIv.copyOf()
    private val keyStream = ByteArray(FrpV1Cfb.IV_SIZE)
    private var blockCipher: Cipher? = null
    private var keyStreamOffset = keyStream.size

    init {
        try {
            blockCipher = newAesBlockCipher(key)
        } catch (failure: Throwable) {
            feedback.fill(0)
            keyStream.fill(0)
            blockCipher = null
            throw failure
        }
    }

    // CFB needs raw AES block encryption; ECB is the primitive here, never the wire data mode.
    @SuppressLint("GetInstance")
    private fun newAesBlockCipher(key: ByteArray): Cipher =
        Cipher.getInstance("AES/ECB/NoPadding").apply {
            init(Cipher.ENCRYPT_MODE, SecretKeySpec(key, "AES"))
        }

    fun process(input: ByteArray, offset: Int, length: Int): ByteArray {
        val activeCipher = blockCipher ?: throw FrpCryptoException(FrpCryptoFailure.IO_FAILURE)
        val output = ByteArray(length)
        try {
            var inputOffset = offset
            var outputOffset = 0
            var remaining = length
            while (remaining > 0) {
                if (keyStreamOffset == keyStream.size) {
                    val nextKeyStream = activeCipher.doFinal(feedback)
                    try {
                        if (nextKeyStream.size != keyStream.size) {
                            throw FrpCryptoException(FrpCryptoFailure.UNSUPPORTED_ALGORITHM)
                        }
                        nextKeyStream.copyInto(keyStream)
                    } finally {
                        nextKeyStream.fill(0)
                    }
                    keyStreamOffset = 0
                }
                val count = minOf(remaining, keyStream.size - keyStreamOffset)
                repeat(count) { index ->
                    val source = input[inputOffset + index]
                    val transformed = (source.toInt() xor keyStream[keyStreamOffset + index].toInt()).toByte()
                    output[outputOffset + index] = transformed
                    feedback[keyStreamOffset + index] = if (encrypt) transformed else source
                }
                inputOffset += count
                outputOffset += count
                remaining -= count
                keyStreamOffset += count
            }
            return output
        } catch (failure: Throwable) {
            output.fill(0)
            destroy()
            throw failure
        }
    }

    fun destroy() {
        feedback.fill(0)
        keyStream.fill(0)
        blockCipher = null
        keyStreamOffset = keyStream.size
    }
}

internal class FrpV1CfbOutputStream(
    private val output: OutputStream,
    password: String,
    sourceIv: ByteArray,
) : OutputStream() {
    private val ioLock = Any()
    private val lifecycle = FrpCryptoStreamLifecycle(FrpCryptoFailure.WRITE_FAILED)
    private val iv: ByteArray
    private var cipher: FrpV1CfbCipher? = null
    @Volatile
    private var ivSent = false

    init {
        if (sourceIv.size != FrpV1Cfb.IV_SIZE) {
            throw FrpCryptoException(FrpCryptoFailure.INVALID_NONCE)
        }
        val ivCopy = sourceIv.copyOf()
        val key = FrpV1Cfb.deriveKey(password)
        try {
            cipher = FrpV1Cfb.newCipher(Cipher.ENCRYPT_MODE, key, ivCopy)
            iv = ivCopy
        } catch (failure: Throwable) {
            ivCopy.fill(0)
            cipher?.destroy()
            cipher = null
            throw failure
        } finally {
            key.fill(0)
        }
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
        runCryptoStreamOperation(lifecycle, ioLock) {
            sendIvIfNeeded()
            val activeCipher = cipher ?: throw FrpCryptoException(FrpCryptoFailure.WRITE_FAILED)
            var position = offset
            val end = offset + length
            while (position < end) {
                val chunk = minOf(end - position, BUFFER_SIZE)
                val encrypted = activeCipher.process(bytes, position, chunk)
                try {
                    if (encrypted.isNotEmpty()) {
                        output.write(encrypted)
                    }
                } finally {
                    encrypted.fill(0)
                }
                position += chunk
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
                iv.fill(0)
                cipher?.destroy()
                cipher = null
            }
            lifecycle.finishClose()
        }
        primaryFailure?.let { throw it }
    }

    override fun toString(): String =
        "FrpV1CfbOutputStream(ivSent=$ivSent, failed=${lifecycle.isFailed()}, authenticated=false)"

    private fun sendIvIfNeeded() {
        if (!ivSent) {
            output.write(iv)
            ivSent = true
        }
    }

    private companion object {
        const val BUFFER_SIZE = 8 * 1_024
    }
}

internal class FrpV1CfbInputStream(
    private val input: InputStream,
    password: String,
) : InputStream() {
    private val ioLock = Any()
    private val lifecycle = FrpCryptoStreamLifecycle(FrpCryptoFailure.IO_FAILURE)
    private val key = FrpV1Cfb.deriveKey(password)
    @Volatile
    private var cipher: FrpV1CfbCipher? = null
    private var pending = ByteArray(0)
    private var pendingOffset = 0
    private var eof = false

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
                when {
                    length == 0 -> 0
                    else -> {
                        while (pendingOffset >= pending.size) {
                            pending.fill(0)
                            pending = ByteArray(0)
                            pendingOffset = 0
                            if (eof || !initialize() || !readPending()) {
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
        "FrpV1CfbInputStream(initialized=${cipher != null}, failed=${lifecycle.isFailed()}, " +
            "authenticated=false)"

    private fun initialize(): Boolean {
        if (cipher != null) {
            return true
        }
        val iv = ByteArray(FrpV1Cfb.IV_SIZE)
        try {
            val first = readRawByte()
            if (first < 0) {
                eof = true
                key.fill(0)
                return false
            }
            iv[0] = first.toByte()
            readRawFully(iv, 1, iv.size - 1, FrpCryptoFailure.TRUNCATED_NONCE)
            var initializedCipher: FrpV1CfbCipher? = FrpV1Cfb.newCipher(Cipher.DECRYPT_MODE, key, iv)
            try {
                key.fill(0)
                cipher = initializedCipher
                initializedCipher = null
                return true
            } finally {
                initializedCipher?.destroy()
            }
        } finally {
            iv.fill(0)
        }
    }

    private fun readPending(): Boolean {
        val encrypted = ByteArray(BUFFER_SIZE)
        try {
            while (true) {
                val count = readRawChunk(encrypted)
                if (count < 0) {
                    eof = true
                    cipher?.destroy()
                    cipher = null
                    return false
                }
                val plaintext = cipher?.process(encrypted, 0, count)
                    ?: throw FrpCryptoException(FrpCryptoFailure.IO_FAILURE)
                var retained = false
                try {
                    if (plaintext.isNotEmpty()) {
                        pending = plaintext
                        retained = true
                        return true
                    }
                } finally {
                    if (!retained) {
                        plaintext.fill(0)
                    }
                }
            }
        } finally {
            encrypted.fill(0)
        }
    }

    private fun readRawChunk(destination: ByteArray): Int {
        val count = try {
            input.read(destination, 0, destination.size)
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: FrpSafeIOException) {
            throw exception
        } catch (exception: Exception) {
            throw FrpCryptoException(FrpCryptoFailure.IO_FAILURE)
        }
        if (count != 0) {
            return count
        }
        val single = readRawByte()
        if (single < 0) {
            return -1
        }
        destination[0] = single.toByte()
        return 1
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
        cipher?.destroy()
        cipher = null
    }

    private companion object {
        const val BUFFER_SIZE = 8 * 1_024
    }
}
