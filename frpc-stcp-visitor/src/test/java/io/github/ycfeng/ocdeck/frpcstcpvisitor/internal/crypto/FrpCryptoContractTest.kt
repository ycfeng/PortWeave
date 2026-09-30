package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.FrpContractFixtures
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.PlannedChunkInputStream
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FRP_AEAD_AES_256_GCM
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FRP_AEAD_XCHACHA20_POLY1305
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpProtocolException
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpProtocolFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV1
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV2
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.Login
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.NewVisitorConn
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.NewWorkConn
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.Ping
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.RawClientHello
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxStreamResetFailure
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class FrpCryptoContractTest {
    @Test
    fun timestampAuthenticationMatchesTheGoldenMessages() {
        val login = FrpWireV1.readMessage(
            ByteArrayInputStream(FrpContractFixtures.bytes("wire-v1-login-token")),
        ) as Login
        assertEquals(
            login.privilegeKey,
            FrpTimestampAuth.key(FrpContractFixtures.syntheticToken, login.timestamp),
        )

        val control = ByteArrayInputStream(FrpContractFixtures.bytes("control-v1-client-plain"))
        val ping = FrpWireV1.readMessage(control) as Ping
        val work = FrpWireV1.readMessage(control) as NewWorkConn
        assertEquals(ping.privilegeKey, FrpTimestampAuth.key(FrpContractFixtures.syntheticToken, ping.timestamp))
        assertEquals(work.privilegeKey, FrpTimestampAuth.key(FrpContractFixtures.syntheticToken, work.timestamp))

        val visitor = FrpWireV1.readMessage(
            ByteArrayInputStream(FrpContractFixtures.bytes("wire-v1-new-visitor-stcp")),
        ) as NewVisitorConn
        assertEquals(
            visitor.signKey,
            FrpTimestampAuth.key(FrpContractFixtures.syntheticStcpSecret, visitor.timestamp),
        )
        assertNotEquals(login.privilegeKey, FrpTimestampAuth.key("wrong-token", login.timestamp))
        assertNotEquals(login.privilegeKey, FrpTimestampAuth.key(FrpContractFixtures.syntheticToken, login.timestamp + 1))
    }

    @Test
    fun v1CfbGoldenVectorsDecryptAndEncryptByteForByte() {
        listOf(
            CfbVector(
                cipherEntry = "control-v1-client-cfb",
                plainEntry = "control-v1-client-plain",
                nonceStart = 0x80,
            ),
            CfbVector(
                cipherEntry = "control-v1-server-cfb",
                plainEntry = "control-v1-server-plain",
                nonceStart = 0x90,
            ),
        ).forEach { vector ->
            val cipher = FrpContractFixtures.bytes(vector.cipherEntry)
            val plain = FrpContractFixtures.bytes(vector.plainEntry)
            val decrypted = FrpV1Cfb.decrypting(
                PlannedChunkInputStream(cipher, listOf(1, 3, 2, 7)),
                FrpContractFixtures.syntheticToken,
            ).use { readBounded(it) }
            assertArrayEquals(vector.cipherEntry, plain, decrypted)

            val output = ByteArrayOutputStream()
            FrpV1CfbOutputStream(
                output,
                FrpContractFixtures.syntheticToken,
                fixedBytes(vector.nonceStart, FrpV1Cfb.IV_SIZE),
            ).use { it.write(plain) }
            assertArrayEquals(vector.cipherEntry, cipher, output.toByteArray())
        }
    }

    @Test
    fun unauthenticatedCfbMutationProducesAlteredPlaintext() {
        val original = FrpContractFixtures.bytes("control-v1-client-plain")
        val altered = FrpV1Cfb.decrypting(
            ByteArrayInputStream(FrpContractFixtures.mutated("control-v1-cfb-bit-flip")),
            FrpContractFixtures.syntheticToken,
        ).use { readBounded(it) }

        assertFalse(altered.contentEquals(original))
        val input = ByteArrayInputStream(altered)
        assertTrue(FrpWireV1.readMessage(input) is Ping)
        assertTrue(FrpWireV1.readMessage(input) is NewWorkConn)
    }

    @Test
    fun v2AeadGoldenVectorsDecryptAndEncryptByteForByte() {
        profiles.forEach { profile ->
            val context = loadContext(profile)
            FrpV2Crypto.deriveControlKeys(FrpContractFixtures.syntheticToken, context).use { keys ->
                assertAeadVector(
                    cipherEntry = "control-v2-${profile.name}-client",
                    plainEntry = "control-v2-client-plain",
                    algorithm = profile.algorithm,
                    decryptKey = keys.readKey(FrpAeadRole.SERVER),
                    encryptKey = keys.writeKey(FrpAeadRole.CLIENT),
                    nonce = fixedBytes(profile.clientNonceStart, profile.algorithm.nonceSize),
                )
                assertAeadVector(
                    cipherEntry = "control-v2-${profile.name}-server",
                    plainEntry = "control-v2-server-plain",
                    algorithm = profile.algorithm,
                    decryptKey = keys.readKey(FrpAeadRole.CLIENT),
                    encryptKey = keys.writeKey(FrpAeadRole.SERVER),
                    nonce = fixedBytes(profile.serverNonceStart, profile.algorithm.nonceSize),
                )
            }
        }
    }

    @Test
    fun manifestAeadChunkPlanDecryptsAcrossRecordBoundaries() {
        val plan = FrpContractFixtures.chunkPlan("control-v2-aes-record-splits")
        val profile = profiles.first { it.algorithm == FrpAeadAlgorithm.AES_256_GCM }
        FrpV2Crypto.deriveControlKeys(FrpContractFixtures.syntheticToken, loadContext(profile)).use { keys ->
            val key = keys.readKey(FrpAeadRole.SERVER)
            try {
                val decrypted = FrpAeadRecordInputStream(
                    PlannedChunkInputStream(bytesForPath(plan.source), plan.sizes),
                    key,
                    profile.algorithm,
                ).use { readBounded(it) }
                assertArrayEquals(FrpContractFixtures.bytes("control-v2-client-plain"), decrypted)
            } finally {
                key.fill(0)
            }
        }
    }

    @Test
    fun manifestAeadMutationsFailWithIntegrityOrTruncationErrors() {
        listOf(
            AeadMutation("control-v2-aes-bit-flip", profiles[0], FrpAeadRole.SERVER, FrpCryptoFailure.INTEGRITY_FAILURE),
            AeadMutation("control-v2-aes-record-reorder", profiles[0], FrpAeadRole.SERVER, FrpCryptoFailure.INTEGRITY_FAILURE),
            AeadMutation("control-v2-aes-truncated", profiles[0], FrpAeadRole.SERVER, FrpCryptoFailure.TRUNCATED_RECORD),
            AeadMutation("control-v2-aes-wrong-role", profiles[0], FrpAeadRole.CLIENT, FrpCryptoFailure.INTEGRITY_FAILURE, wrongRole = true),
            AeadMutation("control-v2-xchacha-bit-flip", profiles[1], FrpAeadRole.SERVER, FrpCryptoFailure.INTEGRITY_FAILURE),
            AeadMutation("control-v2-xchacha-record-reorder", profiles[1], FrpAeadRole.SERVER, FrpCryptoFailure.INTEGRITY_FAILURE),
            AeadMutation("control-v2-xchacha-wrong-role", profiles[1], FrpAeadRole.CLIENT, FrpCryptoFailure.INTEGRITY_FAILURE, wrongRole = true),
        ).forEach { mutation ->
            FrpV2Crypto.deriveControlKeys(
                FrpContractFixtures.syntheticToken,
                loadContext(mutation.profile),
            ).use { keys ->
                val role = if (mutation.wrongRole) {
                    if (mutation.readRole == FrpAeadRole.CLIENT) FrpAeadRole.SERVER else FrpAeadRole.CLIENT
                } else {
                    mutation.readRole
                }
                val key = keys.readKey(role)
                try {
                    assertFailure(mutation.expectedFailure) {
                        FrpAeadRecordInputStream(
                            ByteArrayInputStream(FrpContractFixtures.mutated(mutation.id)),
                            key,
                            mutation.profile.algorithm,
                        ).use { readBounded(it) }
                    }
                } finally {
                    key.fill(0)
                }
            }
        }
    }

    @Test
    fun aeadRecordLengthsAndTruncationAreRejectedBeforeLargeAllocation() {
        val key = ByteArray(FrpV2Crypto.KEY_SIZE) { it.toByte() }
        val nonce = ByteArray(FrpAeadAlgorithm.AES_256_GCM.nonceSize)
        try {
            assertEquals(
                -1,
                FrpAeadRecordInputStream(ByteArrayInputStream(ByteArray(0)), key, FrpAeadAlgorithm.AES_256_GCM).use {
                    it.read()
                },
            )
            assertFailure(FrpCryptoFailure.TRUNCATED_NONCE) {
                readAead(ByteArray(nonce.size - 1), key)
            }
            assertFailure(FrpCryptoFailure.TRUNCATED_HEADER) {
                readAead(nonce + byteArrayOf(0, 0), key)
            }
            assertFailure(FrpCryptoFailure.INVALID_LENGTH) {
                readAead(nonce + uint32(FrpAeadRecordStreams.MIN_CIPHERTEXT_LENGTH - 1), key)
            }
            assertFailure(FrpCryptoFailure.INVALID_LENGTH) {
                readAead(nonce + uint32(FrpAeadRecordStreams.MAX_CIPHERTEXT_LENGTH + 1), key)
            }
            assertFailure(FrpCryptoFailure.TRUNCATED_RECORD) {
                readAead(
                    nonce + uint32(FrpAeadRecordStreams.MIN_CIPHERTEXT_LENGTH) +
                        ByteArray(FrpAeadRecordStreams.MIN_CIPHERTEXT_LENGTH - 1),
                    key,
                )
            }
        } finally {
            key.fill(0)
        }
    }

    @Test
    fun aeadSplitsMaximumPlusOneAndRoundTripsBothAlgorithms() {
        val payload = ByteArray(FrpAeadRecordStreams.MAX_PLAINTEXT_LENGTH + 1) { (it * 31).toByte() }
        FrpAeadAlgorithm.entries.forEach { algorithm ->
            val key = ByteArray(FrpV2Crypto.KEY_SIZE) { (it + 1).toByte() }
            val output = ByteArrayOutputStream()
            try {
                FrpAeadRecordOutputStream(
                    output,
                    key,
                    algorithm,
                    fixedBytes(0x10, algorithm.nonceSize),
                ).use { it.write(payload) }
                val decrypted = FrpAeadRecordInputStream(
                    ByteArrayInputStream(output.toByteArray()),
                    key,
                    algorithm,
                ).use { readBounded(it, payload.size) }
                assertArrayEquals(algorithm.wireName, payload, decrypted)
                assertEquals(2, countAeadRecords(output.toByteArray(), algorithm.nonceSize))
            } finally {
                key.fill(0)
            }
        }
    }

    @Test
    fun nonceAndAesRecordLimitsBecomeStickyFailures() {
        val key = ByteArray(FrpV2Crypto.KEY_SIZE) { it.toByte() }
        try {
            val limited = FrpAeadRecordOutputStream(
                ByteArrayOutputStream(),
                key,
                FrpAeadAlgorithm.AES_256_GCM,
                ByteArray(FrpAeadAlgorithm.AES_256_GCM.nonceSize),
                initialRecordCount = FrpAeadRecordStreams.AES_GCM_MAX_RECORDS,
            )
            val recordLimit = assertThrows(FrpCryptoException::class.java) { limited.write(1) }
            assertEquals(FrpCryptoFailure.RECORD_LIMIT, recordLimit.failure)
            assertSame(recordLimit, assertThrows(FrpCryptoException::class.java) { limited.write(2) })
            assertSame(recordLimit, assertThrows(FrpCryptoException::class.java) { limited.close() })

            val exhausted = FrpAeadRecordOutputStream(
                ByteArrayOutputStream(),
                key,
                FrpAeadAlgorithm.XCHACHA20_POLY1305,
                ByteArray(FrpAeadAlgorithm.XCHACHA20_POLY1305.nonceSize) { 0xff.toByte() },
            )
            val nonceFailure = assertThrows(FrpCryptoException::class.java) { exhausted.write(1) }
            assertEquals(FrpCryptoFailure.NONCE_EXHAUSTED, nonceFailure.failure)
            assertSame(nonceFailure, assertThrows(FrpCryptoException::class.java) { exhausted.write(2) })
            assertSame(nonceFailure, assertThrows(FrpCryptoException::class.java) { exhausted.close() })

            val readerBytes = ByteArray(FrpAeadAlgorithm.AES_256_GCM.nonceSize) +
                uint32(FrpAeadRecordStreams.MIN_CIPHERTEXT_LENGTH)
            assertFailure(FrpCryptoFailure.RECORD_LIMIT) {
                FrpAeadRecordInputStream(
                    ByteArrayInputStream(readerBytes),
                    key,
                    FrpAeadAlgorithm.AES_256_GCM,
                    initialRecordCount = FrpAeadRecordStreams.AES_GCM_MAX_RECORDS,
                ).use { it.read() }
            }
        } finally {
            key.fill(0)
        }
    }

    @Test
    fun invalidKeysAreRejectedAndWrongKeysFailAuthentication() {
        assertFailure(FrpCryptoFailure.INVALID_KEY) {
            FrpAeadRecordOutputStream(
                ByteArrayOutputStream(),
                ByteArray(31),
                FrpAeadAlgorithm.AES_256_GCM,
                ByteArray(FrpAeadAlgorithm.AES_256_GCM.nonceSize),
            )
        }

        val profile = profiles[0]
        FrpV2Crypto.deriveControlKeys(FrpContractFixtures.syntheticToken, loadContext(profile)).use { keys ->
            val wrongKey = keys.readKey(FrpAeadRole.SERVER).also { it[0] = (it[0].toInt() xor 1).toByte() }
            try {
                assertFailure(FrpCryptoFailure.INTEGRITY_FAILURE) {
                    FrpAeadRecordInputStream(
                        ByteArrayInputStream(FrpContractFixtures.bytes("control-v2-aes-256-gcm-client")),
                        wrongKey,
                        profile.algorithm,
                    ).use { readBounded(it) }
                }
            } finally {
                wrongKey.fill(0)
            }
        }
    }

    @Test
    fun stickyWriterCloseClosesDelegatePreservesPrimaryAndClearsSensitiveState() {
        val factories = listOf(
            WriterFactory("v1-cfb", listOf("iv"), listOf("cipher")) { delegate ->
                FrpV1CfbOutputStream(
                    delegate,
                    FrpContractFixtures.syntheticToken,
                    fixedBytes(0x21, FrpV1Cfb.IV_SIZE),
                )
            },
            WriterFactory("v2-aead", listOf("key", "streamNonce", "recordNonce")) { delegate ->
                FrpAeadRecordOutputStream(
                    delegate,
                    fixedBytes(0x31, FrpV2Crypto.KEY_SIZE),
                    FrpAeadAlgorithm.XCHACHA20_POLY1305,
                    fixedBytes(0x61, FrpAeadAlgorithm.XCHACHA20_POLY1305.nonceSize),
                )
            },
        )
        val cases = listOf(
            FailurePriorityCase(
                source = { IOException("source-io-detail-marker") },
                cleanup = { IOException("cleanup-io-detail-marker") },
                expected = FailureSide.SOURCE,
            ),
            FailurePriorityCase(
                source = { IOException("source-io-detail-marker") },
                cleanup = { CancellationException("cleanup-cancel-detail-marker") },
                expected = FailureSide.CLEANUP,
            ),
            FailurePriorityCase(
                source = { CancellationException("source-cancel-detail-marker") },
                cleanup = { IOException("cleanup-io-detail-marker") },
                expected = FailureSide.SOURCE,
            ),
            FailurePriorityCase(
                source = { IOException("source-io-detail-marker") },
                cleanup = { AssertionError("cleanup-error-detail-marker") },
                expected = FailureSide.CLEANUP,
            ),
            FailurePriorityCase(
                source = { CancellationException("source-cancel-detail-marker") },
                cleanup = { AssertionError("cleanup-error-detail-marker") },
                expected = FailureSide.CLEANUP,
            ),
            FailurePriorityCase(
                source = { AssertionError("source-error-detail-marker") },
                cleanup = { CancellationException("cleanup-cancel-detail-marker") },
                expected = FailureSide.SOURCE,
            ),
        )
        factories.forEach { factory ->
            cases.forEach { case ->
                val sourceFailure = case.source()
                val cleanupFailure = case.cleanup()
                val delegate = FailingCloseTrackingOutputStream(sourceFailure, cleanupFailure)
                val writer = factory.create(delegate)
                val sensitiveArrays = factory.sensitiveFields.map { privateByteArray(writer, it) }
                val cfbCipher = if (factory.name == "v1-cfb") privateField(writer, "cipher") else null
                val cfbCipherArrays = cfbCipher?.let {
                    listOf(privateByteArray(it, "feedback"), privateByteArray(it, "keyStream"))
                }.orEmpty()

                val firstFailure = captureFailure { writer.write(0x5a) }
                if (sourceFailure is IOException) {
                    assertTrue("${factory.name} should map ordinary IO", firstFailure is FrpCryptoException)
                    assertEquals(FrpCryptoFailure.WRITE_FAILED, (firstFailure as FrpCryptoException).failure)
                } else {
                    assertSame("${factory.name} changed the original fatal/cancellation", sourceFailure, firstFailure)
                }
                val closeFailure = captureFailure(writer::close)
                assertSame(
                    "${factory.name} used the wrong close failure priority",
                    if (case.expected == FailureSide.SOURCE) firstFailure else cleanupFailure,
                    closeFailure,
                )
                closeFailure.suppressed.forEach { suppressed ->
                    assertFalse("${factory.name} leaked a suppressed source message", suppressed.toString().contains("source-"))
                    assertFalse("${factory.name} leaked a suppressed cleanup message", suppressed.toString().contains("cleanup-"))
                }
                assertTrue("${factory.name} did not close its delegate", delegate.closed)
                sensitiveArrays.forEach { assertCleared("${factory.name} retained sensitive state", it) }
                cfbCipherArrays.forEach { assertCleared("${factory.name} retained CFB cipher state", it) }
                cfbCipher?.let { assertNull(privateField(it, "blockCipher")) }
                factory.releasedFields.forEach { field ->
                    assertNull("${factory.name} retained $field", privateField(writer, field))
                }
            }
        }
    }

    @Test
    fun decryptingStreamsRejectReadsAfterCloseAndClearRetainedState() {
        val v1Plaintext = byteArrayOf(0x31, 0x32, 0x33)
        val v1Encrypted = ByteArrayOutputStream().also { output ->
            FrpV1CfbOutputStream(
                output,
                FrpContractFixtures.syntheticToken,
                fixedBytes(0x21, FrpV1Cfb.IV_SIZE),
            ).use { it.write(v1Plaintext) }
        }.toByteArray()
        val v1Delegate = CloseTrackingInputStream(ByteArrayInputStream(v1Encrypted))
        val v1Reader = FrpV1CfbInputStream(v1Delegate, FrpContractFixtures.syntheticToken)
        val v1Key = privateByteArray(v1Reader, "key")
        assertEquals(v1Plaintext[0].toInt() and 0xff, v1Reader.read())
        val v1Pending = privateByteArray(v1Reader, "pending")
        val v1Cipher = privateField(v1Reader, "cipher") ?: throw AssertionError("v1 cipher was not initialized")
        val v1Feedback = privateByteArray(v1Cipher, "feedback")
        val v1KeyStream = privateByteArray(v1Cipher, "keyStream")
        assertTrue("v1 reader did not retain the unread plaintext needed by this test", v1Pending.any { it != 0.toByte() })
        v1Reader.close()
        assertTrue(v1Delegate.closed)
        assertCleared("v1 reader key", v1Key)
        assertCleared("v1 reader pending plaintext", v1Pending)
        assertCleared("v1 reader CFB feedback", v1Feedback)
        assertCleared("v1 reader CFB key stream", v1KeyStream)
        assertNull(privateField(v1Cipher, "blockCipher"))
        assertNull(privateField(v1Reader, "cipher"))
        assertClosedReader(v1Reader)
        v1Plaintext.fill(0)
        v1Encrypted.fill(0)

        val key = fixedBytes(0x41, FrpV2Crypto.KEY_SIZE)
        val nonce = fixedBytes(0x71, FrpAeadAlgorithm.XCHACHA20_POLY1305.nonceSize)
        val plaintext = byteArrayOf(0x51, 0x52, 0x53)
        try {
            val encrypted = ByteArrayOutputStream().also { output ->
                FrpAeadRecordOutputStream(
                    output,
                    key,
                    FrpAeadAlgorithm.XCHACHA20_POLY1305,
                    nonce,
                ).use { it.write(plaintext) }
            }.toByteArray()
            val delegate = CloseTrackingInputStream(ByteArrayInputStream(encrypted))
            val reader = FrpAeadRecordInputStream(delegate, key, FrpAeadAlgorithm.XCHACHA20_POLY1305)
            assertEquals(plaintext[0].toInt() and 0xff, reader.read())
            val retained = listOf(
                privateByteArray(reader, "key"),
                privateByteArray(reader, "streamNonce"),
                privateByteArray(reader, "recordNonce"),
                privateByteArray(reader, "pending"),
            )

            reader.close()
            assertTrue(delegate.closed)
            retained.forEach { assertCleared("AEAD reader retained sensitive state", it) }
            assertClosedReader(reader)
        } finally {
            key.fill(0)
            nonce.fill(0)
            plaintext.fill(0)
        }
    }

    @Test
    fun decryptingStreamsSerializeConcurrentCloseAndRead() {
        val plaintext = byteArrayOf(0x51, 0x52, 0x53)
        val v1Encrypted = ByteArrayOutputStream().also { output ->
            FrpV1CfbOutputStream(
                output,
                FrpContractFixtures.syntheticToken,
                fixedBytes(0x11, FrpV1Cfb.IV_SIZE),
            ).use { it.write(plaintext) }
        }.toByteArray()
        assertConcurrentCloseReadSafe(
            expectedFirst = plaintext[0].toInt() and 0xff,
            expectedSecond = plaintext[1].toInt() and 0xff,
        ) {
            FrpV1CfbInputStream(ByteArrayInputStream(v1Encrypted), FrpContractFixtures.syntheticToken)
        }

        val key = fixedBytes(0x41, FrpV2Crypto.KEY_SIZE)
        val nonce = fixedBytes(0x71, FrpAeadAlgorithm.AES_256_GCM.nonceSize)
        try {
            val aeadEncrypted = ByteArrayOutputStream().also { output ->
                FrpAeadRecordOutputStream(
                    output,
                    key,
                    FrpAeadAlgorithm.AES_256_GCM,
                    nonce,
                ).use { it.write(plaintext) }
            }.toByteArray()
            assertConcurrentCloseReadSafe(
                expectedFirst = plaintext[0].toInt() and 0xff,
                expectedSecond = plaintext[1].toInt() and 0xff,
            ) {
                FrpAeadRecordInputStream(
                    ByteArrayInputStream(aeadEncrypted),
                    key,
                    FrpAeadAlgorithm.AES_256_GCM,
                )
            }
        } finally {
            key.fill(0)
            nonce.fill(0)
            plaintext.fill(0)
        }
    }

    @Test
    fun closeUnblocksBlockingDelegateIoBeforeClearingSensitiveState() {
        val writerFactories = listOf(
            WriterFactory("v1-cfb", listOf("iv"), listOf("cipher")) { delegate ->
                FrpV1CfbOutputStream(
                    delegate,
                    FrpContractFixtures.syntheticToken,
                    fixedBytes(0x21, FrpV1Cfb.IV_SIZE),
                )
            },
            WriterFactory("v2-aead", listOf("key", "streamNonce", "recordNonce")) { delegate ->
                FrpAeadRecordOutputStream(
                    delegate,
                    fixedBytes(0x31, FrpV2Crypto.KEY_SIZE),
                    FrpAeadAlgorithm.XCHACHA20_POLY1305,
                    fixedBytes(0x61, FrpAeadAlgorithm.XCHACHA20_POLY1305.nonceSize),
                )
            },
        )
        writerFactories.forEach { factory ->
            val delegate = CloseUnblocksOutputStream()
            val writer = factory.create(delegate)
            val sensitiveArrays = factory.sensitiveFields.map { privateByteArray(writer, it) }
            val workerOutcome = AtomicReference<Any?>()
            val closeOutcome = AtomicReference<Throwable?>()
            val worker = thread(name = "${factory.name}-blocking-writer", isDaemon = true) {
                try {
                    writer.write(byteArrayOf(0x41, 0x42, 0x43))
                    workerOutcome.set(Unit)
                } catch (failure: Throwable) {
                    workerOutcome.set(failure)
                }
            }
            assertTrue("${factory.name} writer never reached delegate I/O", delegate.entered.await(5, TimeUnit.SECONDS))
            val closer = thread(name = "${factory.name}-writer-close", isDaemon = true) {
                try {
                    writer.close()
                } catch (failure: Throwable) {
                    closeOutcome.set(failure)
                }
            }
            closer.join(5_000)
            worker.join(5_000)
            val closerStoppedWithoutRescue = !closer.isAlive
            val workerStoppedWithoutRescue = !worker.isAlive
            val delegateClosedWithoutRescue = delegate.closed
            if (closer.isAlive || worker.isAlive) {
                delegate.close()
                closer.join(5_000)
                worker.join(5_000)
            }

            assertTrue("${factory.name} close did not complete promptly", closerStoppedWithoutRescue)
            assertTrue("${factory.name} writer was not released promptly", workerStoppedWithoutRescue)
            assertTrue("${factory.name} wrapper close did not close the delegate", delegateClosedWithoutRescue)
            assertFalse("${factory.name} close remained blocked behind delegate write", closer.isAlive)
            assertFalse("${factory.name} left a blocked writer worker", worker.isAlive)
            assertTrue("${factory.name} did not close the blocking delegate", delegate.closed)
            assertTrue("${factory.name} worker did not receive a typed close failure", workerOutcome.get() is FrpCryptoException)
            assertSame("${factory.name} close did not preserve the in-flight failure", workerOutcome.get(), closeOutcome.get())
            sensitiveArrays.forEach { assertCleared("${factory.name} retained sensitive output state", it) }
            factory.releasedFields.forEach { field -> assertNull(privateField(writer, field)) }
        }

        val readerCases = listOf(
            BlockingReaderCase(
                name = "v1-cfb",
                prefix = fixedBytes(0x21, FrpV1Cfb.IV_SIZE),
                sensitiveFields = listOf("key", "pending"),
                releasedFields = listOf("cipher"),
            ) { delegate ->
                FrpV1CfbInputStream(delegate, FrpContractFixtures.syntheticToken)
            },
            BlockingReaderCase(
                name = "v2-aead",
                prefix = fixedBytes(0x61, FrpAeadAlgorithm.XCHACHA20_POLY1305.nonceSize) +
                    uint32(FrpAeadRecordStreams.TAG_LENGTH),
                sensitiveFields = listOf("key", "streamNonce", "recordNonce", "pending"),
                releasedFields = listOf("streamNonce", "recordNonce"),
            ) { delegate ->
                FrpAeadRecordInputStream(
                    delegate,
                    fixedBytes(0x31, FrpV2Crypto.KEY_SIZE),
                    FrpAeadAlgorithm.XCHACHA20_POLY1305,
                )
            },
        )
        readerCases.forEach { case ->
            val delegate = PrefixThenCloseBlockingInputStream(case.prefix)
            val reader = case.create(delegate)
            val workerOutcome = AtomicReference<Any?>()
            val closeOutcome = AtomicReference<Throwable?>()
            val worker = thread(name = "${case.name}-blocking-reader", isDaemon = true) {
                try {
                    workerOutcome.set(reader.read())
                } catch (failure: Throwable) {
                    workerOutcome.set(failure)
                }
            }
            assertTrue("${case.name} reader never reached blocking delegate I/O", delegate.entered.await(5, TimeUnit.SECONDS))
            val sensitiveArrays = case.sensitiveFields.map { privateByteArray(reader, it) }
            val closer = thread(name = "${case.name}-reader-close", isDaemon = true) {
                try {
                    reader.close()
                } catch (failure: Throwable) {
                    closeOutcome.set(failure)
                }
            }
            closer.join(5_000)
            worker.join(5_000)
            val closerStoppedWithoutRescue = !closer.isAlive
            val workerStoppedWithoutRescue = !worker.isAlive
            val delegateClosedWithoutRescue = delegate.closed
            if (closer.isAlive || worker.isAlive) {
                delegate.close()
                closer.join(5_000)
                worker.join(5_000)
            }

            assertTrue("${case.name} close did not complete promptly", closerStoppedWithoutRescue)
            assertTrue("${case.name} reader was not released promptly", workerStoppedWithoutRescue)
            assertTrue("${case.name} wrapper close did not close the delegate", delegateClosedWithoutRescue)
            assertFalse("${case.name} close remained blocked behind delegate read", closer.isAlive)
            assertFalse("${case.name} left a blocked reader worker", worker.isAlive)
            assertTrue("${case.name} did not close the blocking delegate", delegate.closed)
            if (case.name == "v1-cfb") {
                assertEquals(-1, workerOutcome.get())
                assertNull(closeOutcome.get())
            } else {
                assertTrue(workerOutcome.get() is FrpCryptoException)
                assertSame(workerOutcome.get(), closeOutcome.get())
            }
            sensitiveArrays.forEach { assertCleared("${case.name} retained sensitive input state", it) }
            case.releasedFields.forEach { field -> assertNull(privateField(reader, field)) }
        }
    }

    @Test
    fun failureCombinationUsesErrorThenCancellationThenFirstExpectedException() {
        val firstExpected = FrpCryptoException(FrpCryptoFailure.IO_FAILURE)
        val secondExpected = FrpCryptoException(FrpCryptoFailure.WRITE_FAILED)
        assertSame(
            firstExpected,
            combineCryptoFailures(firstExpected, secondExpected, FrpCryptoFailure.IO_FAILURE),
        )

        val cancellation = CancellationException("sensitive-cancellation-marker")
        assertSame(
            cancellation,
            combineCryptoFailures(firstExpected, cancellation, FrpCryptoFailure.IO_FAILURE),
        )
        val fatal = AssertionError("sensitive-error-marker")
        assertSame(
            fatal,
            combineCryptoFailures(cancellation, fatal, FrpCryptoFailure.IO_FAILURE),
        )
        (cancellation.suppressed.asList() + fatal.suppressed.asList()).forEach { suppressed ->
            assertFalse(suppressed.toString().contains("sensitive-cancellation-marker"))
            assertFalse(suppressed.toString().contains("sensitive-error-marker"))
        }
    }

    @Test
    fun cryptoFailureMappingPreservesTypedYamuxFailure() {
        val failure = YamuxStreamResetFailure()
        assertSame(failure, mapCryptoStreamFailure(failure, FrpCryptoFailure.IO_FAILURE))
    }

    @Test
    fun aeadAuthenticationFailuresClearTemporaryOutputBuffers() {
        val key = fixedBytes(0x31, FrpV2Crypto.KEY_SIZE)
        val plaintext = ByteArray(128) { index -> (index + 1).toByte() }
        try {
            profiles.forEach { profile ->
                val nonce = fixedBytes(0x61, profile.algorithm.nonceSize)
                val ciphertext = ByteArrayOutputStream().also { output ->
                    FrpAeadRecordOutputStream(output, key, profile.algorithm, nonce).use { it.write(plaintext) }
                }.toByteArray()
                ciphertext[ciphertext.lastIndex] = (ciphertext.last().toInt() xor 1).toByte()
                val observations = mutableListOf<TemporaryBufferObservation>()
                FrpCryptoTestHooks.onTemporaryBufferCleared = { label, size, containedData, cleared ->
                    observations += TemporaryBufferObservation(label, size, containedData, cleared)
                }
                try {
                    assertFailure(FrpCryptoFailure.INTEGRITY_FAILURE) {
                        FrpAeadRecordInputStream(
                            ByteArrayInputStream(ciphertext),
                            key,
                            profile.algorithm,
                        ).use { readBounded(it) }
                    }
                } finally {
                    FrpCryptoTestHooks.onTemporaryBufferCleared = null
                    nonce.fill(0)
                    ciphertext.fill(0)
                }
                val label = if (profile.algorithm == FrpAeadAlgorithm.AES_256_GCM) {
                    "aes-gcm-output"
                } else {
                    "xchacha-output"
                }
                val observation = observations.lastOrNull { it.label == label }
                    ?: throw AssertionError("No temporary output clear was observed for $label")
                assertTrue("$label temporary output was empty", observation.size > 0)
                assertTrue("$label temporary output was not cleared", observation.cleared)
                if (profile.algorithm == FrpAeadAlgorithm.XCHACHA20_POLY1305) {
                    assertTrue("XChaCha failure did not exercise unauthenticated plaintext cleanup", observation.containedData)
                }
            }
        } finally {
            FrpCryptoTestHooks.onTemporaryBufferCleared = null
            key.fill(0)
            plaintext.fill(0)
        }
    }

    @Test
    fun trimmingCryptoOutputClearsTheDiscardedOriginalArray() {
        val original = byteArrayOf(0x11, 0x22, 0x33, 0x44)
        val observations = mutableListOf<TemporaryBufferObservation>()
        FrpCryptoTestHooks.onTemporaryBufferCleared = { label, size, containedData, cleared ->
            observations += TemporaryBufferObservation(label, size, containedData, cleared)
        }
        val trimmed = try {
            retainOrTrimTemporaryCryptoOutput("trimmed-output", original, 2)
        } finally {
            FrpCryptoTestHooks.onTemporaryBufferCleared = null
        }
        try {
            assertArrayEquals(byteArrayOf(0x11, 0x22), trimmed)
            assertCleared("discarded full-size crypto output", original)
            assertEquals(
                TemporaryBufferObservation("trimmed-output", 4, containedData = true, cleared = true),
                observations.single(),
            )
        } finally {
            trimmed.fill(0)
        }
    }

    @Test
    fun clientCryptoContextRejectsServerAlgorithmNotAdvertisedByClient() {
        val profile = profiles.first { it.algorithm == FrpAeadAlgorithm.XCHACHA20_POLY1305 }
        val clientInput = ByteArrayInputStream(
            FrpContractFixtures.bytes("wire-v2-${profile.name}-client-bootstrap"),
        )
        FrpWireV2.readMagic(clientInput)
        val originalClientHello = FrpWireV2.readClientHello(clientInput)
        val narrowedClientHello = FrpWireV2.encodeClientHello(
            originalClientHello.value.copy(
                capabilities = originalClientHello.value.capabilities.copy(
                    crypto = originalClientHello.value.capabilities.crypto.copy(
                        algorithms = listOf(FRP_AEAD_AES_256_GCM),
                    ),
                ),
            ),
        )
        val serverHello = FrpWireV2.readServerHello(
            ByteArrayInputStream(FrpContractFixtures.bytes("wire-v2-${profile.name}-server-bootstrap")),
        )

        val wrapperWithUnchangedValue = RawClientHello(
            originalClientHello.value,
            narrowedClientHello.rawPayload,
        )
        val failure = assertThrows(FrpProtocolException::class.java) {
            FrpV2Crypto.context(wrapperWithUnchangedValue, serverHello)
        }
        assertEquals(FrpProtocolFailure.INVALID_HELLO, failure.failure)
    }

    @Test
    fun derivedControlKeysDestroyIdempotentlyClearRetainedCopiesAndBecomeUnavailable() {
        val keys = FrpV2Crypto.deriveControlKeys(
            FrpContractFixtures.syntheticToken,
            loadContext(profiles[0]),
        )
        val clientToServer = privateByteArray(keys, "clientToServer")
        val serverToClient = privateByteArray(keys, "serverToClient")
        val issuedCopy = keys.writeKey(FrpAeadRole.CLIENT)
        try {
            keys.destroy()
            keys.destroy()
            keys.close()

            assertCleared("client-to-server retained key", clientToServer)
            assertCleared("server-to-client retained key", serverToClient)
            assertTrue(keys.toString().contains("destroyed=true"))
            assertFailure(FrpCryptoFailure.INVALID_KEY) { keys.writeKey(FrpAeadRole.CLIENT) }
            assertFailure(FrpCryptoFailure.INVALID_KEY) { keys.readKey(FrpAeadRole.SERVER) }
        } finally {
            issuedCopy.fill(0)
            keys.close()
        }
    }

    @Test
    fun cryptoSummariesFailuresAndStreamsOmitKeysTokensNoncesAndPayloads() {
        val marker = "0123456789abcdef0123456789abcdef"
        val key = marker.encodeToByteArray()
        val nonceMarker = "nonce-leak-marker"
        val nonce = nonceMarker.padEnd(24, 'x').encodeToByteArray()
        val context = loadContext(profiles[0])
        val keys = FrpAeadControlKeys(key, key)
        val aeadOutput = FrpAeadRecordOutputStream(
            ByteArrayOutputStream(),
            key,
            FrpAeadAlgorithm.XCHACHA20_POLY1305,
            nonce,
        )
        val aeadInput = FrpAeadRecordInputStream(
            ByteArrayInputStream(ByteArray(0)),
            key,
            FrpAeadAlgorithm.XCHACHA20_POLY1305,
        )
        val cfbOutput = FrpV1CfbOutputStream(ByteArrayOutputStream(), marker, ByteArray(FrpV1Cfb.IV_SIZE))
        val cfbInput = FrpV1CfbInputStream(ByteArrayInputStream(ByteArray(0)), marker)
        val values = listOf(
            context,
            keys,
            aeadOutput,
            aeadInput,
            cfbOutput,
            cfbInput,
            FrpCryptoException(FrpCryptoFailure.INTEGRITY_FAILURE),
        )

        try {
            values.map(Any::toString).forEach { summary ->
                assertFalse("Summary leaked a key or token", summary.contains(marker))
                assertFalse("Summary leaked a nonce", summary.contains(nonceMarker))
            }
            assertTrue(keys.toString().contains("<redacted>"))
        } finally {
            keys.close()
            aeadOutput.close()
            aeadInput.close()
            cfbOutput.close()
            cfbInput.close()
            key.fill(0)
            nonce.fill(0)
        }
    }

    @Test
    fun cancellationAndJvmErrorsPropagateWhileIoFailuresAreMapped() {
        val key = ByteArray(FrpV2Crypto.KEY_SIZE)
        val nonce = ByteArray(FrpAeadAlgorithm.AES_256_GCM.nonceSize)
        val cancellation = CancellationException("cancel-marker")
        val cancellingOutput = object : OutputStream() {
            override fun write(value: Int) = throw cancellation
            override fun write(bytes: ByteArray, offset: Int, length: Int) = throw cancellation
        }
        val cancellingWriter = FrpAeadRecordOutputStream(
            cancellingOutput,
            key,
            FrpAeadAlgorithm.AES_256_GCM,
            nonce,
        )
        assertSame(
            cancellation,
            assertThrows(CancellationException::class.java) {
                cancellingWriter.write(1)
            },
        )
        assertSame(cancellation, assertThrows(CancellationException::class.java) { cancellingWriter.close() })

        val fatal = AssertionError("fatal-marker")
        val fatalInput = object : InputStream() {
            override fun read(): Int = throw fatal
        }
        assertSame(
            fatal,
            assertThrows(AssertionError::class.java) {
                FrpAeadRecordInputStream(fatalInput, key, FrpAeadAlgorithm.AES_256_GCM).read()
            },
        )

        val ioFailure = assertThrows(FrpCryptoException::class.java) {
            FrpAeadRecordInputStream(object : InputStream() {
                override fun read(): Int = throw IOException("io-detail-marker")
            }, key, FrpAeadAlgorithm.AES_256_GCM).read()
        }
        assertEquals(FrpCryptoFailure.IO_FAILURE, ioFailure.failure)
        assertFalse(ioFailure.toString().contains("io-detail-marker"))
    }

    private fun assertAeadVector(
        cipherEntry: String,
        plainEntry: String,
        algorithm: FrpAeadAlgorithm,
        decryptKey: ByteArray,
        encryptKey: ByteArray,
        nonce: ByteArray,
    ) {
        try {
            val cipher = FrpContractFixtures.bytes(cipherEntry)
            val plain = FrpContractFixtures.bytes(plainEntry)
            val decrypted = FrpAeadRecordInputStream(
                ByteArrayInputStream(cipher),
                decryptKey,
                algorithm,
            ).use { readBounded(it) }
            assertArrayEquals(cipherEntry, plain, decrypted)

            val output = ByteArrayOutputStream()
            FrpAeadRecordOutputStream(output, encryptKey, algorithm, nonce).use { writer ->
                splitV2Frames(plain).forEach(writer::write)
            }
            assertArrayEquals(cipherEntry, cipher, output.toByteArray())
        } finally {
            decryptKey.fill(0)
            encryptKey.fill(0)
            nonce.fill(0)
        }
    }

    private fun loadContext(profile: AeadProfile): FrpV2CryptoContext {
        val clientInput = ByteArrayInputStream(
            FrpContractFixtures.bytes("wire-v2-${profile.name}-client-bootstrap"),
        )
        FrpWireV2.readMagic(clientInput)
        val clientHello = FrpWireV2.readClientHello(clientInput)
        val serverHello = FrpWireV2.readServerHello(
            ByteArrayInputStream(FrpContractFixtures.bytes("wire-v2-${profile.name}-server-bootstrap")),
        )
        return FrpV2Crypto.context(clientHello, serverHello).also {
            assertEquals(profile.algorithm, it.algorithm)
        }
    }

    private fun readAead(source: ByteArray, key: ByteArray) {
        FrpAeadRecordInputStream(
            ByteArrayInputStream(source),
            key,
            FrpAeadAlgorithm.AES_256_GCM,
        ).use { readBounded(it) }
    }

    private fun readBounded(input: InputStream, maximum: Int = 1 shl 20): ByteArray {
        val output = ByteArrayOutputStream(minOf(maximum, 8 * 1_024))
        val buffer = ByteArray(4 * 1_024)
        var total = 0
        while (true) {
            val count = input.read(buffer)
            if (count < 0) {
                return output.toByteArray()
            }
            total += count
            if (total > maximum) {
                throw AssertionError("Decoded test data exceeded its bound")
            }
            output.write(buffer, 0, count)
        }
    }

    private fun splitV2Frames(source: ByteArray): List<ByteArray> {
        val frames = mutableListOf<ByteArray>()
        var position = 0
        while (position < source.size) {
            if (position > source.size - V2_FRAME_HEADER_SIZE) {
                throw AssertionError("Plain v2 fixture has a truncated header")
            }
            val payloadLength = ByteBuffer.wrap(source, position + 4, 4)
                .order(ByteOrder.BIG_ENDIAN)
                .int
                .toLong() and 0xffff_ffffL
            val end = position.toLong() + V2_FRAME_HEADER_SIZE + payloadLength
            if (end > source.size) {
                throw AssertionError("Plain v2 fixture has a truncated payload")
            }
            frames += source.copyOfRange(position, end.toInt())
            position = end.toInt()
        }
        return frames
    }

    private fun countAeadRecords(source: ByteArray, nonceSize: Int): Int {
        var count = 0
        var position = nonceSize
        while (position < source.size) {
            val length = ByteBuffer.wrap(source, position, 4).order(ByteOrder.BIG_ENDIAN).int
            position += 4 + length
            count++
        }
        assertEquals(source.size, position)
        return count
    }

    private fun assertFailure(expected: FrpCryptoFailure, operation: () -> Unit) {
        val failure = assertThrows(FrpCryptoException::class.java) { operation() }
        assertEquals(expected, failure.failure)
    }

    private fun assertClosedReader(reader: InputStream) {
        assertFailure(FrpCryptoFailure.IO_FAILURE) { reader.read() }
        assertFailure(FrpCryptoFailure.IO_FAILURE) { reader.read(ByteArray(0), 0, 0) }
    }

    private fun assertConcurrentCloseReadSafe(
        expectedFirst: Int,
        expectedSecond: Int,
        readerFactory: () -> InputStream,
    ) {
        repeat(32) {
            val reader = readerFactory()
            assertEquals(expectedFirst, reader.read())
            val start = CountDownLatch(1)
            val readOutcome = AtomicReference<Any?>()
            val closeFailure = AtomicReference<Throwable?>()
            val readThread = thread(name = "frp-reader-race") {
                start.await()
                try {
                    readOutcome.set(reader.read())
                } catch (failure: Throwable) {
                    readOutcome.set(failure)
                }
            }
            val closeThread = thread(name = "frp-close-race") {
                start.await()
                try {
                    reader.close()
                } catch (failure: Throwable) {
                    closeFailure.set(failure)
                }
            }
            start.countDown()
            readThread.join()
            closeThread.join()

            closeFailure.get()?.let { throw AssertionError("Concurrent close failed", it) }
            when (val outcome = readOutcome.get()) {
                is Int -> assertEquals(expectedSecond, outcome)
                is FrpCryptoException -> assertEquals(FrpCryptoFailure.IO_FAILURE, outcome.failure)
                is Throwable -> throw AssertionError("Concurrent read propagated an unexpected failure", outcome)
                else -> throw AssertionError("Concurrent read did not complete")
            }
            assertClosedReader(reader)
        }
    }

    private fun privateByteArray(instance: Any, fieldName: String): ByteArray {
        return privateField(instance, fieldName) as? ByteArray
            ?: throw AssertionError("$fieldName was not initialized")
    }

    private fun privateField(instance: Any, fieldName: String): Any? {
        val field = instance.javaClass.getDeclaredField(fieldName)
        field.isAccessible = true
        return field.get(instance)
    }

    private fun assertCleared(label: String, bytes: ByteArray) {
        assertTrue(label, bytes.all { it == 0.toByte() })
    }

    private fun captureFailure(operation: () -> Unit): Throwable {
        try {
            operation()
        } catch (failure: Throwable) {
            return failure
        }
        throw AssertionError("Expected operation to fail")
    }

    private fun bytesForPath(path: String): ByteArray {
        val entry = FrpContractFixtures.manifest.entries.single { it.path == path }
        return FrpContractFixtures.bytes(entry.id)
    }

    private fun fixedBytes(start: Int, count: Int): ByteArray =
        ByteArray(count) { index -> (start + index).toByte() }

    private fun uint32(value: Int): ByteArray = ByteBuffer.allocate(4)
        .order(ByteOrder.BIG_ENDIAN)
        .putInt(value)
        .array()

    private data class CfbVector(
        val cipherEntry: String,
        val plainEntry: String,
        val nonceStart: Int,
    )

    private data class AeadProfile(
        val name: String,
        val algorithm: FrpAeadAlgorithm,
        val clientNonceStart: Int,
        val serverNonceStart: Int,
    )

    private data class AeadMutation(
        val id: String,
        val profile: AeadProfile,
        val readRole: FrpAeadRole,
        val expectedFailure: FrpCryptoFailure,
        val wrongRole: Boolean = false,
    )

    private data class WriterFactory(
        val name: String,
        val sensitiveFields: List<String>,
        val releasedFields: List<String> = emptyList(),
        val create: (OutputStream) -> OutputStream,
    )

    private data class FailurePriorityCase(
        val source: () -> Throwable,
        val cleanup: () -> Throwable,
        val expected: FailureSide,
    )

    private enum class FailureSide {
        SOURCE,
        CLEANUP,
    }

    private data class BlockingReaderCase(
        val name: String,
        val prefix: ByteArray,
        val sensitiveFields: List<String>,
        val releasedFields: List<String>,
        val create: (InputStream) -> InputStream,
    )

    private data class TemporaryBufferObservation(
        val label: String,
        val size: Int,
        val containedData: Boolean,
        val cleared: Boolean,
    )

    private class FailingCloseTrackingOutputStream(
        private val writeFailure: Throwable,
        private val closeFailure: Throwable,
    ) : OutputStream() {
        var closed = false

        override fun write(value: Int) {
            throw writeFailure
        }

        override fun write(bytes: ByteArray, offset: Int, length: Int) {
            throw writeFailure
        }

        override fun close() {
            closed = true
            throw closeFailure
        }
    }

    private class CloseTrackingInputStream(
        private val delegate: InputStream,
    ) : InputStream() {
        var closed = false

        override fun read(): Int = delegate.read()

        override fun read(bytes: ByteArray, offset: Int, length: Int): Int =
            delegate.read(bytes, offset, length)

        override fun close() {
            closed = true
            delegate.close()
        }
    }

    private class CloseUnblocksOutputStream : OutputStream() {
        val entered = CountDownLatch(1)
        private val closeSignal = CountDownLatch(1)

        @Volatile
        var closed = false

        override fun write(value: Int) {
            blockUntilClosed()
        }

        override fun write(bytes: ByteArray, offset: Int, length: Int) {
            blockUntilClosed()
        }

        override fun close() {
            closed = true
            closeSignal.countDown()
        }

        private fun blockUntilClosed() {
            entered.countDown()
            closeSignal.await()
            throw IOException("blocking delegate closed")
        }
    }

    private class PrefixThenCloseBlockingInputStream(
        private val prefix: ByteArray,
    ) : InputStream() {
        val entered = CountDownLatch(1)
        private val closeSignal = CountDownLatch(1)
        private var position = 0

        @Volatile
        var closed = false

        override fun read(): Int {
            if (position < prefix.size) {
                return prefix[position++].toInt() and 0xff
            }
            return blockUntilClosed()
        }

        override fun read(bytes: ByteArray, offset: Int, length: Int): Int {
            if (length == 0) {
                return 0
            }
            if (position < prefix.size) {
                val count = minOf(length, prefix.size - position)
                prefix.copyInto(bytes, offset, position, position + count)
                position += count
                return count
            }
            return blockUntilClosed()
        }

        override fun close() {
            closed = true
            closeSignal.countDown()
        }

        private fun blockUntilClosed(): Int {
            entered.countDown()
            closeSignal.await()
            return -1
        }
    }

    private companion object {
        const val V2_FRAME_HEADER_SIZE = 8
        val profiles = listOf(
            AeadProfile(
                name = FRP_AEAD_AES_256_GCM,
                algorithm = FrpAeadAlgorithm.AES_256_GCM,
                clientNonceStart = 0xa0,
                serverNonceStart = 0xb0,
            ),
            AeadProfile(
                name = FRP_AEAD_XCHACHA20_POLY1305,
                algorithm = FrpAeadAlgorithm.XCHACHA20_POLY1305,
                clientNonceStart = 0xc0,
                serverNonceStart = 0xe0,
            ),
        )
    }
}
