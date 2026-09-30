package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FRP_AEAD_AES_256_GCM
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FRP_AEAD_XCHACHA20_POLY1305
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV2
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.RawClientHello
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.RawServerHello
import java.io.Closeable
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.CancellationException
import org.bouncycastle.crypto.digests.SHA256Digest
import org.bouncycastle.crypto.generators.HKDFBytesGenerator
import org.bouncycastle.crypto.params.HKDFParameters

internal enum class FrpAeadAlgorithm(
    val wireName: String,
    val nonceSize: Int,
) {
    AES_256_GCM(FRP_AEAD_AES_256_GCM, 12),
    XCHACHA20_POLY1305(FRP_AEAD_XCHACHA20_POLY1305, 24),
    ;

    companion object {
        fun fromWireName(value: String): FrpAeadAlgorithm =
            entries.firstOrNull { it.wireName == value }
                ?: throw FrpCryptoException(FrpCryptoFailure.UNSUPPORTED_ALGORITHM)
    }
}

internal enum class FrpAeadDirection(val wireName: String) {
    CLIENT_TO_SERVER("client-to-server"),
    SERVER_TO_CLIENT("server-to-client"),
}

internal enum class FrpAeadRole {
    CLIENT,
    SERVER,
}

internal class FrpV2CryptoContext private constructor(
    val algorithm: FrpAeadAlgorithm,
    transcriptHash: ByteArray,
) {
    private val transcriptHashBytes: ByteArray = transcriptHash.copyOf()

    val transcriptHash: ByteArray
        get() = transcriptHashBytes.copyOf()

    override fun toString(): String =
        "FrpV2CryptoContext(algorithm=${algorithm.wireName}, transcriptHashLength=${transcriptHashBytes.size})"

    companion object {
        fun forClient(clientHello: RawClientHello, serverHello: RawServerHello): FrpV2CryptoContext {
            val algorithm = FrpWireV2.validateClientCryptoNegotiation(clientHello, serverHello)
            val transcriptHash = FrpV2Crypto.hashTranscript(clientHello.rawPayload, serverHello.rawPayload)
            return try {
                FrpV2CryptoContext(
                    algorithm = FrpAeadAlgorithm.fromWireName(algorithm),
                    transcriptHash = transcriptHash,
                )
            } finally {
                transcriptHash.fill(0)
            }
        }
    }
}

internal class FrpAeadControlKeys(
    sourceClientToServer: ByteArray,
    sourceServerToClient: ByteArray,
) : Closeable {
    private val clientToServer: ByteArray
    private val serverToClient: ByteArray
    private var destroyed = false

    init {
        if (sourceClientToServer.size != FrpV2Crypto.KEY_SIZE ||
            sourceServerToClient.size != FrpV2Crypto.KEY_SIZE
        ) {
            throw FrpCryptoException(FrpCryptoFailure.INVALID_KEY)
        }
        val clientToServerCopy = sourceClientToServer.copyOf()
        val serverToClientCopy = try {
            sourceServerToClient.copyOf()
        } catch (failure: Throwable) {
            clientToServerCopy.fill(0)
            throw failure
        }
        clientToServer = clientToServerCopy
        serverToClient = serverToClientCopy
    }

    @Synchronized
    fun writeKey(role: FrpAeadRole): ByteArray = when (role) {
        FrpAeadRole.CLIENT -> copyAvailable(clientToServer)
        FrpAeadRole.SERVER -> copyAvailable(serverToClient)
    }

    @Synchronized
    fun readKey(role: FrpAeadRole): ByteArray = when (role) {
        FrpAeadRole.CLIENT -> copyAvailable(serverToClient)
        FrpAeadRole.SERVER -> copyAvailable(clientToServer)
    }

    @Synchronized
    fun destroy() {
        if (destroyed) {
            return
        }
        clientToServer.fill(0)
        serverToClient.fill(0)
        destroyed = true
    }

    override fun close() = destroy()

    @Synchronized
    override fun toString(): String =
        "FrpAeadControlKeys(destroyed=$destroyed, clientToServer=<redacted>, serverToClient=<redacted>)"

    private fun copyAvailable(key: ByteArray): ByteArray {
        if (destroyed) {
            throw FrpCryptoException(FrpCryptoFailure.INVALID_KEY)
        }
        return key.copyOf()
    }
}

internal object FrpV2Crypto {
    const val KEY_SIZE = 32

    fun context(clientHello: RawClientHello, serverHello: RawServerHello): FrpV2CryptoContext =
        FrpV2CryptoContext.forClient(clientHello, serverHello)

    fun hashTranscript(clientHelloPayload: ByteArray, serverHelloPayload: ByteArray): ByteArray {
        val digest = try {
            MessageDigest.getInstance("SHA-256")
        } catch (exception: Exception) {
            throw FrpCryptoException(FrpCryptoFailure.UNSUPPORTED_ALGORITHM)
        }
        digest.update(TRANSCRIPT_LABEL)
        updateTranscriptPart(digest, CLIENT_HELLO_LABEL, clientHelloPayload)
        updateTranscriptPart(digest, SERVER_HELLO_LABEL, serverHelloPayload)
        return digest.digest()
    }

    fun deriveControlKeys(
        token: String,
        context: FrpV2CryptoContext,
    ): FrpAeadControlKeys {
        val clientToServer = deriveControlKeyPrevalidated(token, context, FrpAeadDirection.CLIENT_TO_SERVER)
        var serverToClient: ByteArray? = null
        try {
            val derivedServerToClient = deriveControlKeyPrevalidated(
                token,
                context,
                FrpAeadDirection.SERVER_TO_CLIENT,
            )
            serverToClient = derivedServerToClient
            return FrpAeadControlKeys(clientToServer, derivedServerToClient)
        } finally {
            clientToServer.fill(0)
            serverToClient?.fill(0)
        }
    }

    private fun deriveControlKeyPrevalidated(
        token: String,
        context: FrpV2CryptoContext,
        direction: FrpAeadDirection,
    ): ByteArray {
        val transcriptHash = context.transcriptHash
        if (transcriptHash.size != KEY_SIZE) {
            transcriptHash.fill(0)
            throw FrpCryptoException(FrpCryptoFailure.INVALID_KEY)
        }
        val inputKeyMaterial = token.toByteArray(StandardCharsets.UTF_8)
        val info = "$HKDF_INFO_PREFIX ${context.algorithm.wireName} ${direction.wireName}"
            .toByteArray(StandardCharsets.US_ASCII)
        val output = ByteArray(KEY_SIZE)
        var completed = false
        return try {
            val generator = HKDFBytesGenerator(SHA256Digest())
            generator.init(HKDFParameters(inputKeyMaterial, transcriptHash, info))
            generator.generateBytes(output, 0, output.size)
            completed = true
            output
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: Exception) {
            throw FrpCryptoException(FrpCryptoFailure.INVALID_KEY)
        } finally {
            if (!completed) {
                output.fill(0)
            }
            inputKeyMaterial.fill(0)
            transcriptHash.fill(0)
            info.fill(0)
        }
    }

    private fun updateTranscriptPart(digest: MessageDigest, label: ByteArray, payload: ByteArray) {
        digest.update(0)
        digest.update(label)
        digest.update(0)
        val length = ByteArray(Long.SIZE_BYTES)
        var value = payload.size.toLong()
        for (index in length.indices.reversed()) {
            length[index] = value.toByte()
            value = value ushr 8
        }
        digest.update(length)
        digest.update(payload)
    }

    private const val HKDF_INFO_PREFIX = "frp wire v2 control aead"
    private val TRANSCRIPT_LABEL = "frp wire v2 crypto transcript".toByteArray(StandardCharsets.US_ASCII)
    private val CLIENT_HELLO_LABEL = "client hello".toByteArray(StandardCharsets.US_ASCII)
    private val SERVER_HELLO_LABEL = "server hello".toByteArray(StandardCharsets.US_ASCII)
}
