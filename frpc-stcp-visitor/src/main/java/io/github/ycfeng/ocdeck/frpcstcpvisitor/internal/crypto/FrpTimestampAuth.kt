package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.CancellationException

internal object FrpTimestampAuth {
    fun key(secret: String, timestamp: Long): String {
        val digest = try {
            MessageDigest.getInstance("MD5")
        } catch (exception: Exception) {
            throw FrpCryptoException(FrpCryptoFailure.UNSUPPORTED_ALGORITHM)
        }
        try {
            digest.update(secret.toByteArray(StandardCharsets.UTF_8))
            digest.update(timestamp.toString().toByteArray(StandardCharsets.US_ASCII))
            return digest.digest().joinToString(separator = "") { byte ->
                HEX_DIGITS[(byte.toInt() ushr 4) and 0x0f].toString() + HEX_DIGITS[byte.toInt() and 0x0f]
            }
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: Exception) {
            throw FrpCryptoException(FrpCryptoFailure.IO_FAILURE)
        }
    }

    private const val HEX_DIGITS = "0123456789abcdef"
}
