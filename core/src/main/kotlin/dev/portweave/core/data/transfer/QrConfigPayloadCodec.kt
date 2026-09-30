package dev.portweave.core.data.transfer

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.zip.Deflater
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

/** Versioned QR payload. V2 uses compact JSON, maximum-level GZIP, then QR-friendly Base45. */
object QrConfigPayloadCodec {
    private const val V2Prefix = "PORTWEAVE:2:"
    private const val V1Prefix = "PORTWEAVE:1:GZIP:"
    private const val MaxEncodedChars = 6 * 1024 * 1024
    private const val MaxDecodedBytes = 4 * 1024 * 1024

    fun encode(json: String): String {
        val compact = QrCompactConfigFormat.compactIfTransferJson(json)
        return V2Prefix + Base45.encode(gzip(compact))
    }

    fun decodeIfEncoded(payload: String): String {
        require(payload.length <= MaxEncodedChars) { "二维码数据过大" }
        return when {
            payload.startsWith(V2Prefix) -> {
                val compressed = runCatching { Base45.decode(payload.substring(V2Prefix.length)) }
                    .getOrElse { throw IllegalArgumentException("二维码 Base45 数据无效", it) }
                QrCompactConfigFormat.expandIfCompact(gunzip(compressed))
            }
            payload.startsWith(V1Prefix) -> {
                val compressed = runCatching { Base64.getDecoder().decode(payload.substring(V1Prefix.length)) }
                    .getOrElse { throw IllegalArgumentException("二维码 Base64 数据无效", it) }
                gunzip(compressed)
            }
            else -> payload
        }
    }

    private fun gzip(value: String): ByteArray = ByteArrayOutputStream().use { output ->
        object : GZIPOutputStream(output) {
            init { def.setLevel(Deflater.BEST_COMPRESSION) }
        }.use { gzip -> gzip.write(value.toByteArray(Charsets.UTF_8)) }
        output.toByteArray()
    }

    private fun gunzip(compressed: ByteArray): String {
        val output = ByteArrayOutputStream()
        GZIPInputStream(ByteArrayInputStream(compressed)).use { gzip ->
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val read = gzip.read(buffer)
                if (read < 0) break
                require(output.size() + read <= MaxDecodedBytes) { "二维码解压后的数据过大" }
                output.write(buffer, 0, read)
            }
        }
        return output.toString(Charsets.UTF_8.name())
    }
}
