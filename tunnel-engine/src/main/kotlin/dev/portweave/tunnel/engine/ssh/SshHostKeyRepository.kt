package dev.portweave.tunnel.engine.ssh

import com.jcraft.jsch.HostKey
import com.jcraft.jsch.HostKeyRepository
import com.jcraft.jsch.UserInfo
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.atomic.AtomicReference

internal class PinnedSshHostKeyRepository(expectedFingerprint: String) : EmptyHostKeyRepository() {
    private val expected = SshHostKeyFingerprint.parse(expectedFingerprint)

    @Volatile
    var mismatchObserved = false
        private set

    @Volatile
    var matchedFingerprint: String? = null
        private set

    override fun check(host: String?, key: ByteArray?): Int {
        if (key == null || key.isEmpty() || !expected.matches(key)) {
            mismatchObserved = true
            return HostKeyRepository.CHANGED
        }
        matchedFingerprint = SshHostKeyFingerprint.sha256(key)
        return HostKeyRepository.OK
    }
}

internal class DiscoveringSshHostKeyRepository : EmptyHostKeyRepository() {
    private val key = AtomicReference<ByteArray?>()

    override fun check(host: String?, key: ByteArray?): Int {
        if (key != null && key.isNotEmpty()) this.key.compareAndSet(null, key.copyOf())
        return HostKeyRepository.NOT_INCLUDED
    }

    fun result(): DiscoveredHostKey? = key.get()?.let { bytes ->
        DiscoveredHostKey(
            algorithm = runCatching { HostKey("discovered", bytes).type }.getOrDefault("unknown"),
            sha256Fingerprint = SshHostKeyFingerprint.sha256(bytes),
        )
    }
}

internal abstract class EmptyHostKeyRepository : HostKeyRepository {
    final override fun add(hostkey: HostKey?, ui: UserInfo?) = Unit
    final override fun remove(host: String?, type: String?) = Unit
    final override fun remove(host: String?, type: String?, key: ByteArray?) = Unit
    final override fun getKnownHostsRepositoryID(): String = "PortWeave in-memory host keys"
    final override fun getHostKey(): Array<HostKey> = emptyArray()
    final override fun getHostKey(host: String?, type: String?): Array<HostKey> = emptyArray()
}

object SshHostKeyFingerprint {
    private const val Prefix = "SHA256:"
    private val Payload = Regex("[A-Za-z0-9+/]{43}")

    fun isValid(value: String): Boolean = runCatching { parse(value); true }.getOrDefault(false)

    internal fun parse(value: String): ParsedFingerprint {
        if (!value.startsWith(Prefix)) throw SshFingerprintFormatException()
        val payload = value.removePrefix(Prefix)
        if (!Payload.matches(payload)) throw SshFingerprintFormatException()
        val digest = try {
            Base64.getDecoder().decode(payload)
        } catch (_: IllegalArgumentException) {
            throw SshFingerprintFormatException()
        }
        if (digest.size != 32 || Base64.getEncoder().withoutPadding().encodeToString(digest) != payload) {
            throw SshFingerprintFormatException()
        }
        return ParsedFingerprint(digest)
    }

    fun sha256(rawKey: ByteArray): String = Prefix + Base64.getEncoder().withoutPadding()
        .encodeToString(MessageDigest.getInstance("SHA-256").digest(rawKey))
}

internal class ParsedFingerprint(private val digest: ByteArray) {
    fun matches(rawKey: ByteArray): Boolean = MessageDigest.isEqual(
        digest,
        MessageDigest.getInstance("SHA-256").digest(rawKey),
    )

    override fun toString(): String = "ParsedFingerprint(<redacted>)"
}

class SshFingerprintFormatException : IllegalArgumentException("SSH fingerprint format is invalid")

data class DiscoveredHostKey(
    val algorithm: String,
    val sha256Fingerprint: String,
) {
    override fun toString(): String = "DiscoveredHostKey(algorithm=$algorithm, sha256Fingerprint=<redacted>)"
}
