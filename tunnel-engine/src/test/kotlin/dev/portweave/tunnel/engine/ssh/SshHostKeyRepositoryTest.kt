package dev.portweave.tunnel.engine.ssh

import com.jcraft.jsch.HostKeyRepository
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class SshHostKeyRepositoryTest {
    @Test
    fun canonicalSha256FingerprintMatches() {
        val key = "host-key-material".toByteArray()
        val fingerprint = SshHostKeyFingerprint.sha256(key)
        val repository = PinnedSshHostKeyRepository(fingerprint)

        assertEquals(HostKeyRepository.OK, repository.check("logical.example", key))
        assertEquals(fingerprint, repository.matchedFingerprint)
        assertFalse(repository.mismatchObserved)
    }

    @Test
    fun mismatchIsRejectedWithoutLeakingExpectedValue() {
        val expected = SshHostKeyFingerprint.sha256("expected".toByteArray())
        val repository = PinnedSshHostKeyRepository(expected)

        assertEquals(HostKeyRepository.CHANGED, repository.check("logical.example", "actual".toByteArray()))
        assertTrue(repository.mismatchObserved)
        assertNull(repository.matchedFingerprint)
        assertFalse(repository.toString().contains(expected))
    }

    @Test
    fun malformedPinsAreRejected() {
        listOf(
            "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "SHA256:not-valid",
        ).forEach { value ->
            val failure = assertThrows(SshFingerprintFormatException::class.java) {
                PinnedSshHostKeyRepository(value)
            }
            assertFalse(failure.message.orEmpty().contains(value))
        }
    }

    @Test
    fun discoveryCapturesButRejectsKey() {
        val key = "discover-me".toByteArray()
        val repository = DiscoveringSshHostKeyRepository()

        assertEquals(HostKeyRepository.NOT_INCLUDED, repository.check("logical.example", key))
        assertEquals(SshHostKeyFingerprint.sha256(key), repository.result()?.sha256Fingerprint)
    }
}
