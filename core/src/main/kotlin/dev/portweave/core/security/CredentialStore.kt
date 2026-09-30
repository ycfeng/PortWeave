package dev.portweave.core.security

import java.util.UUID

enum class CredentialKind {
    PASSWORD,
    PRIVATE_KEY,
    PRIVATE_KEY_PASSPHRASE,
    SSH_HOST_FINGERPRINT,
    TOKEN,
    STCP_SECRET,
}

@JvmInline
value class CredentialRef(val value: UUID) {
    override fun toString(): String = "CredentialRef(<redacted>)"
}

class CredentialUnavailableException : IllegalStateException("Credential is unavailable")

interface CredentialStore {
    suspend fun put(kind: CredentialKind, value: ByteArray): CredentialRef
    suspend fun read(ref: CredentialRef): ByteArray
    suspend fun delete(ref: CredentialRef)
    suspend fun rotate(ref: CredentialRef, kind: CredentialKind, value: ByteArray): CredentialRef
}
