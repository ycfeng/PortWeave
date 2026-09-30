package dev.portweave.core.security

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import java.security.KeyStore
import java.util.UUID
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

class AndroidKeystoreCredentialStore(context: Context) : CredentialStore {
    private val preferences = context.applicationContext.getSharedPreferences(
        PreferencesName,
        Context.MODE_PRIVATE,
    )

    override suspend fun put(kind: CredentialKind, value: ByteArray): CredentialRef = onIo {
        require(value.isNotEmpty()) { "Credential cannot be empty" }
        val ref = CredentialRef(UUID.randomUUID())
        val payload = encrypt(ref, kind, value)
        if (!preferences.edit().putString(ref.value.toString(), payload).commit()) {
            throw CredentialUnavailableException()
        }
        ref
    }

    override suspend fun read(ref: CredentialRef): ByteArray = onIo {
        val payload = preferences.getString(ref.value.toString(), null)
            ?: throw CredentialUnavailableException()
        decrypt(ref, payload)
    }

    override suspend fun delete(ref: CredentialRef) = onIo {
        if (!preferences.edit().remove(ref.value.toString()).commit()) {
            throw CredentialUnavailableException()
        }
    }

    override suspend fun rotate(
        ref: CredentialRef,
        kind: CredentialKind,
        value: ByteArray,
    ): CredentialRef {
        val replacement = put(kind, value)
        try {
            delete(ref)
        } catch (_: CredentialUnavailableException) {
            // The new record is usable; stale-record cleanup is best effort.
        }
        return replacement
    }

    private fun encrypt(ref: CredentialRef, kind: CredentialKind, value: ByteArray): String {
        val cipher = Cipher.getInstance(Transformation)
        cipher.init(Cipher.ENCRYPT_MODE, getOrCreateKey())
        cipher.updateAAD(aad(ref, kind))
        val ciphertext = cipher.doFinal(value)
        return listOf(
            RecordVersion,
            kind.name,
            Base64.encodeToString(cipher.iv, Base64.NO_WRAP),
            Base64.encodeToString(ciphertext, Base64.NO_WRAP),
        ).joinToString(Separator)
    }

    private fun decrypt(ref: CredentialRef, payload: String): ByteArray {
        try {
            val parts = payload.split(Separator)
            require(parts.size == 4 && parts[0] == RecordVersion)
            val kind = CredentialKind.valueOf(parts[1])
            val iv = Base64.decode(parts[2], Base64.NO_WRAP)
            val ciphertext = Base64.decode(parts[3], Base64.NO_WRAP)
            val cipher = Cipher.getInstance(Transformation)
            cipher.init(Cipher.DECRYPT_MODE, getOrCreateKey(), GCMParameterSpec(GcmTagBits, iv))
            cipher.updateAAD(aad(ref, kind))
            return cipher.doFinal(ciphertext)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (_: Exception) {
            throw CredentialUnavailableException()
        }
    }

    private fun aad(ref: CredentialRef, kind: CredentialKind): ByteArray =
        "$RecordVersion|${ref.value}|${kind.name}".toByteArray(Charsets.UTF_8)

    private fun getOrCreateKey(): SecretKey {
        val keyStore = KeyStore.getInstance(AndroidKeystore).apply { load(null) }
        (keyStore.getKey(MasterKeyAlias, null) as? SecretKey)?.let { return it }
        val generator = KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, AndroidKeystore)
        generator.init(
            KeyGenParameterSpec.Builder(
                MasterKeyAlias,
                KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT,
            )
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setRandomizedEncryptionRequired(true)
                .build(),
        )
        return generator.generateKey()
    }

    private suspend fun <T> onIo(block: () -> T): T = withContext(Dispatchers.IO) {
        try {
            block()
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (failure: CredentialUnavailableException) {
            throw failure
        } catch (_: Exception) {
            throw CredentialUnavailableException()
        }
    }

    private companion object {
        const val AndroidKeystore = "AndroidKeyStore"
        const val GcmTagBits = 128
        const val MasterKeyAlias = "portweave_credentials_v1"
        const val PreferencesName = "credentials"
        const val RecordVersion = "v1"
        const val Separator = ":"
        const val Transformation = "AES/GCM/NoPadding"
    }
}
