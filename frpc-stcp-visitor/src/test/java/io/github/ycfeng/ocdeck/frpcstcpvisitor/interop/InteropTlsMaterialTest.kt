package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.SecureRandom
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class InteropTlsMaterialTest {
    @Test
    fun generatedMaterialUsesJdkParseableCertificateAndPrivateKeyPem() {
        val root = Files.createTempDirectory("frp-interop-tls-test-")
        try {
            val material = InteropTlsMaterial.create(root, SecureRandom())

            val certificate = Files.newInputStream(material.certificatePath).use { input ->
                CertificateFactory.getInstance("X.509").generateCertificate(input) as X509Certificate
            }
            certificate.checkValidity()
            assertTrue(certificate.issuerX500Principal.name.isNotBlank())
            assertTrue(certificate.subjectX500Principal.name.isNotBlank())

            val privateKeyHeader = Files.newInputStream(material.privateKeyPath).use { input ->
                input.readNBytes(PRIVATE_KEY_HEADER_BYTES.size)
            }
            try {
                assertTrue(privateKeyHeader.contentEquals(PRIVATE_KEY_HEADER_BYTES))
            } finally {
                privateKeyHeader.fill(0)
            }
            assertTrue(Files.size(material.privateKeyPath) in 1..MAXIMUM_PRIVATE_KEY_FILE_BYTES)
            assertTrue(material.toString().contains("<redacted>"))
            assertFalse(material.toString().contains(root.toString()))
        } finally {
            deleteTreeBestEffort(root)
        }
    }

    private companion object {
        val PRIVATE_KEY_HEADER_BYTES = "-----BEGIN PRIVATE KEY-----\n".toByteArray(StandardCharsets.US_ASCII)
        const val MAXIMUM_PRIVATE_KEY_FILE_BYTES = 16L * 1024L
    }
}
