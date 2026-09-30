package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.io.ByteArrayInputStream
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.security.KeyPairGenerator
import java.security.MessageDigest
import java.security.PublicKey
import java.security.SecureRandom
import java.security.Signature
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.time.Instant
import java.util.Base64
import java.util.Date
import org.bouncycastle.asn1.ASN1Integer
import org.bouncycastle.asn1.DERBitString
import org.bouncycastle.asn1.DERNull
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.AlgorithmIdentifier
import org.bouncycastle.asn1.x509.BasicConstraints
import org.bouncycastle.asn1.x509.Certificate as BouncyCastleCertificate
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.ExtensionsGenerator
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.asn1.x509.Time
import org.bouncycastle.asn1.x509.V3TBSCertificateGenerator

internal data class InteropTlsMaterial(
    val certificatePath: Path,
    val privateKeyPath: Path,
    val certificateSha256: String,
) {
    override fun toString(): String =
        "InteropTlsMaterial(certificatePresent=true, privateKey=<redacted>)"

    companion object {
        fun create(
            directory: Path,
            secureRandom: SecureRandom,
            certificateAuthority: Boolean = false,
        ): InteropTlsMaterial {
            val certificatePath = directory.resolve(CERTIFICATE_FILE_NAME)
            val privateKeyPath = directory.resolve(PRIVATE_KEY_FILE_NAME)
            var certificateDer = ByteArray(0)
            var privateKeyDer = ByteArray(0)
            var certificatePem = ByteArray(0)
            var privateKeyPem = ByteArray(0)
            var signatureBytes = ByteArray(0)
            try {
                val keyPair = KeyPairGenerator.getInstance("RSA").apply {
                    initialize(RSA_KEY_BITS, secureRandom)
                }.generateKeyPair()
                val signatureAlgorithm = AlgorithmIdentifier(
                    PKCSObjectIdentifiers.sha256WithRSAEncryption,
                    DERNull.INSTANCE,
                )
                val now = Instant.now()
                val certificateGenerator = V3TBSCertificateGenerator().apply {
                    setSerialNumber(ASN1Integer(randomSerialNumber(secureRandom)))
                    setSignature(signatureAlgorithm)
                    setIssuer(X500Name(CERTIFICATE_DISTINGUISHED_NAME))
                    setStartDate(Time(Date.from(now.minusSeconds(CERTIFICATE_CLOCK_SKEW_SECONDS))))
                    setEndDate(Time(Date.from(now.plusSeconds(CERTIFICATE_LIFETIME_SECONDS))))
                    setSubject(X500Name(CERTIFICATE_DISTINGUISHED_NAME))
                    setSubjectPublicKeyInfo(SubjectPublicKeyInfo.getInstance(keyPair.public.encoded))
                    setExtensions(
                        ExtensionsGenerator().apply {
                            if (certificateAuthority) {
                                addExtension(Extension.basicConstraints, true, BasicConstraints(true))
                            }
                            addExtension(
                                Extension.subjectAlternativeName,
                                false,
                                GeneralNames(
                                    arrayOf(
                                        GeneralName(GeneralName.dNSName, "localhost"),
                                        GeneralName(GeneralName.iPAddress, "127.0.0.1"),
                                    ),
                                ),
                            )
                        }.generate(),
                    )
                }
                val tbsCertificate = certificateGenerator.generateTBSCertificate()
                signatureBytes = Signature.getInstance(SIGNATURE_ALGORITHM).run {
                    initSign(keyPair.private, secureRandom)
                    update(tbsCertificate.encoded)
                    sign()
                }
                certificateDer = BouncyCastleCertificate(
                    tbsCertificate,
                    signatureAlgorithm,
                    DERBitString(signatureBytes),
                ).encoded
                privateKeyDer = keyPair.private.encoded
                    ?: throw InteropFailure("temporary TLS private key encoding was unavailable")
                validateCertificate(certificateDer, keyPair.public)
                certificatePem = pemBytes("CERTIFICATE", certificateDer)
                privateKeyPem = pemBytes("PRIVATE KEY", privateKeyDer)
                writePrivateFile(certificatePath, certificatePem)
                writePrivateFile(privateKeyPath, privateKeyPem)
                val certificateSha256 = MessageDigest.getInstance("SHA-256")
                    .digest(certificateDer)
                    .joinToString("") { byte -> "%02x".format(byte) }
                return InteropTlsMaterial(certificatePath, privateKeyPath, certificateSha256)
            } catch (failure: InteropFailure) {
                throw failure
            } catch (_: Exception) {
                throw InteropFailure("temporary TLS material generation failed")
            } finally {
                certificateDer.fill(0)
                privateKeyDer.fill(0)
                certificatePem.fill(0)
                privateKeyPem.fill(0)
                signatureBytes.fill(0)
            }
        }

        private fun validateCertificate(encoded: ByteArray, publicKey: PublicKey) {
            val certificate = CertificateFactory.getInstance("X.509")
                .generateCertificate(ByteArrayInputStream(encoded)) as X509Certificate
            certificate.checkValidity()
            certificate.verify(publicKey)
        }

        private fun randomSerialNumber(secureRandom: SecureRandom): BigInteger {
            val serial = BigInteger(SERIAL_NUMBER_BITS, secureRandom)
            return if (serial.signum() == 0) BigInteger.ONE else serial
        }

        private fun pemBytes(type: String, der: ByteArray): ByteArray {
            val header = "-----BEGIN $type-----\n".toByteArray(StandardCharsets.US_ASCII)
            val footer = "\n-----END $type-----\n".toByteArray(StandardCharsets.US_ASCII)
            val body = Base64.getMimeEncoder(PEM_LINE_BYTES, byteArrayOf('\n'.code.toByte())).encode(der)
            return try {
                ByteArray(header.size + body.size + footer.size).also { output ->
                    header.copyInto(output)
                    body.copyInto(output, header.size)
                    footer.copyInto(output, header.size + body.size)
                }
            } finally {
                body.fill(0)
            }
        }

        private const val CERTIFICATE_FILE_NAME = "frps-cert.pem"
        private const val PRIVATE_KEY_FILE_NAME = "frps-key.pem"
        private const val CERTIFICATE_DISTINGUISHED_NAME = "CN=OC Deck frp interoperability"
        private const val RSA_KEY_BITS = 2048
        private const val SERIAL_NUMBER_BITS = 128
        private const val PEM_LINE_BYTES = 64
        private const val CERTIFICATE_CLOCK_SKEW_SECONDS = 60L * 60L
        private const val CERTIFICATE_LIFETIME_SECONDS = 24L * 60L * 60L
        private const val SIGNATURE_ALGORITHM = "SHA256withRSA"
    }
}
