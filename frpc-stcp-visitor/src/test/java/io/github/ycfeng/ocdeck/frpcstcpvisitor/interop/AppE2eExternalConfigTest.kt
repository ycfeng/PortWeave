package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.nio.charset.StandardCharsets
import java.net.InetAddress
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.AclEntry
import java.nio.file.attribute.AclEntryPermission
import java.nio.file.attribute.AclEntryType
import java.nio.file.attribute.AclFileAttributeView
import java.nio.file.attribute.PosixFilePermission
import java.security.MessageDigest
import java.security.SecureRandom
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.EnumSet
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Test

class AppE2eExternalConfigTest {
    @Test
    fun acceptsPublicEndpointsAndRedactsStructuralSummary() {
        val fixture = tlsFixture()
        val ipv4 = config("8.8.8.8", fixture)
        val ipv6 = config("2001:4860:4860::8888", fixture)

        validateAppE2eExternalConfig(ipv4)
        validateAppE2eExternalConfig(ipv6)
        validateAppE2eExternalConfig(config("frps.example.com", fixture)) {
            arrayOf(InetAddress.getByName("8.8.8.8"), InetAddress.getByName("2001:4860:4860::8888"))
        }
        assertFalse(ipv4.toString().contains(fixture.pin))
        assertFalse(ipv4.toString().contains("BEGIN CERTIFICATE"))
    }

    @Test
    fun rejectsNonPublicEndpointsAndInvalidCredentials() {
        val fixture = tlsFixture()
        listOf(
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.0.1",
            "192.0.2.1",
            "100.64.0.1",
            "169.254.1.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "240.0.0.1",
            "::1",
            "fe80::1",
            "fd00::1",
            "2001:2::1",
            "2001:db8::1",
            "2001:20::1",
            "2001::1",
            "2001:5::1",
            "2002::1",
            "3fff::1",
            "example",
            "-bad.example.com",
            "bad_.example.com",
            "01.2.3.4",
        ).forEach { host ->
            assertThrows(InteropFailure::class.java) {
                validateAppE2eExternalConfig(config(host, fixture))
            }
        }
        assertThrows(InteropFailure::class.java) {
            validateAppE2eExternalConfig(config("frps.example.com", fixture)) {
                arrayOf(InetAddress.getByName("192.168.1.1"))
            }
        }
        assertThrows(InteropFailure::class.java) {
            validateAppE2eExternalConfig(config("frps.example.com", fixture)) {
                arrayOf(InetAddress.getByName("8.8.8.8"), InetAddress.getByName("10.0.0.1"))
            }
        }
        assertThrows(InteropFailure::class.java) {
            validateAppE2eExternalConfig(config("frps.example.com", fixture)) { emptyArray() }
        }
        assertThrows(InteropFailure::class.java) {
            validateAppE2eExternalConfig(config("frps.example.com", fixture)) { throw java.net.UnknownHostException() }
        }
        assertThrows(InteropFailure::class.java) {
            validateAppE2eExternalConfig(config("8.8.8.8", fixture).copy(authToken = "short"))
        }
        assertThrows(InteropFailure::class.java) {
            validateAppE2eExternalConfig(
                config("8.8.8.8", fixture).copy(tlsServerCertificateSha256 = "A".repeat(64)),
            )
        }
        assertThrows(InteropFailure::class.java) {
            validateAppE2eExternalConfig(
                config("8.8.8.8", fixture).copy(tlsServerCertificateSha256 = "0".repeat(64)),
            )
        }
        val caFixture = tlsFixture(certificateAuthority = true)
        assertThrows(InteropFailure::class.java) {
            validateAppE2eExternalConfig(config("8.8.8.8", caFixture))
        }
    }

    @Test
    fun loadsOnlyBoundedStrictJson() {
        val directory = Files.createTempDirectory("app-e2e-external-config-root-")
        try {
            securePermissions(directory, directory = true)
            val fixture = tlsFixture()
            val valid = directory.resolve("valid.json")
            Files.write(valid, encodedConfig(fixture).toByteArray(StandardCharsets.UTF_8))
            securePermissions(valid, directory = false)
            check(loadAppE2eExternalConfig(valid, directory).frpsPort == 7000)

            val unknown = directory.resolve("unknown.json")
            Files.write(
                unknown,
                encodedConfig(fixture, extra = ",\"extra\":true").toByteArray(StandardCharsets.UTF_8),
            )
            securePermissions(unknown, directory = false)
            assertThrows(InteropFailure::class.java) { loadAppE2eExternalConfig(unknown, directory) }

            val oversized = directory.resolve("oversized.json")
            Files.write(oversized, ByteArray(16 * 1024 + 1) { 'x'.code.toByte() })
            securePermissions(oversized, directory = false)
            assertThrows(InteropFailure::class.java) { loadAppE2eExternalConfig(oversized, directory) }

            val nestedDirectory = Files.createDirectory(directory.resolve("nested"))
            val nested = nestedDirectory.resolve("nested.json")
            Files.write(nested, encodedConfig(fixture).toByteArray(StandardCharsets.UTF_8))
            assertThrows(InteropFailure::class.java) { loadAppE2eExternalConfig(nested, directory) }

            val target = directory.resolve("target.json")
            Files.write(target, encodedConfig(fixture).toByteArray(StandardCharsets.UTF_8))
            securePermissions(target, directory = false)
            val link = directory.resolve("link.json")
            try {
                Files.createSymbolicLink(link, target.fileName)
                assertThrows(InteropFailure::class.java) { loadAppE2eExternalConfig(link, directory) }
            } catch (_: UnsupportedOperationException) {
                // The no-follow open is still exercised on platforms that support symbolic links.
            } catch (_: java.nio.file.FileSystemException) {
                // Windows may require elevated symbolic-link privileges.
            }
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun parsesOnlyTheValidatedActiveTransport() {
        val snapshot = """
            Active default network: 101
            NetworkAgentInfo{network{100} nc{[ Transports: CELLULAR Capabilities: INTERNET&VALIDATED ]}}
            NetworkAgentInfo{network{101} nc{[ Transports: WIFI Capabilities: INTERNET&VALIDATED ]}}
        """.trimIndent()

        check(hasValidatedActiveTransport(snapshot, "WIFI"))
        check(!hasValidatedActiveTransport(snapshot, "CELLULAR"))
        check(hasValidatedTransport(snapshot, "CELLULAR"))
        check(!hasValidatedActiveTransport(snapshot.replace("VALIDATED", "CAPTIVE_PORTAL"), "WIFI"))
    }

    private fun tlsFixture(certificateAuthority: Boolean = false): TlsFixture {
        val directory = Files.createTempDirectory("app-e2e-external-tls-")
        return try {
            val material = InteropTlsMaterial.create(
                directory,
                SecureRandom(),
                certificateAuthority = certificateAuthority,
            )
            val pem = Files.newInputStream(material.certificatePath).use { input ->
                input.readBytes().toString(StandardCharsets.US_ASCII)
            }
            val certificate = Files.newInputStream(material.certificatePath).use { input ->
                CertificateFactory.getInstance("X.509").generateCertificate(input) as X509Certificate
            }
            val pin = MessageDigest.getInstance("SHA-256").digest(certificate.encoded)
                .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
            TlsFixture(pin, pem)
        } finally {
            directory.toFile().deleteRecursively()
        }
    }

    private fun config(host: String, fixture: TlsFixture) = AppE2eExternalConfig(
        schemaVersion = 3,
        frpsHost = host,
        frpsPort = 7000,
        authToken = TOKEN,
        tlsServerCertificateSha256 = fixture.pin,
        tlsTrustedCaCertificatePem = fixture.pem,
    )

    private fun encodedConfig(fixture: TlsFixture, extra: String = ""): String =
        JSON.encodeToString(config("8.8.8.8", fixture)).dropLast(1) + extra + "}"

    private fun securePermissions(path: Path, directory: Boolean) {
        if (Files.getFileStore(path).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(
                path,
                if (directory) {
                    setOf(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE,
                    )
                } else {
                    setOf(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)
                },
            )
            return
        }
        val view = Files.getFileAttributeView(path, AclFileAttributeView::class.java)
            ?: throw AssertionError("ACL view was unavailable")
        view.acl = listOf(
            AclEntry.newBuilder()
                .setType(AclEntryType.ALLOW)
                .setPrincipal(view.owner)
                .setPermissions(EnumSet.allOf(AclEntryPermission::class.java))
                .build(),
        )
    }

    private data class TlsFixture(val pin: String, val pem: String)

    private companion object {
        const val TOKEN = "auth_0123456789abcdefghijklmnop"
        val JSON = Json { encodeDefaults = true }
    }
}
