package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.SecureDirectoryStream
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.AclEntryPermission
import java.nio.file.attribute.AclEntryType
import java.nio.file.attribute.AclFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.UserPrincipal
import java.security.MessageDigest
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json

@Serializable
internal data class AppE2eExternalConfig(
    val schemaVersion: Int,
    val frpsHost: String,
    val frpsPort: Int,
    val authToken: String,
    val tlsServerCertificateSha256: String,
    val tlsTrustedCaCertificatePem: String,
) {
    override fun toString(): String =
        "AppE2eExternalConfig(schemaVersion=$schemaVersion, frpsHost=<redacted>, " +
            "frpsPort=$frpsPort, authToken=<redacted>, tlsServerCertificateSha256=<redacted>, " +
            "tlsTrustedCaCertificatePem=<redacted>)"
}

internal fun loadAppE2eExternalConfig(
    path: Path,
    trustedRoot: Path = defaultAppE2eExternalConfigRoot(),
): AppE2eExternalConfig {
    val normalized = path.toAbsolutePath().normalize()
    val normalizedTrustedRoot = trustedRoot.toAbsolutePath().normalize()
    val trustedRootAttributes = readConcreteAttributes(normalizedTrustedRoot, directory = true)
    val trustedRootIdentity = trustedRootAttributes.concreteIdentity()
    val trustedRootRealPath = concreteRealPath(normalizedTrustedRoot, "root")
    val parentRealPath = try {
        normalized.parent?.toRealPath(LinkOption.NOFOLLOW_LINKS)
    } catch (_: Exception) {
        null
    }
    if (normalized.parent != normalizedTrustedRoot || parentRealPath != trustedRootRealPath ||
        Files.isSymbolicLink(normalized)
    ) {
        throw InteropFailure("App E2E external configuration file was missing")
    }
    val fileAttributes = readConcreteAttributes(normalized, directory = false)
    val fileIdentity = fileAttributes.concreteIdentity()
    requirePrivateOwnership(normalizedTrustedRoot, normalized)
    requirePrivatePermissions(normalizedTrustedRoot, normalized)
    val bytes = Files.newDirectoryStream(normalizedTrustedRoot).use { directoryStream ->
        val channel = if (directoryStream is SecureDirectoryStream<Path>) {
            directoryStream.newByteChannel(
                normalized.fileName,
                setOf(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS),
            )
        } else {
            Files.newByteChannel(
                normalized,
                setOf(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS),
            )
        }
        channel.use {
            val rootAfterOpen = readConcreteAttributes(normalizedTrustedRoot, directory = true)
            val fileAfterOpen = readConcreteAttributes(normalized, directory = false)
            if (rootAfterOpen.concreteIdentity() != trustedRootIdentity ||
                fileAfterOpen.concreteIdentity() != fileIdentity ||
                concreteRealPath(normalizedTrustedRoot, "root") != trustedRootRealPath
            ) {
                throw InteropFailure("App E2E external configuration path changed during open")
            }
            readBoundedExternalConfig(it)
        }
    }
    try {
        if (bytes.isEmpty() || bytes.size > MAXIMUM_EXTERNAL_CONFIG_BYTES) {
            throw InteropFailure("App E2E external configuration size was invalid")
        }
        val config = try {
            EXTERNAL_CONFIG_JSON.decodeFromString<AppE2eExternalConfig>(
                bytes.toString(StandardCharsets.UTF_8),
            )
        } catch (_: Exception) {
            throw InteropFailure("App E2E external configuration schema was invalid")
        }
        validateAppE2eExternalConfig(config)
        return config
    } finally {
        bytes.fill(0)
    }
}

private fun readBoundedExternalConfig(channel: SeekableByteChannel): ByteArray {
    val output = ByteArrayOutputStream(MAXIMUM_EXTERNAL_CONFIG_BYTES)
    val buffer = ByteBuffer.allocate(EXTERNAL_CONFIG_BUFFER_BYTES)
    try {
        while (channel.read(buffer) >= 0) {
            buffer.flip()
            val count = buffer.remaining()
            if (output.size() + count > MAXIMUM_EXTERNAL_CONFIG_BYTES) {
                throw InteropFailure("App E2E external configuration size was invalid")
            }
            output.write(buffer.array(), 0, count)
            buffer.clear()
        }
        return output.toByteArray()
    } finally {
        buffer.array().fill(0)
    }
}

private data class ConcreteFileIdentity(
    val fileKey: Any?,
    val creationTimeMillis: Long,
    val size: Long,
)

private fun BasicFileAttributes.concreteIdentity(): ConcreteFileIdentity = ConcreteFileIdentity(
    fileKey = fileKey(),
    creationTimeMillis = creationTime().toMillis(),
    size = size(),
)

private fun readConcreteAttributes(path: Path, directory: Boolean): BasicFileAttributes {
    val attributes = try {
        Files.readAttributes(path, BasicFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS)
    } catch (_: Exception) {
        throw InteropFailure(
            if (directory) "App E2E external configuration root was invalid"
            else "App E2E external configuration file was missing",
        )
    }
    val validType = if (directory) attributes.isDirectory else attributes.isRegularFile
    if (!validType || attributes.isOther || Files.isSymbolicLink(path)) {
        throw InteropFailure(
            if (directory) "App E2E external configuration root was invalid"
            else "App E2E external configuration file was missing",
        )
    }
    return attributes
}

private fun concreteRealPath(path: Path, label: String): Path = try {
    path.toRealPath(LinkOption.NOFOLLOW_LINKS)
} catch (_: Exception) {
    throw InteropFailure("App E2E external configuration $label was invalid")
}

private fun requirePrivateOwnership(root: Path, file: Path) {
    try {
        val expectedOwner = currentProcessOwner()
        if (Files.getOwner(root) != expectedOwner || Files.getOwner(file) != expectedOwner) {
            throw InteropFailure("App E2E external configuration ownership was invalid")
        }
    } catch (failure: InteropFailure) {
        throw failure
    } catch (_: Exception) {
        throw InteropFailure("App E2E external configuration ownership was unavailable")
    }
}

private fun requirePrivatePermissions(root: Path, file: Path) {
    try {
        if (Files.getFileStore(root).supportsFileAttributeView("posix")) {
            listOf(root, file).forEach { path ->
                val permissions = Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS)
                if (permissions.any { it in NON_OWNER_POSIX_PERMISSIONS }) {
                    throw InteropFailure("App E2E external configuration permissions were invalid")
                }
            }
            return
        }
        val expectedOwner = currentProcessOwner()
        requirePrivateAcl(root, expectedOwner)
        requirePrivateAcl(file, expectedOwner)
    } catch (failure: InteropFailure) {
        throw failure
    } catch (_: Exception) {
        throw InteropFailure("App E2E external configuration permissions were unavailable")
    }
}

private fun requirePrivateAcl(path: Path, expectedOwner: UserPrincipal) {
    val view = Files.getFileAttributeView(path, AclFileAttributeView::class.java, LinkOption.NOFOLLOW_LINKS)
        ?: throw InteropFailure("App E2E external configuration permissions were unavailable")
    if (view.owner != expectedOwner) {
        throw InteropFailure("App E2E external configuration ownership was invalid")
    }
    view.acl.forEach { entry ->
        if (entry.type() != AclEntryType.ALLOW || entry.permissions().none { it in SENSITIVE_ACL_PERMISSIONS }) {
            return@forEach
        }
        if (entry.principal() != expectedOwner) {
            throw InteropFailure("App E2E external configuration permissions were invalid")
        }
    }
}

private fun currentProcessOwner(): UserPrincipal = try {
    currentProcessUserPrincipal(Path.of("").fileSystem)
} catch (_: Exception) {
    throw InteropFailure("App E2E external configuration ownership was unavailable")
}

internal fun validateAppE2eExternalConfig(
    config: AppE2eExternalConfig,
    resolveHost: (String) -> Array<InetAddress> = InetAddress::getAllByName,
) {
    if (config.schemaVersion != EXTERNAL_CONFIG_SCHEMA_VERSION ||
        config.frpsHost.isBlank() || config.frpsHost != config.frpsHost.trim() ||
        config.frpsPort !in 1..65_535 || !SAFE_EXTERNAL_TOKEN.matches(config.authToken) ||
        !TLS_SERVER_CERTIFICATE_SHA256.matches(config.tlsServerCertificateSha256) ||
        config.tlsTrustedCaCertificatePem.length !in 1..MAXIMUM_TRUSTED_CA_PEM_CHARS
    ) {
        throw InteropFailure("App E2E external configuration was invalid")
    }
    val addresses = resolveExternalHost(config.frpsHost, resolveHost)
    if (addresses.isEmpty() || addresses.any { !it.isPublicRoutable() }) {
        throw InteropFailure("App E2E external frps host was not publicly routable")
    }
    validateTrustedCaCertificate(config)
}

internal fun appE2eTrustedCaCertificateBytes(config: AppE2eExternalConfig): ByteArray {
    validateTrustedCaCertificate(config)
    return config.tlsTrustedCaCertificatePem.toByteArray(StandardCharsets.US_ASCII)
}

internal fun hasValidatedActiveTransport(snapshot: String, transport: String): Boolean {
    if (transport !in setOf("WIFI", "CELLULAR")) return false
    val activeNetwork = ACTIVE_DEFAULT_NETWORK.find(snapshot)?.groupValues?.get(1) ?: return false
    return snapshot.lineSequence().any { line ->
        line.contains("network{$activeNetwork}") && line.contains(transport) && line.contains("VALIDATED")
    }
}

internal fun hasValidatedTransport(snapshot: String, transport: String): Boolean =
    transport in setOf("WIFI", "CELLULAR") && snapshot.lineSequence().any { line ->
        line.contains(transport) && line.contains("VALIDATED")
    }

private fun InetAddress.isPublicRoutable(): Boolean {
    if (isAnyLocalAddress || isLoopbackAddress || isLinkLocalAddress || isSiteLocalAddress || isMulticastAddress) {
        return false
    }
    return when (this) {
        is Inet4Address -> {
            val octets = address.map(Byte::toInt).map { it and 0xff }
            when {
                octets[0] == 0 -> false
                octets[0] == 10 -> false
                octets[0] == 100 && octets[1] in 64..127 -> false
                octets[0] == 127 -> false
                octets[0] == 169 && octets[1] == 254 -> false
                octets[0] == 172 && octets[1] in 16..31 -> false
                octets[0] == 192 && octets[1] == 0 && octets[2] in setOf(0, 2) -> false
                octets[0] == 192 && octets[1] == 88 && octets[2] == 99 -> false
                octets[0] == 192 && octets[1] == 168 -> false
                octets[0] == 198 && octets[1] in 18..19 -> false
                octets[0] == 198 && octets[1] == 51 && octets[2] == 100 -> false
                octets[0] == 203 && octets[1] == 0 && octets[2] == 113 -> false
                octets[0] >= 224 -> false
                else -> true
            }
        }
        is Inet6Address -> {
            val bytes = address.map(Byte::toInt).map { it and 0xff }
            val isGlobalUnicast = bytes[0] in 0x20..0x3f
            val isIetfProtocolAssignment = bytes[0] == 0x20 && bytes[1] == 0x01 && bytes[2] in 0x00..0x01
            val isTeredo = bytes.take(4) == listOf(0x20, 0x01, 0x00, 0x00)
            val isBenchmark = bytes.take(6) == listOf(0x20, 0x01, 0x00, 0x02, 0x00, 0x00)
            val isDocumentation = bytes.take(4) == listOf(0x20, 0x01, 0x0d, 0xb8)
            val isOrchid = bytes.take(3) == listOf(0x20, 0x01, 0x00) &&
                (bytes[3] and 0xf0) in setOf(0x10, 0x20)
            val isSixToFour = bytes[0] == 0x20 && bytes[1] == 0x02
            val isDocumentationV2 = bytes[0] == 0x3f && bytes[1] == 0xff && bytes[2] and 0xf0 == 0
            isGlobalUnicast && !isIetfProtocolAssignment && !isTeredo && !isBenchmark && !isDocumentation && !isOrchid &&
                !isSixToFour && !isDocumentationV2
        }
        else -> false
    }
}

private fun resolveExternalHost(
    host: String,
    resolveHost: (String) -> Array<InetAddress>,
): List<InetAddress> {
    val literal = when {
        IPV4_LITERAL.matches(host) -> parseIpv4Literal(host)
        ':' in host && '%' !in host -> try {
            InetAddress.getByName(host).takeIf { it is Inet6Address }
        } catch (_: Exception) {
            null
        }
        else -> null
    }
    if (literal != null) return listOf(literal)
    if (!SAFE_PUBLIC_DNS_NAME.matches(host)) {
        throw InteropFailure("App E2E external frps host was invalid")
    }
    return try {
        resolveHost(host).toList()
    } catch (_: Exception) {
        throw InteropFailure("App E2E external frps host could not be resolved")
    }
}

private fun parseIpv4Literal(host: String): Inet4Address? {
    val octets = host.split('.').map { part ->
        if (part.length > 1 && part.startsWith('0')) return null
        part.toIntOrNull()?.takeIf { it in 0..255 } ?: return null
    }
    return InetAddress.getByAddress(octets.map(Int::toByte).toByteArray()) as Inet4Address
}

private fun validateTrustedCaCertificate(config: AppE2eExternalConfig) {
    val pemBytes = config.tlsTrustedCaCertificatePem.toByteArray(StandardCharsets.US_ASCII)
    try {
        if (!config.tlsTrustedCaCertificatePem.startsWith("-----BEGIN CERTIFICATE-----") ||
            !config.tlsTrustedCaCertificatePem.trimEnd().endsWith("-----END CERTIFICATE-----")
        ) {
            throw InteropFailure("App E2E external TLS certificate was invalid")
        }
        val certificates = try {
            ByteArrayInputStream(pemBytes).use { input ->
                CertificateFactory.getInstance("X.509").generateCertificates(input)
            }
        } catch (_: Exception) {
            throw InteropFailure("App E2E external TLS certificate was invalid")
        }
        val certificate = certificates.singleOrNull() as? X509Certificate
            ?: throw InteropFailure("App E2E external TLS certificate was invalid")
        try {
            certificate.checkValidity()
        } catch (_: Exception) {
            throw InteropFailure("App E2E external TLS certificate was invalid")
        }
        if (certificate.basicConstraints >= 0) {
            throw InteropFailure("App E2E external TLS certificate must be a non-CA leaf")
        }
        val actualPin = MessageDigest.getInstance("SHA-256").digest(certificate.encoded)
        val expectedPin = config.tlsServerCertificateSha256.decodeSha256()
        try {
            if (!MessageDigest.isEqual(actualPin, expectedPin)) {
                throw InteropFailure("App E2E external TLS certificate pin did not match")
            }
        } finally {
            actualPin.fill(0)
            expectedPin.fill(0)
        }
    } finally {
        pemBytes.fill(0)
    }
}

private fun String.decodeSha256(): ByteArray = ByteArray(length / 2) { index ->
    substring(index * 2, index * 2 + 2).toInt(16).toByte()
}

private val EXTERNAL_CONFIG_JSON = Json {
    ignoreUnknownKeys = false
    explicitNulls = false
}
private val IPV4_LITERAL = Regex("[0-9]{1,3}(?:\\.[0-9]{1,3}){3}")
private val SAFE_PUBLIC_DNS_NAME = Regex(
    "(?=.{1,253}$)(?=.*\\.)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\\.)+" +
        "[A-Za-z](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?",
)
private val SAFE_EXTERNAL_TOKEN = Regex("[A-Za-z0-9_-]{16,256}")
private val TLS_SERVER_CERTIFICATE_SHA256 = Regex("[0-9a-f]{64}")
private val ACTIVE_DEFAULT_NETWORK = Regex("Active default network: ([0-9]{1,10})")
private val NON_OWNER_POSIX_PERMISSIONS = setOf(
    PosixFilePermission.GROUP_READ,
    PosixFilePermission.GROUP_WRITE,
    PosixFilePermission.GROUP_EXECUTE,
    PosixFilePermission.OTHERS_READ,
    PosixFilePermission.OTHERS_WRITE,
    PosixFilePermission.OTHERS_EXECUTE,
)
private val SENSITIVE_ACL_PERMISSIONS = setOf(
    AclEntryPermission.READ_DATA,
    AclEntryPermission.WRITE_DATA,
    AclEntryPermission.APPEND_DATA,
    AclEntryPermission.READ_NAMED_ATTRS,
    AclEntryPermission.WRITE_NAMED_ATTRS,
    AclEntryPermission.EXECUTE,
    AclEntryPermission.DELETE_CHILD,
    AclEntryPermission.READ_ATTRIBUTES,
    AclEntryPermission.WRITE_ATTRIBUTES,
    AclEntryPermission.DELETE,
    AclEntryPermission.READ_ACL,
    AclEntryPermission.WRITE_ACL,
    AclEntryPermission.WRITE_OWNER,
)
internal fun defaultAppE2eExternalConfigRoot(): Path =
    Path.of(System.getProperty("java.io.tmpdir"), "opencode").toAbsolutePath().normalize()
private const val EXTERNAL_CONFIG_SCHEMA_VERSION = 3
private const val MAXIMUM_EXTERNAL_CONFIG_BYTES = 16 * 1024
private const val EXTERNAL_CONFIG_BUFFER_BYTES = 4 * 1024
private const val MAXIMUM_TRUSTED_CA_PEM_CHARS = 8 * 1024
