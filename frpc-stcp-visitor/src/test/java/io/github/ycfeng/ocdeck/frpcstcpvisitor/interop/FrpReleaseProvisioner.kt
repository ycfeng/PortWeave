package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.HttpURLConnection
import java.net.URI
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException
import java.nio.charset.CharacterCodingException
import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermission
import java.security.MessageDigest
import java.util.Locale
import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutionException
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream

internal data class FrpReleaseBinaries(
    val frpc: Path,
    val frps: Path,
) {
    override fun toString(): String = "FrpReleaseBinaries(frpcPresent=true, frpsPresent=true)"
}

internal enum class FrpArchiveKind {
    ZIP,
    TAR_GZ,
}

internal data class FrpArchiveLimits(
    val maximumArchiveBytes: Long = 128L * 1024L * 1024L,
    val maximumExpandedArchiveBytes: Long = 256L * 1024L * 1024L,
    val maximumEntryBytes: Long = 64L * 1024L * 1024L,
    val maximumEntries: Int = 1_024,
    val maximumNameBytes: Int = 4 * 1024,
) {
    init {
        require(maximumArchiveBytes > 0L)
        require(maximumExpandedArchiveBytes > 0L)
        require(maximumEntryBytes > 0L)
        require(maximumEntries > 0)
        require(maximumNameBytes > 0)
    }
}

internal class FrpReleaseProvisioner(
    private val cacheDirectory: Path,
    private val limits: FrpArchiveLimits = FrpArchiveLimits(),
) {
    fun provision(): FrpReleaseBinaries = try {
        createPrivateDirectory(cacheDirectory)
        val lockPath = cacheDirectory.resolve("provision.lock")
        FileChannel.open(
            lockPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
        ).use { channel ->
            acquireProvisionLock(channel).use { provisionLocked(FrpReleasePins.current()) }
        }
    } catch (failure: InteropFailure) {
        throw failure
    } catch (_: Exception) {
        throw InteropFailure("official frp provisioning failed")
    }

    private fun provisionLocked(pin: FrpAssetPin): FrpReleaseBinaries {
        val archive = cacheDirectory.resolve(pin.fileName)
        if (!isVerifiedCacheEntry(archive, pin.sha256)) {
            try {
                Files.deleteIfExists(archive)
            } catch (_: Exception) {
                throw InteropFailure("invalid cached frp archive could not be replaced")
            }
            downloadAndInstallArchive(pin, archive)
        }
        verifyArchiveSha256(archive, pin.sha256, limits.maximumArchiveBytes)

        val staging = Files.createTempDirectory(cacheDirectory, ".extract-")
        try {
            createPrivateDirectory(staging)
            val extracted = extractPinnedExecutables(
                archive = archive,
                kind = pin.archiveKind,
                executableSuffix = pin.executableSuffix,
                outputDirectory = staging,
                limits = limits,
            )
            val stagingRedactor = InteropRedactor(
                cacheDirectory.toString(),
                staging.toString(),
                extracted.frpc.toString(),
                extracted.frps.toString(),
            )
            makeExecutable(extracted.frpc)
            makeExecutable(extracted.frps)
            verifyVersion(extracted.frpc, staging, stagingRedactor, "frpc-version")
            verifyVersion(extracted.frps, staging, stagingRedactor, "frps-version")

            val binaryDirectory = cacheDirectory.resolve("bin-${pin.platform}")
            createPrivateDirectory(binaryDirectory)
            val installedFrpc = binaryDirectory.resolve("frpc${pin.executableSuffix}")
            val installedFrps = binaryDirectory.resolve("frps${pin.executableSuffix}")
            atomicReplace(extracted.frpc, installedFrpc)
            atomicReplace(extracted.frps, installedFrps)
            makeExecutable(installedFrpc)
            makeExecutable(installedFrps)
            val installedRedactor = stagingRedactor.including(
                installedFrpc.toString(),
                installedFrps.toString(),
            )
            verifyVersion(installedFrpc, binaryDirectory, installedRedactor, "frpc-version")
            verifyVersion(installedFrps, binaryDirectory, installedRedactor, "frps-version")
            return FrpReleaseBinaries(installedFrpc, installedFrps)
        } finally {
            deleteTreeBestEffort(staging)
        }
    }

    private fun isVerifiedCacheEntry(path: Path, expectedSha256: String): Boolean {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(path)) return false
        return try {
            verifyArchiveSha256(path, expectedSha256, limits.maximumArchiveBytes)
            true
        } catch (_: InteropFailure) {
            false
        }
    }

    private fun downloadAndInstallArchive(pin: FrpAssetPin, target: Path) {
        val temporary = Files.createTempFile(cacheDirectory, ".download-", ".tmp")
        try {
            downloadBounded(pin.downloadUri, temporary, limits.maximumArchiveBytes)
            verifyArchiveSha256(temporary, pin.sha256, limits.maximumArchiveBytes)
            atomicReplace(temporary, target)
        } finally {
            try {
                Files.deleteIfExists(temporary)
            } catch (_: Exception) {
                // A failed download remains unusable because it was never atomically installed.
            }
        }
    }

    private fun downloadBounded(initialUri: URI, destination: Path, maximumBytes: Long) =
        runFrpDownloadWithHardDeadline(DOWNLOAD_TOTAL_TIMEOUT_MILLIS) {
            downloadBoundedBlocking(initialUri, destination, maximumBytes)
        }

    private fun downloadBoundedBlocking(initialUri: URI, destination: Path, maximumBytes: Long) {
        var current = initialUri
        val deadline = InteropDeadline.afterMillis(DOWNLOAD_TOTAL_TIMEOUT_MILLIS)
        repeat(MAXIMUM_HTTP_REDIRECTS + 1) { redirectCount ->
            if (deadline.isExpired()) throw InteropFailure("frp asset download exceeded its deadline")
            validateDownloadUri(current)
            val connection = try {
                current.toURL().openConnection() as HttpURLConnection
            } catch (_: Exception) {
                throw InteropFailure("frp asset download could not be opened")
            }
            var deadlineGuard: DownloadDeadlineGuard? = null
            try {
                connection.instanceFollowRedirects = false
                connection.connectTimeout = boundedTimeoutMillis(deadline, HTTP_CONNECT_TIMEOUT_MILLIS)
                connection.readTimeout = boundedTimeoutMillis(deadline, HTTP_READ_TIMEOUT_MILLIS)
                connection.useCaches = false
                connection.setRequestProperty("Accept-Encoding", "identity")
                connection.setRequestProperty("User-Agent", "OC-Deck-frp-interop-harness")
                val guard = DownloadDeadlineGuard(connection, deadline)
                deadlineGuard = guard
                val status = try {
                    connection.responseCode
                } catch (_: Exception) {
                    guard.throwIfExpired()
                    throw InteropFailure("frp asset download failed")
                }
                guard.throwIfExpired()
                if (status in HTTP_REDIRECT_CODES) {
                    if (redirectCount == MAXIMUM_HTTP_REDIRECTS) {
                        throw InteropFailure("frp asset download exceeded its redirect limit")
                    }
                    val location = connection.getHeaderField("Location")
                        ?: throw InteropFailure("frp asset redirect was invalid")
                    current = try {
                        current.resolve(location)
                    } catch (_: Exception) {
                        throw InteropFailure("frp asset redirect was invalid")
                    }
                    guard.throwIfExpired()
                    return@repeat
                }
                if (status != HttpURLConnection.HTTP_OK) {
                    throw InteropFailure("frp asset download returned an unexpected status")
                }
                val contentEncoding = connection.contentEncoding
                if (contentEncoding != null && !contentEncoding.equals("identity", ignoreCase = true)) {
                    throw InteropFailure("frp asset download used an unsupported content encoding")
                }
                val declaredLength = connection.contentLengthLong
                if (declaredLength > maximumBytes) {
                    throw InteropFailure("frp asset archive exceeded its download limit")
                }
                Files.newOutputStream(
                    destination,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                ).use { fileOutput ->
                    connection.inputStream.use { networkInput ->
                        guard.throwIfExpired()
                        BufferedInputStream(networkInput, COPY_BUFFER_BYTES).use { input ->
                            val output = BufferedOutputStream(fileOutput, COPY_BUFFER_BYTES)
                            output.use {
                                copyBounded(
                                    input,
                                    it,
                                    maximumBytes,
                                    "frp asset archive exceeded its download limit",
                                    guard,
                                )
                            }
                        }
                    }
                }
                guard.throwIfExpired()
                FileChannel.open(destination, StandardOpenOption.WRITE).use { it.force(true) }
                guard.throwIfExpired()
                return
            } catch (failure: InteropFailure) {
                throw failure
            } catch (_: Exception) {
                deadlineGuard?.throwIfExpired()
                throw InteropFailure("frp asset download failed")
            } finally {
                try {
                    deadlineGuard?.close()
                } finally {
                    connection.disconnect()
                }
            }
        }
        throw InteropFailure("frp asset download failed")
    }

    private fun verifyVersion(
        executable: Path,
        workingDirectory: Path,
        redactor: InteropRedactor,
        label: String,
    ) {
        val process = ManagedInteropProcess.start(
            label = label,
            executable = executable,
            arguments = listOf("--version"),
            workingDirectory = workingDirectory,
            redactor = redactor,
        )
        try {
            if (!process.waitForExit(VERSION_TIMEOUT_MILLIS)) {
                process.stop()
                throw InteropFailure("$label exceeded its deadline")
            }
            if (process.exitCode() != 0 || !VERSION_PATTERN.containsMatchIn(process.capturedText())) {
                throw InteropFailure("$label did not report the pinned frp version")
            }
        } finally {
            process.stop()
        }
    }

    private fun makeExecutable(path: Path) {
        try {
            try {
                Files.setPosixFilePermissions(
                    path,
                    setOf(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE,
                    ),
                )
            } catch (_: UnsupportedOperationException) {
                // Windows executable access follows the file extension and inherited ACL.
            }
            path.toFile().setExecutable(true, true)
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(path) ||
                !Files.isExecutable(path)
            ) {
                throw InteropFailure("provisioned frp binary is not executable")
            }
        } catch (failure: InteropFailure) {
            throw failure
        } catch (_: Exception) {
            throw InteropFailure("provisioned frp binary could not be made executable")
        }
    }

    private fun atomicReplace(source: Path, target: Path) {
        try {
            Files.move(
                source,
                target,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING,
            )
        } catch (_: Exception) {
            throw InteropFailure("provisioned frp file could not be atomically installed")
        }
    }

    private fun acquireProvisionLock(channel: FileChannel): java.nio.channels.FileLock {
        val deadline = InteropDeadline.afterMillis(PROVISION_LOCK_TIMEOUT_MILLIS)
        while (!deadline.isExpired()) {
            val lock = try {
                channel.tryLock()
            } catch (_: OverlappingFileLockException) {
                null
            } catch (_: Exception) {
                throw InteropFailure("frp provisioning lock could not be acquired")
            }
            if (lock != null) return lock
            sleepBounded(minOf(PROVISION_LOCK_POLL_MILLIS, deadline.remainingMillis()))
        }
        throw InteropFailure("frp provisioning lock exceeded its deadline")
    }

    private fun boundedTimeoutMillis(deadline: InteropDeadline, maximumMillis: Int): Int {
        val remaining = deadline.remainingMillis()
        if (remaining <= 0L) throw InteropFailure("frp asset download exceeded its deadline")
        return minOf(maximumMillis.toLong(), remaining).coerceAtLeast(1L).toInt()
    }

    private companion object {
        const val HTTP_CONNECT_TIMEOUT_MILLIS = 15_000
        const val HTTP_READ_TIMEOUT_MILLIS = 15_000
        const val DOWNLOAD_TOTAL_TIMEOUT_MILLIS = 120_000L
        const val MAXIMUM_HTTP_REDIRECTS = 5
        const val VERSION_TIMEOUT_MILLIS = 10_000L
        const val PROVISION_LOCK_TIMEOUT_MILLIS = 120_000L
        const val PROVISION_LOCK_POLL_MILLIS = 50L
        const val COPY_BUFFER_BYTES = 64 * 1024
        val HTTP_REDIRECT_CODES = setOf(301, 302, 303, 307, 308)
        val VERSION_PATTERN = Regex("(?m)(^|\\s)0\\.69\\.1(\\s|$)")
    }
}

internal fun verifyArchiveSha256(path: Path, expectedSha256: String, maximumBytes: Long) {
    if (!expectedSha256.matches(Regex("[0-9a-f]{64}"))) {
        throw InteropFailure("pinned frp archive hash is invalid")
    }
    try {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(path) ||
            Files.size(path) > maximumBytes
        ) {
            throw InteropFailure("frp archive failed bounded hash verification")
        }
        val digest = MessageDigest.getInstance("SHA-256")
        Files.newInputStream(path).use { input ->
            val buffer = ByteArray(64 * 1024)
            var total = 0L
            while (true) {
                val count = input.read(buffer)
                if (count < 0) break
                total += count
                if (total > maximumBytes) {
                    throw InteropFailure("frp archive failed bounded hash verification")
                }
                digest.update(buffer, 0, count)
            }
            buffer.fill(0)
        }
        val actual = digest.digest().joinToString("") { byte -> "%02x".format(byte) }
        if (actual != expectedSha256) {
            throw InteropFailure("frp archive SHA-256 did not match the pinned release asset")
        }
    } catch (failure: InteropFailure) {
        throw failure
    } catch (_: Exception) {
        throw InteropFailure("frp archive failed bounded hash verification")
    }
}

internal fun extractPinnedExecutables(
    archive: Path,
    kind: FrpArchiveKind,
    executableSuffix: String,
    outputDirectory: Path,
    limits: FrpArchiveLimits = FrpArchiveLimits(),
): FrpReleaseBinaries = try {
    if (!Files.isRegularFile(archive, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(archive) ||
        Files.size(archive) > limits.maximumArchiveBytes
    ) {
        throw InteropFailure("frp archive is not a bounded regular file")
    }
    createPrivateDirectory(outputDirectory)
    val extracted = when (kind) {
        FrpArchiveKind.ZIP -> extractZip(archive, executableSuffix, outputDirectory, limits)
        FrpArchiveKind.TAR_GZ -> extractTarGz(archive, executableSuffix, outputDirectory, limits)
    }
    val frpc = extracted["frpc$executableSuffix"]
        ?: throw InteropFailure("frp archive did not contain frpc")
    val frps = extracted["frps$executableSuffix"]
        ?: throw InteropFailure("frp archive did not contain frps")
    FrpReleaseBinaries(frpc, frps)
} catch (failure: InteropFailure) {
    throw failure
} catch (_: Exception) {
    throw InteropFailure("frp archive extraction failed")
}

private fun extractZip(
    archive: Path,
    executableSuffix: String,
    outputDirectory: Path,
    limits: FrpArchiveLimits,
): Map<String, Path> {
    val centralEntries = inspectZipCentralDirectory(archive, limits)
    val extracted = LinkedHashMap<String, Path>()
    var expandedBytes = 0L
    var entryIndex = 0
    Files.newInputStream(archive).use { fileInput ->
        ZipInputStream(BufferedInputStream(fileInput), ZIP_LEGACY_CHARSET).use { zip ->
            while (true) {
                val entry = zip.nextEntry ?: break
                if (entryIndex >= centralEntries.size) {
                    throw InteropFailure("zip local entries did not match the central directory")
                }
                val central = centralEntries[entryIndex++]
                val normalized = normalizeArchivePath(entry.name, limits.maximumNameBytes)
                if (normalized != central.normalizedName || entry.isDirectory != central.directory) {
                    throw InteropFailure("zip local entries did not match the central directory")
                }
                if (entry.method != java.util.zip.ZipEntry.STORED && entry.method != java.util.zip.ZipEntry.DEFLATED) {
                    throw InteropFailure("zip entry used an unsupported compression method")
                }
                val targetName = executableTargetName(normalized, central.directory, executableSuffix)
                if (targetName != null && extracted.containsKey(targetName)) {
                    throw InteropFailure("frp archive contained a duplicate executable")
                }
                val target = targetName?.let(outputDirectory::resolve)
                val output = target?.let {
                    Files.newOutputStream(it, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
                }
                output.use { destination ->
                    val copied = copyZipEntryBounded(zip, destination, limits.maximumEntryBytes) { count ->
                        expandedBytes += count
                        if (expandedBytes > limits.maximumExpandedArchiveBytes) {
                            throw InteropFailure("zip archive exceeded its expanded-size limit")
                        }
                    }
                    if (central.uncompressedSize >= 0L && copied != central.uncompressedSize) {
                        throw InteropFailure("zip entry size did not match its central metadata")
                    }
                }
                if (targetName != null) extracted[targetName] = checkNotNull(target)
                zip.closeEntry()
            }
        }
    }
    if (entryIndex != centralEntries.size) {
        throw InteropFailure("zip local entries did not match the central directory")
    }
    return extracted
}

private fun inspectZipCentralDirectory(archive: Path, limits: FrpArchiveLimits): List<ZipCentralEntry> {
    FileChannel.open(archive, StandardOpenOption.READ).use { channel ->
        val size = channel.size()
        if (size < ZIP_END_MINIMUM_BYTES || size > limits.maximumArchiveBytes) {
            throw InteropFailure("zip archive size was invalid")
        }
        val tailSize = minOf(size, (ZIP_END_MINIMUM_BYTES + ZIP_MAXIMUM_COMMENT_BYTES).toLong()).toInt()
        val tail = ByteBuffer.allocate(tailSize).order(ByteOrder.LITTLE_ENDIAN)
        channel.position(size - tailSize)
        readFully(channel, tail)
        val tailBytes = tail.array()
        var endOffset = -1
        for (candidate in tailBytes.size - ZIP_END_MINIMUM_BYTES downTo 0) {
            if (littleEndianInt(tailBytes, candidate) == ZIP_END_SIGNATURE) {
                val commentLength = littleEndianUShort(tailBytes, candidate + 20)
                if (candidate + ZIP_END_MINIMUM_BYTES + commentLength == tailBytes.size) {
                    endOffset = candidate
                    break
                }
            }
        }
        if (endOffset < 0) throw InteropFailure("zip end record was invalid")
        val diskNumber = littleEndianUShort(tailBytes, endOffset + 4)
        val centralDisk = littleEndianUShort(tailBytes, endOffset + 6)
        val diskEntries = littleEndianUShort(tailBytes, endOffset + 8)
        val totalEntries = littleEndianUShort(tailBytes, endOffset + 10)
        val centralSize = littleEndianUInt(tailBytes, endOffset + 12)
        val centralOffset = littleEndianUInt(tailBytes, endOffset + 16)
        if (diskNumber != 0 || centralDisk != 0 || diskEntries != totalEntries ||
            totalEntries > limits.maximumEntries || centralSize == 0xffffffffL || centralOffset == 0xffffffffL ||
            centralOffset > size || centralSize > size - centralOffset
        ) {
            throw InteropFailure("zip central directory was unsupported")
        }

        channel.position(centralOffset)
        val entries = ArrayList<ZipCentralEntry>(totalEntries)
        val names = HashSet<String>()
        var consumed = 0L
        repeat(totalEntries) {
            val fixed = ByteBuffer.allocate(ZIP_CENTRAL_FIXED_BYTES).order(ByteOrder.LITTLE_ENDIAN)
            readFully(channel, fixed)
            consumed += ZIP_CENTRAL_FIXED_BYTES
            val bytes = fixed.array()
            if (littleEndianInt(bytes, 0) != ZIP_CENTRAL_SIGNATURE) {
                throw InteropFailure("zip central directory entry was invalid")
            }
            val versionMadeBy = littleEndianUShort(bytes, 4)
            val flags = littleEndianUShort(bytes, 8)
            val method = littleEndianUShort(bytes, 10)
            val compressedSize = littleEndianUInt(bytes, 20)
            val uncompressedSize = littleEndianUInt(bytes, 24)
            val nameLength = littleEndianUShort(bytes, 28)
            val extraLength = littleEndianUShort(bytes, 30)
            val commentLength = littleEndianUShort(bytes, 32)
            val diskStart = littleEndianUShort(bytes, 34)
            val externalAttributes = littleEndianUInt(bytes, 38)
            if (flags and ZIP_ENCRYPTED_FLAG != 0 || diskStart != 0 ||
                compressedSize == 0xffffffffL || uncompressedSize == 0xffffffffL ||
                nameLength !in 1..limits.maximumNameBytes || commentLength > limits.maximumNameBytes ||
                method != java.util.zip.ZipEntry.STORED && method != java.util.zip.ZipEntry.DEFLATED ||
                uncompressedSize > limits.maximumEntryBytes
            ) {
                throw InteropFailure("zip central directory entry was unsupported")
            }
            val variableLength = nameLength.toLong() + extraLength + commentLength
            if (consumed > centralSize - variableLength) {
                throw InteropFailure("zip central directory entry exceeded its bounds")
            }
            val nameBytes = ByteArray(nameLength)
            readFully(channel, ByteBuffer.wrap(nameBytes))
            skipFully(channel, extraLength.toLong() + commentLength)
            consumed += variableLength
            val charset = if (flags and ZIP_UTF8_FLAG != 0) StandardCharsets.UTF_8 else ZIP_LEGACY_CHARSET
            val rawName = decodeStrict(nameBytes, charset)
            val normalized = normalizeArchivePath(rawName, limits.maximumNameBytes)
            if (!names.add(normalized)) throw InteropFailure("zip archive contained a duplicate entry")
            val unixMode = ((externalAttributes ushr 16) and 0xffffL).toInt()
            val unixType = unixMode and UNIX_FILE_TYPE_MASK
            val madeBySystem = versionMadeBy ushr 8
            if (madeBySystem == ZIP_UNIX_HOST && unixType == UNIX_SYMLINK_TYPE) {
                throw InteropFailure("zip archive contained a symbolic link")
            }
            if (madeBySystem == ZIP_UNIX_HOST && unixType != 0 &&
                unixType != UNIX_REGULAR_TYPE && unixType != UNIX_DIRECTORY_TYPE
            ) {
                throw InteropFailure("zip archive contained an unsupported file type")
            }
            val directory = rawName.endsWith('/') || rawName.endsWith('\\') || unixType == UNIX_DIRECTORY_TYPE
            entries += ZipCentralEntry(normalized, directory, uncompressedSize)
        }
        if (consumed != centralSize) throw InteropFailure("zip central directory size was invalid")
        return entries
    }
}

private fun extractTarGz(
    archive: Path,
    executableSuffix: String,
    outputDirectory: Path,
    limits: FrpArchiveLimits,
): Map<String, Path> {
    val extracted = LinkedHashMap<String, Path>()
    val names = HashSet<String>()
    var entryCount = 0
    Files.newInputStream(archive).use { fileInput ->
        val gzip = try {
            GZIPInputStream(BufferedInputStream(fileInput), 64 * 1024)
        } catch (_: Exception) {
            throw InteropFailure("tar.gz header was invalid")
        }
        gzip.use { decompressed ->
            val input = CountingLimitInputStream(decompressed, limits.maximumExpandedArchiveBytes)
            var zeroBlocks = 0
            while (true) {
                val header = ByteArray(TAR_BLOCK_BYTES)
                val headerRead = readBlock(input, header)
                if (headerRead == 0) throw InteropFailure("tar archive ended before its terminator")
                if (header.all { it == 0.toByte() }) {
                    zeroBlocks += 1
                    if (zeroBlocks == 2) {
                        ensureRemainingTarPaddingIsZero(input)
                        break
                    }
                    continue
                }
                if (zeroBlocks != 0) throw InteropFailure("tar archive terminator was malformed")
                validateTarChecksum(header)
                entryCount += 1
                if (entryCount > limits.maximumEntries) throw InteropFailure("tar archive had too many entries")
                val name = tarEntryName(header, limits.maximumNameBytes)
                val normalized = normalizeArchivePath(name, limits.maximumNameBytes)
                if (!names.add(normalized)) throw InteropFailure("tar archive contained a duplicate entry")
                val size = parseTarOctal(header, 124, 12)
                if (size > limits.maximumEntryBytes) throw InteropFailure("tar entry exceeded its size limit")
                val type = header[156].toInt() and 0xff
                val directory = type == TAR_DIRECTORY_TYPE
                if (type != TAR_REGULAR_TYPE && type != TAR_OLD_REGULAR_TYPE && !directory) {
                    throw InteropFailure("tar archive contained a link or unsupported file type")
                }
                if (directory && size != 0L) throw InteropFailure("tar directory had an invalid size")
                val targetName = executableTargetName(normalized, directory, executableSuffix)
                if (targetName != null && extracted.containsKey(targetName)) {
                    throw InteropFailure("frp archive contained a duplicate executable")
                }
                val target = targetName?.let(outputDirectory::resolve)
                val output = target?.let {
                    Files.newOutputStream(it, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
                }
                output.use { destination ->
                    copyExact(input, destination, size)
                }
                if (targetName != null) extracted[targetName] = checkNotNull(target)
                val padding = (TAR_BLOCK_BYTES - (size % TAR_BLOCK_BYTES)).toInt() % TAR_BLOCK_BYTES
                skipInputFully(input, padding.toLong())
            }
        }
    }
    return extracted
}

private fun validateDownloadUri(uri: URI) {
    if (!uri.scheme.equals("https", ignoreCase = true) || uri.host.isNullOrBlank() || uri.userInfo != null ||
        uri.fragment != null || uri.host.lowercase(Locale.ROOT) !in ALLOWED_DOWNLOAD_HOSTS ||
        uri.toASCIIString().length > MAXIMUM_DOWNLOAD_URI_CHARS
    ) {
        throw InteropFailure("frp asset download URI was invalid")
    }
}

private fun copyBounded(
    input: InputStream,
    output: OutputStream,
    maximumBytes: Long,
    limitMessage: String,
    deadlineGuard: DownloadDeadlineGuard,
): Long {
    val buffer = ByteArray(64 * 1024)
    var total = 0L
    try {
        while (true) {
            deadlineGuard.throwIfExpired()
            val count = input.read(buffer)
            deadlineGuard.throwIfExpired()
            if (count < 0) break
            total += count
            if (total > maximumBytes) throw InteropFailure(limitMessage)
            output.write(buffer, 0, count)
            deadlineGuard.throwIfExpired()
        }
        return total
    } finally {
        buffer.fill(0)
    }
}

internal fun <T> runFrpDownloadWithHardDeadline(timeoutMillis: Long, block: () -> T): T {
    if (timeoutMillis <= 0L) throw InteropFailure("frp asset download deadline was invalid")
    val task = FutureTask(block)
    Thread(task, "frp-interop-download-worker").apply {
        isDaemon = true
        start()
    }
    return try {
        task.get(timeoutMillis, TimeUnit.MILLISECONDS)
    } catch (_: TimeoutException) {
        task.cancel(true)
        throw InteropFailure("frp asset download exceeded its deadline")
    } catch (_: CancellationException) {
        throw InteropFailure("frp asset download exceeded its deadline")
    } catch (_: InterruptedException) {
        task.cancel(true)
        Thread.currentThread().interrupt()
        throw InteropFailure("frp asset download wait was interrupted")
    } catch (failure: ExecutionException) {
        when (val cause = failure.cause) {
            is Error -> throw cause
            is InteropFailure -> throw cause
            else -> throw InteropFailure("frp asset download failed")
        }
    }
}

private class DownloadDeadlineGuard(
    private val connection: HttpURLConnection,
    private val deadline: InteropDeadline,
) : AutoCloseable {
    private val resolved = AtomicBoolean(false)
    private val timedOut = AtomicBoolean(false)
    private val watchdog = Thread(
        {
            try {
                while (!resolved.get() && !deadline.isExpired()) {
                    Thread.sleep(deadline.remainingMillis().coerceAtLeast(1L))
                }
                if (resolved.compareAndSet(false, true)) {
                    timedOut.set(true)
                    try {
                        connection.disconnect()
                    } catch (_: Exception) {
                        // The timeout state is authoritative even if disconnect reports a secondary failure.
                    }
                }
            } catch (_: InterruptedException) {
                // Normal completion interrupts the watchdog.
            }
        },
        "frp-interop-download-deadline",
    ).apply {
        isDaemon = true
        start()
    }

    fun throwIfExpired() {
        if (timedOut.get() || deadline.isExpired()) {
            throw InteropFailure("frp asset download exceeded its deadline")
        }
    }

    override fun close() {
        resolved.compareAndSet(false, true)
        watchdog.interrupt()
        try {
            watchdog.join(WATCHDOG_JOIN_TIMEOUT_MILLIS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            throw InteropFailure("frp asset download cleanup was interrupted")
        }
        if (watchdog.isAlive) throw InteropFailure("frp asset download watchdog did not terminate")
    }

    private companion object {
        const val WATCHDOG_JOIN_TIMEOUT_MILLIS = 1_000L
    }
}

private fun copyZipEntryBounded(
    input: ZipInputStream,
    output: OutputStream?,
    maximumBytes: Long,
    onBytes: (Long) -> Unit,
): Long {
    val buffer = ByteArray(64 * 1024)
    var total = 0L
    while (true) {
        val count = input.read(buffer)
        if (count < 0) break
        total += count
        if (total > maximumBytes) throw InteropFailure("zip entry exceeded its size limit")
        onBytes(count.toLong())
        output?.write(buffer, 0, count)
    }
    buffer.fill(0)
    return total
}

private fun copyExact(input: InputStream, output: OutputStream?, byteCount: Long) {
    val buffer = ByteArray(64 * 1024)
    var remaining = byteCount
    while (remaining > 0L) {
        val count = input.read(buffer, 0, minOf(buffer.size.toLong(), remaining).toInt())
        if (count < 0) throw InteropFailure("tar entry was truncated")
        output?.write(buffer, 0, count)
        remaining -= count
    }
    buffer.fill(0)
}

private fun executableTargetName(normalized: String, directory: Boolean, executableSuffix: String): String? {
    if (directory) return null
    val baseName = normalized.substringAfterLast('/')
    return baseName.takeIf { it == "frpc$executableSuffix" || it == "frps$executableSuffix" }
}

private fun normalizeArchivePath(raw: String, maximumNameBytes: Int): String {
    if (raw.isBlank() || raw.indexOf('\u0000') >= 0 ||
        raw.toByteArray(StandardCharsets.UTF_8).size > maximumNameBytes
    ) {
        throw InteropFailure("archive entry name was invalid")
    }
    val separatorsNormalized = raw.replace('\\', '/')
    if (separatorsNormalized.startsWith('/') || WINDOWS_ABSOLUTE_PATH.matches(separatorsNormalized)) {
        throw InteropFailure("archive entry used an absolute path")
    }
    val segments = ArrayList<String>()
    separatorsNormalized.split('/').forEach { segment ->
        when (segment) {
            "", "." -> Unit
            ".." -> throw InteropFailure("archive entry attempted path traversal")
            else -> segments += segment
        }
    }
    if (segments.isEmpty()) throw InteropFailure("archive entry name was invalid")
    return segments.joinToString("/")
}

private fun tarEntryName(header: ByteArray, maximumNameBytes: Int): String {
    val name = tarString(header, 0, 100)
    val prefix = tarString(header, 345, 155)
    val combined = if (prefix.isEmpty()) name else "$prefix/$name"
    if (combined.toByteArray(StandardCharsets.UTF_8).size > maximumNameBytes) {
        throw InteropFailure("tar entry name exceeded its limit")
    }
    return combined
}

private fun tarString(header: ByteArray, offset: Int, length: Int): String {
    var end = offset
    val maximum = offset + length
    while (end < maximum && header[end] != 0.toByte()) end += 1
    return decodeStrict(header.copyOfRange(offset, end), StandardCharsets.UTF_8)
}

private fun parseTarOctal(header: ByteArray, offset: Int, length: Int): Long {
    if (header[offset].toInt() and 0x80 != 0) throw InteropFailure("tar used an unsupported numeric encoding")
    var index = offset
    val end = offset + length
    while (index < end && (header[index] == 0.toByte() || header[index] == ' '.code.toByte())) index += 1
    var value = 0L
    while (index < end) {
        val byte = header[index]
        if (byte == 0.toByte() || byte == ' '.code.toByte()) break
        if (byte < '0'.code.toByte() || byte > '7'.code.toByte()) {
            throw InteropFailure("tar numeric field was invalid")
        }
        val digit = byte - '0'.code.toByte()
        if (value > (Long.MAX_VALUE - digit) / 8L) throw InteropFailure("tar numeric field overflowed")
        value = value * 8L + digit
        index += 1
    }
    return value
}

private fun validateTarChecksum(header: ByteArray) {
    val expected = parseTarOctal(header, 148, 8)
    var actual = 0L
    header.forEachIndexed { index, byte ->
        actual += if (index in 148..155) ' '.code else byte.toInt() and 0xff
    }
    if (actual != expected) throw InteropFailure("tar header checksum was invalid")
}

private fun readBlock(input: InputStream, destination: ByteArray): Int {
    var offset = 0
    while (offset < destination.size) {
        val count = input.read(destination, offset, destination.size - offset)
        if (count < 0) {
            if (offset == 0) return 0
            throw InteropFailure("tar block was truncated")
        }
        offset += count
    }
    return offset
}

private fun ensureRemainingTarPaddingIsZero(input: InputStream) {
    val buffer = ByteArray(8 * 1024)
    while (true) {
        val count = input.read(buffer)
        if (count < 0) break
        for (index in 0 until count) {
            if (buffer[index] != 0.toByte()) throw InteropFailure("tar archive had data after its terminator")
        }
    }
}

private fun skipInputFully(input: InputStream, count: Long) {
    var remaining = count
    val buffer = ByteArray(512)
    while (remaining > 0L) {
        val read = input.read(buffer, 0, minOf(buffer.size.toLong(), remaining).toInt())
        if (read < 0) throw InteropFailure("archive padding was truncated")
        remaining -= read
    }
}

private fun decodeStrict(bytes: ByteArray, charset: Charset): String = try {
    charset.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes))
        .toString()
} catch (_: CharacterCodingException) {
    throw InteropFailure("archive entry name encoding was invalid")
}

private fun readFully(channel: FileChannel, destination: ByteBuffer) {
    while (destination.hasRemaining()) {
        if (channel.read(destination) < 0) throw InteropFailure("zip central directory was truncated")
    }
}

private fun skipFully(channel: FileChannel, count: Long) {
    val next = channel.position() + count
    if (next < channel.position() || next > channel.size()) {
        throw InteropFailure("zip central directory was truncated")
    }
    channel.position(next)
}

private fun littleEndianInt(bytes: ByteArray, offset: Int): Int =
    ByteBuffer.wrap(bytes, offset, Int.SIZE_BYTES).order(ByteOrder.LITTLE_ENDIAN).int

private fun littleEndianUShort(bytes: ByteArray, offset: Int): Int =
    ByteBuffer.wrap(bytes, offset, Short.SIZE_BYTES).order(ByteOrder.LITTLE_ENDIAN).short.toInt() and 0xffff

private fun littleEndianUInt(bytes: ByteArray, offset: Int): Long =
    littleEndianInt(bytes, offset).toLong() and 0xffffffffL

private class CountingLimitInputStream(
    private val delegate: InputStream,
    private val maximumBytes: Long,
) : InputStream() {
    private var count = 0L

    override fun read(): Int {
        val value = delegate.read()
        if (value >= 0) addCount(1)
        return value
    }

    override fun read(destination: ByteArray, offset: Int, length: Int): Int {
        val read = delegate.read(destination, offset, length)
        if (read > 0) addCount(read.toLong())
        return read
    }

    private fun addCount(value: Long) {
        count += value
        if (count > maximumBytes) throw InteropFailure("archive exceeded its expanded-size limit")
    }
}

private data class ZipCentralEntry(
    val normalizedName: String,
    val directory: Boolean,
    val uncompressedSize: Long,
)

private data class FrpAssetPin(
    val platform: String,
    val fileName: String,
    val sha256: String,
    val archiveKind: FrpArchiveKind,
    val executableSuffix: String,
) {
    val downloadUri: URI
        get() = URI.create(FrpReleasePins.BASE_URL + fileName)

    override fun toString(): String =
        "FrpAssetPin(platform=$platform, version=${FrpReleasePins.VERSION}, archiveKind=$archiveKind)"
}

private object FrpReleasePins {
    const val VERSION = "0.69.1"
    const val BASE_URL = "https://github.com/fatedier/frp/releases/download/v0.69.1/"

    private val assets = listOf(
        FrpAssetPin(
            "linux_amd64",
            "frp_0.69.1_linux_amd64.tar.gz",
            "7be257b72dbbc60bcb3e0e25a5afd1dfac7b63f897084864d3c956dd3d5674e1",
            FrpArchiveKind.TAR_GZ,
            "",
        ),
        FrpAssetPin(
            "linux_arm64",
            "frp_0.69.1_linux_arm64.tar.gz",
            "bbc0c75e896af3f292fb46ba09c844a04fa9b5ea3530c039c7af20637f836355",
            FrpArchiveKind.TAR_GZ,
            "",
        ),
        FrpAssetPin(
            "windows_amd64",
            "frp_0.69.1_windows_amd64.zip",
            "829ac915f8655d4d4e021b8db61b46c3445205ed80d32b04cda7fa89d87c46e0",
            FrpArchiveKind.ZIP,
            ".exe",
        ),
        FrpAssetPin(
            "windows_arm64",
            "frp_0.69.1_windows_arm64.zip",
            "9b88e6eefc5d9ea2a1d5869026287e269e3d1486ac5bb08f7b4d2b26bdd6166d",
            FrpArchiveKind.ZIP,
            ".exe",
        ),
        FrpAssetPin(
            "darwin_amd64",
            "frp_0.69.1_darwin_amd64.tar.gz",
            "2bc26d02100ef333f2712149ea5997dc530dc0eefac64f4be41cb0f49d032f40",
            FrpArchiveKind.TAR_GZ,
            "",
        ),
        FrpAssetPin(
            "darwin_arm64",
            "frp_0.69.1_darwin_arm64.tar.gz",
            "310012e2f1dcf3cdde2605d29b95340b686c94d1680a23711d58efeffc02f64e",
            FrpArchiveKind.TAR_GZ,
            "",
        ),
    )

    fun current(): FrpAssetPin {
        val platform = normalizedFrpPlatform(
            System.getProperty("os.name").orEmpty(),
            System.getProperty("os.arch").orEmpty(),
        )
        return assets.singleOrNull { it.platform == platform }
            ?: throw InteropFailure("current platform has no pinned frp release asset")
    }
}

private val WINDOWS_ABSOLUTE_PATH = Regex("^[A-Za-z]:($|/).*")
private val ZIP_LEGACY_CHARSET: Charset = Charset.forName("IBM437")
private const val MAXIMUM_DOWNLOAD_URI_CHARS = 8 * 1024
private val ALLOWED_DOWNLOAD_HOSTS = setOf(
    "github.com",
    "objects.githubusercontent.com",
    "release-assets.githubusercontent.com",
)
private const val ZIP_END_SIGNATURE = 0x06054b50
private const val ZIP_CENTRAL_SIGNATURE = 0x02014b50
private const val ZIP_END_MINIMUM_BYTES = 22
private const val ZIP_MAXIMUM_COMMENT_BYTES = 65_535
private const val ZIP_CENTRAL_FIXED_BYTES = 46
private const val ZIP_ENCRYPTED_FLAG = 1
private const val ZIP_UTF8_FLAG = 1 shl 11
private const val ZIP_UNIX_HOST = 3
private const val UNIX_FILE_TYPE_MASK = 0xf000
private const val UNIX_REGULAR_TYPE = 0x8000
private const val UNIX_DIRECTORY_TYPE = 0x4000
private const val UNIX_SYMLINK_TYPE = 0xa000
private const val TAR_BLOCK_BYTES = 512
private const val TAR_OLD_REGULAR_TYPE = 0
private const val TAR_REGULAR_TYPE = '0'.code
private const val TAR_DIRECTORY_TYPE = '5'.code
