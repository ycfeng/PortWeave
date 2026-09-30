package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import com.sun.security.auth.module.NTSystem
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file.DirectoryStream
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.AclEntry
import java.nio.file.attribute.AclEntryPermission
import java.nio.file.attribute.AclEntryType
import java.nio.file.attribute.AclFileAttributeView
import java.nio.file.attribute.GroupPrincipal
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.attribute.UserPrincipal
import java.util.ArrayDeque
import java.util.EnumSet
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

private const val REDACTED = "<redacted>"

internal class InteropFailure(message: String) : Exception(message)

internal enum class InteropHostOs(val platformName: String) {
    DARWIN("darwin"),
    WINDOWS("windows"),
    LINUX("linux"),
}

internal fun classifyInteropHostOs(osName: String): InteropHostOs {
    val normalized = osName.lowercase(Locale.ROOT)
    return when {
        normalized.contains("mac") || normalized.contains("darwin") -> InteropHostOs.DARWIN
        normalized.contains("win") -> InteropHostOs.WINDOWS
        normalized.contains("linux") -> InteropHostOs.LINUX
        else -> throw InteropFailure("current operating system has no pinned frp release asset")
    }
}

internal fun normalizedFrpPlatform(osName: String, archName: String): String {
    val architecture = when (archName.lowercase(Locale.ROOT)) {
        "amd64", "x86_64" -> "amd64"
        "arm64", "aarch64" -> "arm64"
        else -> throw InteropFailure("current architecture has no pinned frp release asset")
    }
    return "${classifyInteropHostOs(osName).platformName}_$architecture"
}

internal class InteropRedactor private constructor(
    rawSensitiveValues: Collection<String>,
) {
    private val sensitiveValues = rawSensitiveValues
        .asSequence()
        .filter(String::isNotBlank)
        .flatMap { value -> sequenceOf(value, value.replace('\\', '/')) }
        .distinct()
        .sortedByDescending(String::length)
        .toList()

    constructor(vararg sensitiveValues: String) : this(sensitiveValues.asList())

    fun including(vararg values: String): InteropRedactor =
        InteropRedactor(sensitiveValues + values)

    fun redact(raw: String): String {
        if (raw.length > MAXIMUM_REDACTION_INPUT_CHARS) return REDACTED
        var value = raw
        sensitiveValues.forEach { sensitive ->
            value = value.replace(sensitive, REDACTED)
        }
        value = URL_PATTERN.replace(value, REDACTED)
        value = LOOPBACK_ENDPOINT_PATTERN.replace(value, REDACTED)
        value = KEY_VALUE_PATTERN.replace(value) { match ->
            "${match.groupValues[1]}${match.groupValues[2]}$REDACTED"
        }
        return if (value.length <= MAXIMUM_REDACTION_OUTPUT_CHARS) {
            value
        } else {
            value.take(MAXIMUM_REDACTION_OUTPUT_CHARS) + " [truncated]"
        }
    }

    override fun toString(): String = "InteropRedactor(valueCount=${sensitiveValues.size})"

    private companion object {
        const val MAXIMUM_REDACTION_INPUT_CHARS = 16 * 1024
        const val MAXIMUM_REDACTION_OUTPUT_CHARS = 4 * 1024
        val URL_PATTERN = Regex("(?i)https?://[^\\s\\\"']+")
        val LOOPBACK_ENDPOINT_PATTERN = Regex(
            "(?i)(?<![a-z0-9_.-])(?:127(?:\\.\\d{1,3}){3}|localhost|\\[?::1]?):(\\d{1,5})(?!\\d)",
        )
        val KEY_VALUE_PATTERN = Regex(
            "(?i)\\b(token|secret|password|authorization|cookie)(\\s*[:=]\\s*)([^\\s,;]+)",
        )
    }
}

internal class ManagedInteropProcess private constructor(
    private val label: String,
    private val process: Process,
    private val log: BoundedProcessLog,
) : AutoCloseable {
    val isAlive: Boolean
        get() = process.isAlive

    fun requireAlive(stage: String) {
        log.throwIfFatal()
        if (!process.isAlive) {
            log.awaitDrain()
            throw InteropFailure("$stage: $label exited unexpectedly${log.summarySuffix()}")
        }
    }

    fun waitForExit(timeoutMillis: Long): Boolean {
        val exited = process.waitFor(timeoutMillis, TimeUnit.MILLISECONDS)
        if (exited) log.awaitDrain()
        return exited
    }

    fun exitCode(): Int = process.exitValue()

    fun capturedText(): String {
        log.throwIfFatal()
        return log.text()
    }

    fun outputRevision(): Long {
        log.throwIfFatal()
        return log.revision()
    }

    fun awaitOutput(pattern: Regex, timeoutMillis: Long, stage: String) {
        awaitOutputAfter(pattern, afterRevision = 0L, timeoutMillis, stage)
    }

    fun awaitOutputAfter(pattern: Regex, afterRevision: Long, timeoutMillis: Long, stage: String) {
        if (afterRevision < 0L) throw InteropFailure("process output revision was invalid")
        val deadline = InteropDeadline.afterMillis(timeoutMillis)
        while (!deadline.isExpired()) {
            requireAlive(stage)
            if (log.containsMatchAfter(pattern, afterRevision)) return
            sleepBounded(minOf(PROCESS_OUTPUT_POLL_MILLIS, deadline.remainingMillis()))
        }
        throw InteropFailure("$stage did not report readiness within its deadline")
    }

    fun diagnosticSummary(): String = capturedText()
        .takeIf(String::isNotBlank)
        ?.takeLast(MAXIMUM_DIAGNOSTIC_CHARS)
        ?.let { "$label log tail:\n$it" }
        .orEmpty()

    fun stop() {
        if (process.isAlive) {
            destroyProcessTree(force = false)
            if (!process.waitFor(PROCESS_DESTROY_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                destroyProcessTree(force = true)
                if (!process.waitFor(PROCESS_FORCE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    throw InteropFailure("$label did not terminate within its cleanup deadline")
                }
            }
        }
        log.awaitDrain()
    }

    override fun close() = stop()

    override fun toString(): String = "ManagedInteropProcess(label=$label, alive=${process.isAlive})"

    private fun destroyProcessTree(force: Boolean) {
        destroyDescendantsReflectively(force)
        if (force) process.destroyForcibly() else process.destroy()
    }

    private fun destroyDescendantsReflectively(force: Boolean) {
        try {
            val handleType = Class.forName("java.lang.ProcessHandle")
            val rootHandle = Process::class.java.getMethod("toHandle").invoke(process)
            @Suppress("UNCHECKED_CAST")
            val descendants = handleType.getMethod("descendants").invoke(rootHandle) as java.util.stream.Stream<Any>
            val handles = descendants.use { stream -> stream.iterator().asSequence().toList().asReversed() }
            val destroy = handleType.getMethod(if (force) "destroyForcibly" else "destroy")
            handles.forEach { handle ->
                try {
                    destroy.invoke(handle)
                } catch (_: Exception) {
                    // The direct process remains the cleanup authority.
                }
            }
        } catch (_: Exception) {
            // Host JDKs expose ProcessHandle; direct cleanup remains safe if reflection is unavailable.
        }
    }

    companion object {
        fun start(
            label: String,
            executable: Path,
            arguments: List<String>,
            workingDirectory: Path,
            redactor: InteropRedactor,
        ): ManagedInteropProcess {
            val command = ArrayList<String>(arguments.size + 1).apply {
                add(executable.toAbsolutePath().normalize().toString())
                addAll(arguments)
            }
            val builder = ProcessBuilder(command)
                .directory(workingDirectory.toFile())
                .redirectErrorStream(true)
            minimizeEnvironment(builder, workingDirectory)
            val process = try {
                builder.start()
            } catch (_: Exception) {
                throw InteropFailure("$label could not be started")
            }
            return ManagedInteropProcess(
                label = label,
                process = process,
                log = BoundedProcessLog(process.inputStream, redactor, label),
            )
        }

        private fun minimizeEnvironment(builder: ProcessBuilder, workingDirectory: Path) {
            val inherited = HashMap(builder.environment())
            val environment = builder.environment()
            environment.clear()
            val hostOs = classifyInteropHostOs(System.getProperty("os.name").orEmpty())
            if (hostOs == InteropHostOs.WINDOWS) {
                listOf("SystemRoot", "WINDIR", "ComSpec", "PATHEXT").forEach { key ->
                    inherited.entries.firstOrNull { it.key.equals(key, ignoreCase = true) }
                        ?.let { environment[it.key] = it.value }
                }
                environment["TEMP"] = workingDirectory.toString()
                environment["TMP"] = workingDirectory.toString()
            } else {
                environment["HOME"] = workingDirectory.toString()
                environment["TMPDIR"] = workingDirectory.toString()
                environment["PATH"] = "/usr/bin:/bin"
                environment["LANG"] = "C"
                environment["LC_ALL"] = "C"
            }
            environment["NO_COLOR"] = "1"
        }

        private const val PROCESS_DESTROY_TIMEOUT_MILLIS = 5_000L
        private const val PROCESS_FORCE_TIMEOUT_MILLIS = 5_000L
        private const val PROCESS_OUTPUT_POLL_MILLIS = 25L
        private const val MAXIMUM_DIAGNOSTIC_CHARS = 4 * 1024
    }
}

internal class BoundedProcessLog(
    input: InputStream,
    private val redactor: InteropRedactor,
    label: String,
) {
    private val lock = Any()
    private val lines = ArrayDeque<ProcessLogLine>()
    private var discardedLines = 0
    private var latestRevision = 0L
    private val fatalFailure = AtomicReference<Throwable?>(null)
    private val thread = Thread(
        { drain(input) },
        "frp-interop-$label-log",
    ).apply {
        isDaemon = true
        uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, failure ->
            fatalFailure.compareAndSet(null, failure)
        }
        start()
    }

    fun awaitDrain() {
        try {
            thread.join(LOG_JOIN_TIMEOUT_MILLIS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            throw InteropFailure("process log cleanup was interrupted")
        }
        if (thread.isAlive) throw InteropFailure("process log did not drain within its cleanup deadline")
        throwIfFatal()
    }

    fun throwIfFatal() {
        fatalFailure.get()?.let { throw it }
    }

    fun text(): String = synchronized(lock) {
        buildString {
            lines.forEachIndexed { index, line ->
                if (index > 0) append('\n')
                append(line.value)
            }
            if (discardedLines > 0) {
                if (isNotEmpty()) append('\n')
                append("[older process log lines discarded]")
            }
        }
    }

    fun revision(): Long = synchronized(lock) { latestRevision }

    fun containsMatchAfter(pattern: Regex, afterRevision: Long): Boolean = synchronized(lock) {
        lines.any { line -> line.revision > afterRevision && pattern.containsMatchIn(line.value) }
    }

    fun summarySuffix(): String = text().takeIf(String::isNotBlank)?.let { "\n$it" }.orEmpty()

    private fun drain(input: InputStream) {
        try {
            input.use { stream ->
                val buffer = ByteArray(PROCESS_READ_BUFFER_BYTES)
                val line = ByteArrayOutputStream(MAXIMUM_STORED_LINE_BYTES)
                var lineTruncated = false
                while (true) {
                    val count = stream.read(buffer)
                    if (count < 0) break
                    for (index in 0 until count) {
                        val byte = buffer[index]
                        if (byte == '\n'.code.toByte()) {
                            storeLine(line, lineTruncated)
                            line.reset()
                            lineTruncated = false
                        } else if (!lineTruncated) {
                            if (line.size() < MAXIMUM_STORED_LINE_BYTES) {
                                line.write(byte.toInt())
                            } else {
                                lineTruncated = true
                            }
                        }
                    }
                }
                if (line.size() > 0 || lineTruncated) storeLine(line, lineTruncated)
                buffer.fill(0)
            }
        } catch (_: Exception) {
            // Process failure remains authoritative; log collection is diagnostic only.
        }
    }

    private fun storeLine(bytes: ByteArrayOutputStream, truncated: Boolean) {
        val line = if (truncated) {
            REDACTED
        } else {
            redactor.redact(bytes.toString(StandardCharsets.UTF_8).removeSuffix("\r"))
        }
        synchronized(lock) {
            latestRevision += 1L
            if (lines.size == MAXIMUM_STORED_LINES) {
                lines.removeFirst()
                discardedLines += 1
            }
            lines.addLast(ProcessLogLine(latestRevision, line))
        }
    }

    private data class ProcessLogLine(
        val revision: Long,
        val value: String,
    )

    private companion object {
        const val PROCESS_READ_BUFFER_BYTES = 8 * 1024
        const val MAXIMUM_STORED_LINE_BYTES = 16 * 1024
        const val MAXIMUM_STORED_LINES = 64
        const val LOG_JOIN_TIMEOUT_MILLIS = 2_000L
    }
}

internal class InteropDeadline private constructor(
    private val deadlineNanos: Long,
) {
    fun isExpired(): Boolean = System.nanoTime() - deadlineNanos >= 0L

    fun remainingMillis(): Long =
        TimeUnit.NANOSECONDS.toMillis((deadlineNanos - System.nanoTime()).coerceAtLeast(0L))

    companion object {
        fun afterMillis(timeoutMillis: Long): InteropDeadline {
            require(timeoutMillis > 0L)
            val durationNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
            return InteropDeadline(System.nanoTime() + durationNanos)
        }
    }
}

internal fun createPrivateDirectory(path: Path) {
    Files.createDirectories(path)
    if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(path)) {
        throw InteropFailure("temporary directory was not a regular directory")
    }
    setPosixPermissionsIfSupported(
        path,
        setOf(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE,
        ),
    )
    restrictPrivateAclIfRequired(path)
    verifyPrivatePathPermissions(path, directory = true)
}

internal fun writePrivateFile(path: Path, value: String) {
    val bytes = value.toByteArray(StandardCharsets.UTF_8)
    try {
        writePrivateFile(path, bytes)
    } finally {
        bytes.fill(0)
    }
}

internal fun writePrivateFile(path: Path, value: ByteArray) {
    val options = setOf(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    val channel = if (Files.getFileStore(path.parent).supportsFileAttributeView("posix")) {
        Files.newByteChannel(
            path,
            options,
            PosixFilePermissions.asFileAttribute(setOf(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)),
        )
    } else {
        Files.newByteChannel(path, options)
    }
    channel.use { fileChannel ->
        Channels.newOutputStream(fileChannel).use { output -> output.write(value) }
    }
    setPosixPermissionsIfSupported(
        path,
        setOf(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE),
    )
    restrictPrivateAclIfRequired(path)
    verifyPrivatePathPermissions(path, directory = false)
}

internal fun deleteTreeBestEffort(root: Path?) {
    if (root == null || !Files.exists(root, LinkOption.NOFOLLOW_LINKS)) return
    val budget = intArrayOf(MAXIMUM_DELETE_ENTRIES)
    try {
        deleteTree(root, depth = 0, budget = budget)
    } catch (_: Exception) {
        // Cleanup is best effort after every logical owner has been stopped.
    }
}

internal fun deleteTreeChecked(root: Path?) {
    deleteTreeBestEffort(root)
    if (root != null && Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
        throw InteropFailure("sensitive temporary workspace cleanup failed")
    }
}

private fun deleteTree(path: Path, depth: Int, budget: IntArray) {
    if (depth > MAXIMUM_DELETE_DEPTH || budget[0]-- <= 0) return
    if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(path)) {
        Files.newDirectoryStream(path).use { entries: DirectoryStream<Path> ->
            entries.forEach { child -> deleteTree(child, depth + 1, budget) }
        }
    }
    Files.deleteIfExists(path)
}

private fun setPosixPermissionsIfSupported(path: Path, permissions: Set<PosixFilePermission>) {
    try {
        Files.setPosixFilePermissions(path, permissions)
    } catch (_: UnsupportedOperationException) {
        // Windows permissions are inherited from the private temporary directory.
    } catch (_: Exception) {
        throw InteropFailure("temporary file permissions could not be restricted")
    }
}

private fun restrictPrivateAclIfRequired(path: Path) {
    if (Files.getFileStore(path).supportsFileAttributeView("posix")) return
    val view = Files.getFileAttributeView(path, AclFileAttributeView::class.java, LinkOption.NOFOLLOW_LINKS)
        ?: throw InteropFailure("temporary file permissions could not be restricted")
    val owner = currentProcessUserPrincipal(path.fileSystem)
    try {
        view.owner = owner
        view.acl = listOf(
            AclEntry.newBuilder()
                .setType(AclEntryType.ALLOW)
                .setPrincipal(owner)
                .setPermissions(EnumSet.allOf(AclEntryPermission::class.java))
                .build(),
        )
    } catch (_: Exception) {
        throw InteropFailure("temporary file permissions could not be restricted")
    }
}

private fun verifyPrivatePathPermissions(path: Path, directory: Boolean) {
    try {
        if (Files.getFileStore(path).supportsFileAttributeView("posix")) {
            val permissions = Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS)
            val expected = if (directory) {
                setOf(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE,
                )
            } else {
                setOf(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)
            }
            if (permissions != expected) throw InteropFailure("temporary file permissions were not private")
            return
        }
        val view = Files.getFileAttributeView(path, AclFileAttributeView::class.java, LinkOption.NOFOLLOW_LINKS)
            ?: throw InteropFailure("temporary file permissions could not be verified")
        val owner = currentProcessUserPrincipal(path.fileSystem)
        if (view.owner != owner || owner is GroupPrincipal) {
            throw InteropFailure("temporary file permissions were not private")
        }
        val ownerAllow = view.acl.filter { entry ->
            entry.type() == AclEntryType.ALLOW && entry.principal() == owner
        }
        val required = if (directory) {
            setOf(AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA, AclEntryPermission.EXECUTE)
        } else {
            setOf(AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA)
        }
        if (ownerAllow.none { it.permissions().containsAll(required) } || view.acl.any { entry ->
                entry.type() == AclEntryType.ALLOW && entry.principal() != owner
            }
        ) {
            throw InteropFailure("temporary file permissions were not private")
        }
    } catch (failure: InteropFailure) {
        throw failure
    } catch (_: Exception) {
        throw InteropFailure("temporary file permissions could not be verified")
    }
}

internal fun currentProcessUserPrincipal(fileSystem: FileSystem): UserPrincipal {
    val accountName = if (System.getProperty("os.name").orEmpty().lowercase(Locale.ROOT).contains("win")) {
        val identity = NTSystem()
        val user = identity.name?.takeIf(String::isNotBlank)
            ?: throw InteropFailure("current process identity was unavailable")
        identity.domain?.takeIf(String::isNotBlank)?.let { domain -> "$domain\\$user" } ?: user
    } else {
        System.getProperty("user.name")?.takeIf(String::isNotBlank)
            ?: throw InteropFailure("current process identity was unavailable")
    }
    val principal = try {
        fileSystem.userPrincipalLookupService.lookupPrincipalByName(accountName)
    } catch (_: Exception) {
        throw InteropFailure("current process identity was unavailable")
    }
    if (principal is GroupPrincipal) {
        throw InteropFailure("current process identity was invalid")
    }
    return principal
}

internal fun sleepBounded(millis: Long) {
    if (millis <= 0L) return
    try {
        Thread.sleep(millis)
    } catch (_: InterruptedException) {
        Thread.currentThread().interrupt()
        throw InteropFailure("interoperability operation was interrupted")
    }
}

private const val MAXIMUM_DELETE_DEPTH = 16
private const val MAXIMUM_DELETE_ENTRIES = 4_096
