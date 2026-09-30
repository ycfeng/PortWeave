package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlinx.serialization.Serializable

@Serializable
internal data class AppE2eRepositoryIdentity(
    val candidateCommitSha: String,
    val treeSha: String,
    val worktreeClean: Boolean,
)

@Serializable
internal data class AppE2eArtifactIdentity(
    val localSha256: String,
    val installedSha256: String,
    val byteIdentical: Boolean,
)

internal fun readAppE2eRepositoryIdentity(repositoryRoot: Path): AppE2eRepositoryIdentity {
    val root = repositoryRoot.toAbsolutePath().normalize()
    if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(root) ||
        !Files.exists(root.resolve(".git"), LinkOption.NOFOLLOW_LINKS)
    ) {
        throw InteropFailure("App E2E repository root was invalid")
    }
    return parseAppE2eRepositoryIdentity(
        headOutput = runBoundedGit(root, "rev-parse", "--verify", "HEAD"),
        treeOutput = runBoundedGit(root, "rev-parse", "--verify", "HEAD^{tree}"),
        statusOutput = runBoundedGit(root, "status", "--porcelain=v1", "--untracked-files=all"),
    )
}

internal fun parseAppE2eRepositoryIdentity(
    headOutput: String,
    treeOutput: String,
    statusOutput: String,
): AppE2eRepositoryIdentity {
    val commit = parseSingleGitObjectId(headOutput)
    val tree = parseSingleGitObjectId(treeOutput)
    if (statusOutput.isNotEmpty()) {
        throw InteropFailure("App E2E exact-candidate evidence requires a clean worktree")
    }
    return AppE2eRepositoryIdentity(
        candidateCommitSha = commit,
        treeSha = tree,
        worktreeClean = true,
    )
}

internal fun sha256AppE2eArtifact(path: Path): String {
    val artifact = path.toAbsolutePath().normalize()
    if (!Files.isRegularFile(artifact, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(artifact)) {
        throw InteropFailure("App E2E APK identity source was invalid")
    }
    val expectedSize = try {
        Files.size(artifact)
    } catch (_: Exception) {
        throw InteropFailure("App E2E APK identity source was unreadable")
    }
    if (expectedSize !in 1..MAXIMUM_APP_E2E_APK_BYTES) {
        throw InteropFailure("App E2E APK identity source exceeded its bound")
    }

    val digest = MessageDigest.getInstance("SHA-256")
    var total = 0L
    val buffer = ByteArray(APP_E2E_HASH_BUFFER_BYTES)
    try {
        Files.newInputStream(artifact, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS).use { input ->
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                total += read
                if (total > expectedSize || total > MAXIMUM_APP_E2E_APK_BYTES) {
                    throw InteropFailure("App E2E APK identity source changed while hashing")
                }
                digest.update(buffer, 0, read)
            }
        }
    } catch (failure: InteropFailure) {
        throw failure
    } catch (_: Exception) {
        throw InteropFailure("App E2E APK identity source could not be hashed")
    } finally {
        buffer.fill(0)
    }
    if (total != expectedSize || Files.size(artifact) != expectedSize) {
        throw InteropFailure("App E2E APK identity source changed while hashing")
    }
    return digest.digest().joinToString("") { byte -> "%02x".format(byte) }
}

internal fun parseInstalledAppE2eApkPath(output: String): String {
    val lines = output.lineSequence().map(String::trim).filter(String::isNotEmpty).toList()
    if (lines.size != 1 || !lines.single().startsWith(APP_E2E_PACKAGE_PATH_PREFIX)) {
        throw InteropFailure("App E2E installed package path was ambiguous")
    }
    return lines.single().removePrefix(APP_E2E_PACKAGE_PATH_PREFIX)
        .takeIf(APP_E2E_INSTALLED_APK_PATH::matches)
        ?: throw InteropFailure("App E2E installed package path was invalid")
}

internal fun parseInstalledAppE2eSha256(output: String): String {
    val line = output.lineSequence().map(String::trim).filter(String::isNotEmpty).singleOrNull()
        ?: throw InteropFailure("App E2E installed APK digest output was ambiguous")
    return APP_E2E_SHA256SUM.matchEntire(line)?.groupValues?.get(1)
        ?: throw InteropFailure("App E2E installed APK digest output was invalid")
}

internal fun buildAppE2eArtifactIdentity(
    localSha256: String,
    installedSha256: String,
): AppE2eArtifactIdentity {
    if (!APP_E2E_SHA256.matches(localSha256) || !APP_E2E_SHA256.matches(installedSha256) ||
        !MessageDigest.isEqual(
            localSha256.toByteArray(StandardCharsets.US_ASCII),
            installedSha256.toByteArray(StandardCharsets.US_ASCII),
        )
    ) {
        throw InteropFailure("App E2E local and installed APK identities differed")
    }
    return AppE2eArtifactIdentity(
        localSha256 = localSha256,
        installedSha256 = installedSha256,
        byteIdentical = true,
    )
}

private fun parseSingleGitObjectId(output: String): String {
    val value = output.lineSequence().map(String::trim).filter(String::isNotEmpty).singleOrNull()
    return value?.takeIf(APP_E2E_GIT_OBJECT_ID::matches)
        ?: throw InteropFailure("App E2E repository identity was invalid")
}

private fun runBoundedGit(repositoryRoot: Path, vararg arguments: String): String {
    val process = try {
        ProcessBuilder(listOf("git", "-C", repositoryRoot.toString()) + arguments)
            .redirectErrorStream(true)
            .start()
    } catch (_: Exception) {
        throw InteropFailure("App E2E repository identity command could not start")
    }
    val output = BoundedGitOutput(process.inputStream)
    try {
        val completed = try {
            process.waitFor(APP_E2E_GIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            stopGitProcess(process)
            throw InteropFailure("App E2E repository identity command was interrupted")
        }
        if (!completed) {
            stopGitProcess(process)
            output.awaitAfterStop()
            throw InteropFailure("App E2E repository identity command exceeded its deadline")
        }
        output.await()
        if (output.overflowed || process.exitValue() != 0) {
            throw InteropFailure("App E2E repository identity command failed")
        }
        return output.consumeText()
    } finally {
        if (process.isAlive) stopGitProcess(process)
        output.clear()
    }
}

private fun stopGitProcess(process: Process) {
    if (!process.isAlive) return
    process.destroy()
    try {
        if (!process.waitFor(APP_E2E_GIT_STOP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            process.destroyForcibly()
            process.waitFor(APP_E2E_GIT_STOP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        }
    } catch (_: InterruptedException) {
        Thread.currentThread().interrupt()
        process.destroyForcibly()
    }
}

private class BoundedGitOutput(input: InputStream) {
    private val bytes = ByteArray(MAXIMUM_APP_E2E_GIT_OUTPUT_BYTES)
    private val count = AtomicInteger(0)
    private val overflow = AtomicBoolean(false)
    private val failure = AtomicReference<Throwable?>(null)
    private val thread = Thread(
        { drain(input) },
        "frpc-app-e2e-git-output",
    ).apply {
        isDaemon = true
        start()
    }

    val overflowed: Boolean
        get() = overflow.get()

    fun await() {
        try {
            thread.join(APP_E2E_GIT_OUTPUT_JOIN_TIMEOUT_MILLIS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            throw InteropFailure("App E2E repository identity output was interrupted")
        }
        if (thread.isAlive) throw InteropFailure("App E2E repository identity output did not drain")
        failure.get()?.let { throwable ->
            if (throwable is Error) throw throwable
            throw InteropFailure("App E2E repository identity output failed")
        }
    }

    fun awaitAfterStop() {
        try {
            thread.join(APP_E2E_GIT_OUTPUT_JOIN_TIMEOUT_MILLIS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    fun consumeText(): String {
        val length = count.get().coerceIn(0, bytes.size)
        return try {
            String(bytes, 0, length, StandardCharsets.UTF_8)
        } finally {
            clear()
        }
    }

    fun clear() = bytes.fill(0)

    private fun drain(input: InputStream) {
        try {
            input.use { stream ->
                val buffer = ByteArray(APP_E2E_GIT_READ_BUFFER_BYTES)
                try {
                    while (true) {
                        val read = stream.read(buffer)
                        if (read < 0) break
                        val offset = count.get()
                        val accepted = minOf(read, (bytes.size - offset).coerceAtLeast(0))
                        if (accepted > 0) {
                            buffer.copyInto(bytes, destinationOffset = offset, endIndex = accepted)
                            count.addAndGet(accepted)
                        }
                        if (accepted != read) overflow.set(true)
                    }
                } finally {
                    buffer.fill(0)
                }
            }
        } catch (throwable: Throwable) {
            failure.compareAndSet(null, throwable)
        }
    }
}

private val APP_E2E_GIT_OBJECT_ID = Regex("[0-9a-f]{40}")
private val APP_E2E_SHA256 = Regex("[0-9a-f]{64}")
private val APP_E2E_SHA256SUM = Regex("([0-9a-f]{64})\\s+\\S+")
private val APP_E2E_INSTALLED_APK_PATH = Regex("/[A-Za-z0-9._/+=~-]{1,4096}\\.apk")
private const val APP_E2E_PACKAGE_PATH_PREFIX = "package:"
private const val MAXIMUM_APP_E2E_APK_BYTES = 512L * 1024L * 1024L
private const val APP_E2E_HASH_BUFFER_BYTES = 64 * 1024
private const val MAXIMUM_APP_E2E_GIT_OUTPUT_BYTES = 64 * 1024
private const val APP_E2E_GIT_READ_BUFFER_BYTES = 8 * 1024
private const val APP_E2E_GIT_TIMEOUT_MILLIS = 15_000L
private const val APP_E2E_GIT_STOP_TIMEOUT_MILLIS = 2_000L
private const val APP_E2E_GIT_OUTPUT_JOIN_TIMEOUT_MILLIS = 2_000L
