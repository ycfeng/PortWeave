package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

internal data class AdbCommandResult(
    val exitCode: Int,
    val standardOutput: String,
    val standardError: String,
) {
    override fun toString(): String =
        "AdbCommandResult(exitCode=$exitCode, stdoutPresent=${standardOutput.isNotBlank()}, " +
            "stderrPresent=${standardError.isNotBlank()})"
}

internal class AdbCommandRunner(
    adbExecutable: Path,
    private val serial: String?,
    private val redactor: InteropRedactor,
) {
    private val executable = adbExecutable.toAbsolutePath().normalize().also { path ->
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(path)) {
            throw InteropFailure("configured adb executable was invalid")
        }
    }

    init {
        if (serial != null && !SAFE_SERIAL.matches(serial)) {
            throw InteropFailure("configured Android device serial was invalid")
        }
    }

    fun run(
        label: String,
        arguments: List<String>,
        timeoutMillis: Long,
        standardInput: ByteArray? = null,
        requireSuccess: Boolean = true,
    ): AdbCommandResult {
        if (!SAFE_LABEL.matches(label) || timeoutMillis <= 0L || arguments.isEmpty() ||
            arguments.size > MAXIMUM_ARGUMENT_COUNT || arguments.any(::invalidArgument)
        ) {
            throw InteropFailure("adb command configuration was invalid")
        }
        if (standardInput != null && standardInput.size > MAXIMUM_STANDARD_INPUT_BYTES) {
            throw InteropFailure("adb command standard input exceeded its limit")
        }

        val command = ArrayList<String>(arguments.size + 3).apply {
            add(executable.toString())
            serial?.let {
                add("-s")
                add(it)
            }
            addAll(arguments)
        }
        val process = try {
            ProcessBuilder(command)
                .redirectErrorStream(false)
                .start()
        } catch (_: Exception) {
            throw InteropFailure("$label could not start adb")
        }
        val stdout = BoundedAdbOutput(process.inputStream, "$label-stdout")
        val stderr = BoundedAdbOutput(process.errorStream, "$label-stderr")
        var inputFailure = false
        try {
            try {
                process.outputStream.use { output ->
                    standardInput?.let(output::write)
                    output.flush()
                }
            } catch (_: Exception) {
                inputFailure = true
            }

            val completed = try {
                process.waitFor(timeoutMillis, TimeUnit.MILLISECONDS)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                stopProcess(process)
                throw InteropFailure("$label was interrupted")
            }
            if (!completed) {
                stopProcess(process)
                stdout.awaitAfterStop()
                stderr.awaitAfterStop()
                throw InteropFailure("$label exceeded its deadline")
            }

            stdout.await()
            stderr.await()
            if (stdout.overflowed || stderr.overflowed) {
                throw InteropFailure("$label produced excessive adb output")
            }
            if (inputFailure) throw InteropFailure("$label could not send bounded adb input")

            val result = AdbCommandResult(
                exitCode = process.exitValue(),
                standardOutput = stdout.consumeText(),
                standardError = stderr.consumeText(),
            )
            if (requireSuccess && result.exitCode != 0) {
                throw InteropFailure(failureSummary(label, result))
            }
            return result
        } finally {
            if (process.isAlive) stopProcess(process)
            stdout.clear()
            stderr.clear()
        }
    }

    private fun failureSummary(label: String, result: AdbCommandResult): String {
        val detail = sequenceOf(result.standardError, result.standardOutput)
            .map(redactor::redact)
            .firstOrNull(String::isNotBlank)
            ?.take(MAXIMUM_DIAGNOSTIC_CHARS)
        return if (detail == null) {
            "$label failed with adb exit code ${result.exitCode}"
        } else {
            "$label failed with adb exit code ${result.exitCode}: $detail"
        }
    }

    private fun invalidArgument(value: String): Boolean =
        value.isEmpty() || value.length > MAXIMUM_ARGUMENT_CHARS || '\u0000' in value || '\r' in value || '\n' in value

    private fun stopProcess(process: Process) {
        if (!process.isAlive) return
        process.destroy()
        try {
            if (!process.waitFor(PROCESS_STOP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                process.destroyForcibly()
                process.waitFor(PROCESS_FORCE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            process.destroyForcibly()
        }
    }

    private class BoundedAdbOutput(input: InputStream, label: String) {
        private val bytes = ByteArray(MAXIMUM_OUTPUT_BYTES)
        private val count = java.util.concurrent.atomic.AtomicInteger(0)
        private val overflow = AtomicBoolean(false)
        private val failure = AtomicReference<Throwable?>(null)
        private val thread = Thread(
            { drain(input) },
            "frpc-android-interop-$label",
        ).apply {
            isDaemon = true
            uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, throwable ->
                failure.compareAndSet(null, throwable)
            }
            start()
        }

        val overflowed: Boolean
            get() = overflow.get()

        fun await() {
            try {
                thread.join(OUTPUT_JOIN_TIMEOUT_MILLIS)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                throw InteropFailure("adb output collection was interrupted")
            }
            if (thread.isAlive) throw InteropFailure("adb output did not drain within its deadline")
            failure.get()?.let { throwable ->
                if (throwable is Error) throw throwable
                throw InteropFailure("adb output collection failed")
            }
        }

        fun awaitAfterStop() {
            try {
                thread.join(OUTPUT_JOIN_TIMEOUT_MILLIS)
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
                    val buffer = ByteArray(OUTPUT_READ_BUFFER_BYTES)
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

    private companion object {
        val SAFE_SERIAL = Regex("[A-Za-z0-9._:-]{1,128}")
        val SAFE_LABEL = Regex("[A-Za-z0-9 _-]{1,96}")
        const val MAXIMUM_ARGUMENT_COUNT = 64
        const val MAXIMUM_ARGUMENT_CHARS = 4 * 1024
        const val MAXIMUM_STANDARD_INPUT_BYTES = 16 * 1024
        const val MAXIMUM_OUTPUT_BYTES = 128 * 1024
        const val OUTPUT_READ_BUFFER_BYTES = 8 * 1024
        const val MAXIMUM_DIAGNOSTIC_CHARS = 2 * 1024
        const val PROCESS_STOP_TIMEOUT_MILLIS = 2_000L
        const val PROCESS_FORCE_TIMEOUT_MILLIS = 2_000L
        const val OUTPUT_JOIN_TIMEOUT_MILLIS = 2_000L
    }
}
