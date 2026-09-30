package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class FrpReleaseProvisionerTest {
    @Test
    fun downloadHardDeadlineReturnsEvenWhenWorkerIgnoresInterruption() {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val startedAt = System.nanoTime()

        val failure = try {
            captureInteropFailure {
                runFrpDownloadWithHardDeadline(100L) {
                    started.countDown()
                    while (true) {
                        try {
                            if (release.await(10L, TimeUnit.MILLISECONDS)) return@runFrpDownloadWithHardDeadline
                        } catch (_: InterruptedException) {
                            // Simulate a blocking operation that does not honor interruption.
                        }
                    }
                }
            }
        } finally {
            release.countDown()
        }

        assertTrue(started.await(1L, TimeUnit.SECONDS))
        assertEquals("frp asset download exceeded its deadline", failure.message)
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt) < 2_000L)
    }

    @Test
    fun platformNormalizationDistinguishesDarwinFromWindows() {
        assertEquals("darwin_amd64", normalizedFrpPlatform("Darwin", "x86_64"))
        assertEquals("darwin_arm64", normalizedFrpPlatform("Mac OS X", "aarch64"))
        assertEquals("windows_amd64", normalizedFrpPlatform("Windows 11", "amd64"))
        assertEquals("windows_arm64", normalizedFrpPlatform("Windows Server", "arm64"))
        assertEquals("linux_amd64", normalizedFrpPlatform("Linux", "x86_64"))
        assertEquals("linux_arm64", normalizedFrpPlatform("GNU/Linux", "aarch64"))
        captureInteropFailure { normalizedFrpPlatform("FreeBSD", "amd64") }
        captureInteropFailure { normalizedFrpPlatform("Linux", "riscv64") }
    }

    @Test
    fun hashMismatchIsRejectedWithoutLeakingThePrivatePath() = withTemporaryDirectory { root ->
        val archive = root.resolve("asset.bin")
        Files.write(archive, byteArrayOf(1, 2, 3), StandardOpenOption.CREATE_NEW)

        val failure = captureInteropFailure {
            verifyArchiveSha256(archive, "0".repeat(64), 64L)
        }

        assertTrue(failure.message.orEmpty().contains("SHA-256"))
        assertFalse(failure.message.orEmpty().contains(root.toString()))
    }

    @Test
    fun zipExtractionAcceptsOnlyThePinnedExecutables() = withTemporaryDirectory { root ->
        val archive = root.resolve("asset.zip")
        writeZip(
            archive,
            listOf(
                ZipFixture("frp_0.69.1_windows_amd64/frpc.exe", byteArrayOf(1, 2, 3)),
                ZipFixture("frp_0.69.1_windows_amd64/frps.exe", byteArrayOf(4, 5, 6)),
                ZipFixture("frp_0.69.1_windows_amd64/README.md", byteArrayOf(7, 8)),
            ),
        )
        val output = root.resolve("output")

        val binaries = extractPinnedExecutables(archive, FrpArchiveKind.ZIP, ".exe", output)

        assertArrayEquals(byteArrayOf(1, 2, 3), Files.newInputStream(binaries.frpc).use { it.readNBytes(4) })
        assertArrayEquals(byteArrayOf(4, 5, 6), Files.newInputStream(binaries.frps).use { it.readNBytes(4) })
        assertFalse(Files.exists(output.resolve("README.md")))
    }

    @Test
    fun zipTraversalDuplicateSymlinkAndEntryLimitAreRejected() = withTemporaryDirectory { root ->
        val traversal = root.resolve("traversal.zip")
        writeZip(traversal, listOf(ZipFixture("../frpc.exe", byteArrayOf(1))))
        captureInteropFailure {
            extractPinnedExecutables(traversal, FrpArchiveKind.ZIP, ".exe", root.resolve("traversal-out"))
        }

        val duplicate = root.resolve("duplicate.zip")
        writeZip(
            duplicate,
            listOf(
                ZipFixture("root/frpc.exe", byteArrayOf(1)),
                ZipFixture("root/./frpc.exe", byteArrayOf(2)),
            ),
        )
        captureInteropFailure {
            extractPinnedExecutables(duplicate, FrpArchiveKind.ZIP, ".exe", root.resolve("duplicate-out"))
        }

        val symlink = root.resolve("symlink.zip")
        writeZip(
            symlink,
            listOf(
                ZipFixture("root/frpc.exe", byteArrayOf(1)),
                ZipFixture("root/frps.exe", byteArrayOf(2)),
            ),
        )
        markFirstZipCentralEntryAsSymlink(symlink)
        captureInteropFailure {
            extractPinnedExecutables(symlink, FrpArchiveKind.ZIP, ".exe", root.resolve("symlink-out"))
        }

        val oversized = root.resolve("oversized.zip")
        writeZip(
            oversized,
            listOf(
                ZipFixture("root/frpc.exe", ByteArray(5)),
                ZipFixture("root/frps.exe", byteArrayOf(1)),
            ),
        )
        captureInteropFailure {
            extractPinnedExecutables(
                oversized,
                FrpArchiveKind.ZIP,
                ".exe",
                root.resolve("oversized-out"),
                tinyLimits(),
            )
        }
    }

    @Test
    fun tarGzExtractionAcceptsOnlyThePinnedExecutables() = withTemporaryDirectory { root ->
        val archive = root.resolve("asset.tar.gz")
        writeTarGz(
            archive,
            listOf(
                TarFixture("frp_0.69.1_linux_amd64", '5', byteArrayOf()),
                TarFixture("frp_0.69.1_linux_amd64/frpc", '0', byteArrayOf(1, 2, 3)),
                TarFixture("frp_0.69.1_linux_amd64/frps", '0', byteArrayOf(4, 5, 6)),
                TarFixture("frp_0.69.1_linux_amd64/README.md", '0', byteArrayOf(7, 8)),
            ),
        )
        val output = root.resolve("output")

        val binaries = extractPinnedExecutables(archive, FrpArchiveKind.TAR_GZ, "", output)

        assertArrayEquals(byteArrayOf(1, 2, 3), Files.newInputStream(binaries.frpc).use { it.readNBytes(4) })
        assertArrayEquals(byteArrayOf(4, 5, 6), Files.newInputStream(binaries.frps).use { it.readNBytes(4) })
        assertFalse(Files.exists(output.resolve("README.md")))
    }

    @Test
    fun tarTraversalDuplicateSymlinkAndEntryLimitAreRejected() = withTemporaryDirectory { root ->
        val traversal = root.resolve("traversal.tar.gz")
        writeTarGz(traversal, listOf(TarFixture("../frpc", '0', byteArrayOf(1))))
        captureInteropFailure {
            extractPinnedExecutables(traversal, FrpArchiveKind.TAR_GZ, "", root.resolve("traversal-out"))
        }

        val duplicate = root.resolve("duplicate.tar.gz")
        writeTarGz(
            duplicate,
            listOf(
                TarFixture("root/frpc", '0', byteArrayOf(1)),
                TarFixture("root/./frpc", '0', byteArrayOf(2)),
            ),
        )
        captureInteropFailure {
            extractPinnedExecutables(duplicate, FrpArchiveKind.TAR_GZ, "", root.resolve("duplicate-out"))
        }

        val symlink = root.resolve("symlink.tar.gz")
        writeTarGz(
            symlink,
            listOf(
                TarFixture("root/frpc", '2', byteArrayOf()),
                TarFixture("root/frps", '0', byteArrayOf(1)),
            ),
        )
        captureInteropFailure {
            extractPinnedExecutables(symlink, FrpArchiveKind.TAR_GZ, "", root.resolve("symlink-out"))
        }

        val oversized = root.resolve("oversized.tar.gz")
        writeTarGz(
            oversized,
            listOf(
                TarFixture("root/frpc", '0', ByteArray(5)),
                TarFixture("root/frps", '0', byteArrayOf(1)),
            ),
        )
        captureInteropFailure {
            extractPinnedExecutables(
                oversized,
                FrpArchiveKind.TAR_GZ,
                "",
                root.resolve("oversized-out"),
                tinyLimits(),
            )
        }
    }

    private fun tinyLimits(): FrpArchiveLimits = FrpArchiveLimits(
        maximumArchiveBytes = 1024L * 1024L,
        maximumExpandedArchiveBytes = 1024L * 1024L,
        maximumEntryBytes = 4L,
        maximumEntries = 32,
        maximumNameBytes = 256,
    )

    private fun writeZip(path: Path, entries: List<ZipFixture>) {
        ZipOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)).use { zip ->
            entries.forEach { fixture ->
                zip.putNextEntry(ZipEntry(fixture.name))
                zip.write(fixture.body)
                zip.closeEntry()
            }
        }
    }

    private fun markFirstZipCentralEntryAsSymlink(path: Path) {
        val size = Files.size(path)
        require(size in 1 until 1024L * 1024L)
        val bytes = Files.newInputStream(path).use { it.readNBytes(size.toInt() + 1) }
        require(bytes.size == size.toInt())
        val signature = byteArrayOf(0x50, 0x4b, 0x01, 0x02)
        val offset = bytes.indices.firstOrNull { index ->
            index <= bytes.size - signature.size && signature.indices.all { bytes[index + it] == signature[it] }
        } ?: error("missing central directory")
        bytes[offset + 4] = 20
        bytes[offset + 5] = 3
        val mode = 0xa1ffL shl 16
        ByteBuffer.wrap(bytes, offset + 38, 4).order(ByteOrder.LITTLE_ENDIAN).putInt(mode.toInt())
        Files.write(path, bytes, StandardOpenOption.TRUNCATE_EXISTING)
    }

    private fun writeTarGz(path: Path, entries: List<TarFixture>) {
        GZIPOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)).use { gzip ->
            entries.forEach { fixture ->
                val header = ByteArray(TAR_BLOCK_BYTES)
                writeTarString(header, 0, 100, fixture.name)
                writeTarOctal(header, 100, 8, if (fixture.type == '5') 493L else 448L)
                writeTarOctal(header, 108, 8, 0L)
                writeTarOctal(header, 116, 8, 0L)
                writeTarOctal(header, 124, 12, fixture.body.size.toLong())
                writeTarOctal(header, 136, 12, 0L)
                for (index in 148..155) header[index] = ' '.code.toByte()
                header[156] = fixture.type.code.toByte()
                writeTarString(header, 257, 6, "ustar")
                writeTarString(header, 263, 2, "00")
                val checksum = header.sumOf { it.toInt() and 0xff }.toLong()
                val checksumText = checksum.toString(8).padStart(6, '0')
                checksumText.toByteArray(StandardCharsets.US_ASCII).copyInto(header, 148)
                header[154] = 0
                header[155] = ' '.code.toByte()
                gzip.write(header)
                gzip.write(fixture.body)
                val padding = (TAR_BLOCK_BYTES - fixture.body.size % TAR_BLOCK_BYTES) % TAR_BLOCK_BYTES
                if (padding > 0) gzip.write(ByteArray(padding))
            }
            gzip.write(ByteArray(TAR_BLOCK_BYTES * 2))
        }
    }

    private fun writeTarString(header: ByteArray, offset: Int, length: Int, value: String) {
        val bytes = value.toByteArray(StandardCharsets.UTF_8)
        require(bytes.size <= length)
        bytes.copyInto(header, offset)
    }

    private fun writeTarOctal(header: ByteArray, offset: Int, length: Int, value: Long) {
        val bytes = value.toString(8).padStart(length - 1, '0').toByteArray(StandardCharsets.US_ASCII)
        require(bytes.size == length - 1)
        bytes.copyInto(header, offset)
        header[offset + length - 1] = 0
    }

    private fun captureInteropFailure(block: () -> Unit): InteropFailure = try {
        block()
        throw AssertionError("expected InteropFailure")
    } catch (failure: InteropFailure) {
        failure
    }

    private fun withTemporaryDirectory(block: (Path) -> Unit) {
        val root = Files.createTempDirectory("frp-provisioner-test-")
        try {
            block(root)
        } finally {
            deleteTreeBestEffort(root)
        }
    }

    private data class ZipFixture(val name: String, val body: ByteArray)

    private data class TarFixture(val name: String, val type: Char, val body: ByteArray)

    private companion object {
        const val TAR_BLOCK_BYTES = 512
    }
}
