package dev.portweave.app.diagnostics

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.ZipInputStream
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test

class DiagnosticBundleExporterTest {
    @Test
    fun archiveContainsExactlyTheDocumentedEntries() {
        val output = ByteArrayOutputStream()
        writeDiagnosticArchive(output, RequiredDiagnosticEntries.associateWith { "sanitized-$it" })

        val contents = linkedMapOf<String, String>()
        ZipInputStream(ByteArrayInputStream(output.toByteArray())).use { zip ->
            while (true) {
                val entry = zip.nextEntry ?: break
                contents[entry.name] = zip.readBytes().decodeToString()
            }
        }

        assertEquals(RequiredDiagnosticEntries, contents.keys)
        assertFalse(output.toByteArray().decodeToString().contains("password"))
    }
}
