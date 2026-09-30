package dev.portweave.app.diagnostics

import android.content.Context
import android.os.Build
import dev.portweave.app.service.RuntimeSnapshot
import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.data.transfer.ConfigTransferService
import dev.portweave.core.diagnostics.EventRepository
import dev.portweave.core.diagnostics.TunnelEvent
import java.io.OutputStream
import java.util.UUID
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

class DiagnosticBundleExporter(
    private val context: Context,
    private val repository: ConfigRepository,
    private val events: EventRepository,
    private val transfer: ConfigTransferService,
    private val workCounters: () -> Map<String, Long> = { emptyMap() },
) {
    suspend fun write(output: OutputStream, runtime: Map<UUID, RuntimeSnapshot>) {
        val configs = repository.snapshot()
        val recent = events.recent(5_000).asReversed()
        writeDiagnosticArchive(
            output,
            linkedMapOf(
                "app-version.json" to appVersionJson(),
                "runtime-summary.json" to runtimeJson(runtime),
                "sanitized-configs.json" to transfer.export(configs, privacyMode = true),
                "recent-events.jsonl" to recent.joinToString("\n", transform = ::eventJson),
                "system-summary.json" to systemJson(),
                "licenses.txt" to context.assets.open("licenses.txt").bufferedReader().use { it.readText() },
            ),
        )
    }

    private fun appVersionJson(): String {
        val info = context.packageManager.getPackageInfo(context.packageName, 0)
        return pretty(buildJsonObject {
            put("package", context.packageName)
            put("versionName", info.versionName.orEmpty())
            put("versionCode", if (Build.VERSION.SDK_INT >= 28) info.longVersionCode else @Suppress("DEPRECATION") info.versionCode.toLong())
        })
    }

    private fun runtimeJson(runtime: Map<UUID, RuntimeSnapshot>): String = pretty(buildJsonArray {
        runtime.entries.sortedBy { it.key.toString() }.forEach { (id, value) ->
            add(buildJsonObject {
                put("configId", id.toString())
                put("generation", value.generation)
                put("state", value.state.javaClass.simpleName)
                put("startedAtEpochMillis", value.startedAtEpochMillis?.let(::JsonPrimitive) ?: JsonNull)
                put("lastReadyAtEpochMillis", value.lastReadyAtEpochMillis?.let(::JsonPrimitive) ?: JsonNull)
                put("reconnectCount", value.reconnectCount)
                put("networkChanges", value.networkChanges)
                put("lastFailureCode", value.lastFailureCode?.let(::JsonPrimitive) ?: JsonNull)
                put("capabilities", buildJsonObject {
                    put("byteCounters", value.capabilities.byteCounters)
                    put("activeConnectionCount", value.capabilities.activeConnectionCount)
                    put("handshakeLatency", value.capabilities.handshakeLatency)
                })
                put("metrics", buildJsonObject {
                    put("bytesUploaded", value.metrics.bytesUploaded?.let(::JsonPrimitive) ?: JsonNull)
                    put("bytesDownloaded", value.metrics.bytesDownloaded?.let(::JsonPrimitive) ?: JsonNull)
                    put("activeConnections", value.metrics.activeConnections?.let(::JsonPrimitive) ?: JsonNull)
                    put("peakConnections", value.metrics.peakConnections?.let(::JsonPrimitive) ?: JsonNull)
                    put("handshakeLatencyMillis", value.metrics.handshakeLatencyMillis?.let(::JsonPrimitive) ?: JsonNull)
                })
            })
        }
    })

    private fun eventJson(event: TunnelEvent): String = Json.encodeToString(
        kotlinx.serialization.json.JsonObject.serializer(),
        buildJsonObject {
            put("configId", event.configId.toString())
            put("generation", event.generation)
            put("level", event.level.name)
            put("code", event.code)
            put("messageKey", event.messageKey)
            put("elapsedRealtimeMillis", event.elapsedRealtimeMillis)
            put("occurredAtEpochMillis", event.occurredAtEpochMillis)
            put("traceId", event.traceId)
        },
    )

    private fun systemJson(): String = pretty(buildJsonObject {
        put("runtimeWorkCounters", buildJsonObject { workCounters().toSortedMap().forEach { (name, count) -> put(name, count) } })
        put("sdkInt", Build.VERSION.SDK_INT)
        put("osRelease", Build.VERSION.RELEASE)
        put("supportedAbis", buildJsonArray { Build.SUPPORTED_ABIS.forEach { add(JsonPrimitive(it)) } })
    })

    private fun pretty(value: kotlinx.serialization.json.JsonElement): String =
        Json { prettyPrint = true }.encodeToString(kotlinx.serialization.json.JsonElement.serializer(), value)
}

internal fun writeDiagnosticArchive(output: OutputStream, entries: Map<String, String>) {
    require(entries.keys == RequiredDiagnosticEntries) { "诊断包条目不完整" }
    ZipOutputStream(output.buffered()).use { zip -> entries.forEach(zip::textEntry) }
}

internal val RequiredDiagnosticEntries = linkedSetOf(
    "app-version.json",
    "runtime-summary.json",
    "sanitized-configs.json",
    "recent-events.jsonl",
    "system-summary.json",
    "licenses.txt",
)

private fun ZipOutputStream.textEntry(name: String, value: String) {
    putNextEntry(ZipEntry(name).apply { time = 0L })
    write(value.toByteArray(Charsets.UTF_8))
    closeEntry()
}
