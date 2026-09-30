package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.nio.file.DirectoryStream
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.security.SecureRandom
import java.util.Base64
import java.util.UUID
import java.util.concurrent.CancellationException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import kotlin.system.exitProcess
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

private val APP_E2E_TLS_CERTIFICATE_PIN = Regex("[0-9a-f]{64}")

@Serializable
internal data class AndroidInteropScenario(
    val kind: String,
)

internal fun parseAndroidRuntimePageSize(output: String): Int = output.trim().toIntOrNull()
    ?.takeIf { it > 0 }
    ?: throw InteropFailure("Android device page size was invalid")

internal fun resolveAndroidRuntimePageSize(
    getconfResult: AdbCommandResult,
    legacyProbe: () -> String,
): Int = when (getconfResult.exitCode) {
    0 -> parseAndroidRuntimePageSize(getconfResult.standardOutput)
    127 -> parseAndroidRuntimePageSize(legacyProbe())
    else -> throw InteropFailure(
        "Android device page size failed with adb exit code ${getconfResult.exitCode}",
    )
}

internal fun isValidAppE2eTlsCertificatePin(suite: String, pin: String?): Boolean =
    if (suite == "network") pin?.matches(APP_E2E_TLS_CERTIFICATE_PIN) == true else pin == null

@Serializable
internal data class AndroidInteropRestartMarkers(
    val deviceReadyFile: String,
    val frpsStoppedFile: String,
    val deviceDisconnectedFile: String,
    val frpsRestartedFile: String,
) {
    val fileNames: List<String>
        get() = listOf(deviceReadyFile, frpsStoppedFile, deviceDisconnectedFile, frpsRestartedFile)

    override fun toString(): String = "AndroidInteropRestartMarkers(configured=true)"
}

@Serializable
internal data class AndroidInteropInvocation(
    val schemaVersion: Int,
    val runId: String,
    val backend: String,
    val caseId: String,
    val scenario: AndroidInteropScenario,
    val serverPort: Int,
    val authToken: String,
    val stcpSecret: String,
    val bindPort: Int,
    val wireProtocol: String,
    val useEncryption: Boolean,
    val useCompression: Boolean,
    val restartMarkers: AndroidInteropRestartMarkers? = null,
) {
    override fun toString(): String =
        "AndroidInteropInvocation(caseId=$caseId, backend=$backend, scenario=${scenario.kind}, " +
            "wireProtocol=$wireProtocol, useEncryption=$useEncryption, " +
            "useCompression=$useCompression, authToken=<redacted>, stcpSecret=<redacted>, " +
            "restartMarkersPresent=${restartMarkers != null})"
}

@Serializable
internal data class AndroidInteropResult(
    val schemaVersion: Int,
    val runId: String,
    val backend: String,
    val caseId: String,
    val scenario: AndroidInteropScenario,
    val wireProtocol: String,
    val useEncryption: Boolean,
    val useCompression: Boolean,
    val status: String,
    val failedStage: String? = null,
    val failureCode: String? = null,
    val semanticOutcome: String? = null,
    val bindPort: Int = 0,
    val checks: List<String> = emptyList(),
) {
    override fun toString(): String =
        "AndroidInteropResult(caseId=$caseId, backend=$backend, status=$status, " +
            "semanticOutcomePresent=${semanticOutcome != null}, checkCount=${checks.size}, " +
            "failedStagePresent=${failedStage != null}, " +
            "failureCodePresent=${failureCode != null})"
}

@Serializable
internal data class AndroidInteropSuiteSummary(
    val schemaVersion: Int,
    val suite: String,
    val device: AndroidInteropSummaryDevice,
    val backendOrder: List<String>,
    val profiles: List<AndroidInteropProfileSummary>,
    val authorizesKotlinDefault: Boolean,
)

@Serializable
internal data class AndroidInteropSummaryDevice(
    val apiLevel: Int,
    val abi: String,
    val pageSize: Int,
)

@Serializable
internal data class AndroidInteropProfileSummary(
    val caseId: String,
    val scenario: String,
    val wireProtocol: String,
    val useEncryption: Boolean,
    val useCompression: Boolean,
    val backends: List<AndroidInteropBackendSummary>,
    val equivalent: Boolean,
)

@Serializable
internal data class AndroidInteropBackendSummary(
    val backend: String,
    val semanticOutcome: String,
    val checks: List<String>,
)

internal data class AndroidInteropCompletedProfile(
    val profile: AndroidInteropProfile,
    val goMobile: AndroidInteropResult,
    val kotlin: AndroidInteropResult,
)

@Serializable
internal data class AndroidInteropRestartMarker(
    val schemaVersion: Int,
    val runId: String,
    val caseId: String,
    val backend: String,
    val step: String,
) {
    override fun toString(): String = "AndroidInteropRestartMarker(step=$step)"
}

@Serializable
private data class AppE2eInvocation(
    val schemaVersion: Int,
    val runId: String,
    val backend: String,
    val suite: String,
    val phase: String,
    val outageMillis: Long,
    val frpsHost: String,
    val frpsPort: Int,
    val authToken: String,
    val tlsServerCertificateSha256: String?,
    val user: String,
    val serverUser: String,
    val serverName: String,
    val secretKey: String,
    val restartMarkers: AppE2eRestartMarkers,
    val workload: AppE2eWorkload,
) {
    override fun toString(): String =
        "AppE2eInvocation(schemaVersion=$schemaVersion, runId=<redacted>, backend=$backend, suite=$suite, phase=$phase, " +
            "outageMillis=$outageMillis, frpsHost=<redacted>, frpsPort=$frpsPort, " +
            "authToken=<redacted>, tlsServerCertificateSha256=" +
            "${if (tlsServerCertificateSha256 == null) "null" else "<redacted>"}, " +
            "user=<redacted>, serverUser=<redacted>, " +
            "serverName=<redacted>, secretKey=<redacted>, workload=$workload)"
}

@Serializable
private data class AppE2eRestartMarkers(
    val deviceReadyFile: String,
    val deviceDisconnectedFile: String,
    val frpsOnlyFile: String,
    val providerWindowObservedFile: String,
    val frpsRestartedFile: String,
    val wifiDisconnectedFile: String,
    val wifiRetentionObservedFile: String,
    val wifiRecoveredFile: String,
    val wifiRecoveryObservedFile: String,
) {
    val allFiles: List<String>
        get() = listOf(
            deviceReadyFile,
            deviceDisconnectedFile,
            frpsOnlyFile,
            providerWindowObservedFile,
            frpsRestartedFile,
            wifiDisconnectedFile,
            wifiRetentionObservedFile,
            wifiRecoveredFile,
            wifiRecoveryObservedFile,
        )

    override fun toString(): String = "AppE2eRestartMarkers(configured=true)"
}

@Serializable
internal data class AppE2eResult(
    val schemaVersion: Int,
    val runId: String,
    val status: String,
    val checks: List<String>,
    val backend: String? = null,
    val bindPort: Int? = null,
    val initialGeneration: Long? = null,
    val initialControlEpoch: Long? = null,
    val recoveredGeneration: Long? = null,
    val recoveredControlEpoch: Long? = null,
    val suite: String? = null,
    val topology: String? = null,
    val networkSwitchProven: Boolean? = null,
    val phase: String? = null,
    val processId: Int? = null,
    val failedStage: String? = null,
    val failureCode: String? = null,
    val runtimeInitialization: AppE2eRuntimeInitialization? = null,
    val resources: AppE2eResourceEvidence? = null,
    val workload: AppE2eWorkloadResult? = null,
) {
    override fun toString(): String =
        "AppE2eResult(schemaVersion=$schemaVersion, runId=<redacted>, status=$status, backend=$backend, " +
            "checkCount=${checks.size}, bindPort=$bindPort, initialGeneration=$initialGeneration, " +
            "initialControlEpoch=$initialControlEpoch, recoveredGeneration=$recoveredGeneration, " +
            "recoveredControlEpoch=$recoveredControlEpoch, suite=$suite, topology=$topology, " +
            "networkSwitchProven=$networkSwitchProven, phase=$phase, processPresent=${processId != null}, " +
            "failedStage=$failedStage, failureCode=$failureCode, " +
            "runtimeInitializationPresent=${runtimeInitialization != null}, resourcesPresent=${resources != null}, " +
            "workloadProfile=${workload?.profile})"
}

@Serializable
internal data class AppE2eRuntimeInitialization(
    val requestedDelayMillis: Long,
    val coldThreadCount: Int,
    val initializedThreadCount: Int,
    val initializedGoroutineCount: Int? = null,
    val initializedSessionCount: Int? = null,
    val initializedOwnerCounts: Map<String, Long> = emptyMap(),
    val initializedRuntimeCounts: Map<String, Long> = emptyMap(),
    val postSessionQuiescenceThreadCount: Int? = null,
    val postSessionQuiescenceGoroutineCount: Int? = null,
    val postSessionQuiescenceSessionCount: Int? = null,
    val postSessionQuiescenceOwnerCounts: Map<String, Long> = emptyMap(),
    val postSessionQuiescenceRuntimeCounts: Map<String, Long> = emptyMap(),
)

private val APP_E2E_RUNTIME_COUNT_KEYS = setOf(
    "/cgo/go-to-c-calls:calls",
    "/gc/cycles/forced:gc-cycles",
    "/gc/cycles/total:gc-cycles",
    "/gc/heap/live:bytes",
    "/gc/heap/objects:objects",
    "/gc/scan/stack:bytes",
    "/memory/classes/heap/stacks:bytes",
    "/memory/classes/os-stacks:bytes",
    "/sched/gomaxprocs:threads",
    "/sched/goroutines:goroutines",
    "/sched/goroutines/not-in-go:goroutines",
    "/sched/goroutines/runnable:goroutines",
    "/sched/goroutines/running:goroutines",
    "/sched/goroutines/waiting:goroutines",
    "/sched/goroutines-created:goroutines",
    "/sched/threads/total:threads",
    "app.fd.category.total",
    "app.fd.category.socket",
    "app.fd.category.pipe",
    "app.fd.category.anon_inode",
    "app.fd.category.memfd",
    "app.fd.category.device",
    "app.fd.category.file",
    "app.fd.category.other",
    "app.fd.category.unreadable",
)

internal fun isValidAppE2eRuntimeInitialization(value: AppE2eRuntimeInitialization): Boolean =
    listOf(value.initializedRuntimeCounts, value.postSessionQuiescenceRuntimeCounts).all { counts ->
        counts.all { (key, count) -> key in APP_E2E_RUNTIME_COUNT_KEYS && count >= 0L }
    }

private data class AppE2eBackendSpec(
    val backend: String,
    val targetPackage: String,
    val testPackage: String,
    val targetApkMarker: String,
    val testApkFileName: String,
)

private data class AppE2eExecution(
    val checks: List<String>,
    val initialGeneration: Long,
    val initialControlEpoch: Long,
    val recoveredGeneration: Long,
    val recoveredControlEpoch: Long,
    val processRecreated: Boolean,
    val runtimeInitializations: List<AppE2eRuntimeInitialization>,
    val resources: List<AppE2eResourceEvidence>,
    val workloads: List<AppE2eWorkloadResult>,
)

@Serializable
private data class AppE2eSummary(
    val schemaVersion: Int,
    val repository: AppE2eRepositoryIdentity,
    val device: AndroidInteropSummaryDevice,
    val targetApk: AppE2eArtifactIdentity,
    val testApk: AppE2eArtifactIdentity,
    val backend: String,
    val suite: String,
    val topology: String,
    val networkSwitchProven: Boolean,
    val outageMillis: Long,
    val status: String,
    val checks: List<String>,
    val hostChecks: List<String>,
    val initialGeneration: Long,
    val initialControlEpoch: Long,
    val recoveredGeneration: Long,
    val recoveredControlEpoch: Long,
    val processRecreated: Boolean,
    val runtimeInitializations: List<AppE2eRuntimeInitialization>,
    val resources: List<AppE2eResourceEvidence>,
    val workload: AppE2eWorkload,
    val workloads: List<AppE2eWorkloadResult>,
    val cleanupVerified: Boolean,
    val authorizesKotlinDefault: Boolean,
)

internal fun awaitAndroidInteropRestartMarker(
    expected: AndroidInteropRestartMarker,
    readMarker: () -> AdbCommandResult,
    instrumentationIsDone: () -> Boolean,
    observeInstrumentation: () -> Unit,
    timeoutMillis: Long,
    pollMillis: Long,
    sleep: (Long) -> Unit = ::sleepBounded,
) {
    if (timeoutMillis <= 0L || pollMillis <= 0L) {
        throw InteropFailure("Android restart marker polling configuration was invalid")
    }
    val deadline = InteropDeadline.afterMillis(timeoutMillis)
    while (!deadline.isExpired()) {
        val actual = decodeAndroidInteropRestartMarker(readMarker())
        if (actual != null) {
            if (actual != expected) throw InteropFailure("Android restart marker did not match its invocation")
            return
        }
        if (instrumentationIsDone()) {
            observeInstrumentation()
            throw InteropFailure("Android instrumentation completed before its restart marker")
        }
        sleep(minOf(pollMillis, deadline.remainingMillis()))
    }
    throw InteropFailure("Android restart marker did not become readable within its deadline")
}

private fun decodeAndroidInteropRestartMarker(result: AdbCommandResult): AndroidInteropRestartMarker? {
    if (result.exitCode != 0) return null
    val byteCount = result.standardOutput.toByteArray(StandardCharsets.UTF_8).size
    if (byteCount !in 1..MAXIMUM_RESTART_MARKER_BYTES) return null
    return try {
        ANDROID_INTEROP_JSON.decodeFromString(result.standardOutput)
    } catch (_: Exception) {
        null
    }
}

internal data class AndroidInteropProfile(
    val caseId: String,
    val scenario: AndroidInteropScenario,
    val wireProtocol: String,
    val useEncryption: Boolean,
    val useCompression: Boolean,
    val expectedChecks: List<String>,
) {
    val usesConcurrentTraffic: Boolean
        get() = scenario.kind == SCENARIO_SUCCESS || scenario.kind == SCENARIO_RESTART

    val isRestart: Boolean
        get() = scenario.kind == SCENARIO_RESTART

    val expectedSemanticOutcome: String
        get() = semanticOutcomeForScenario(scenario.kind)
}

internal data class AndroidInteropResultExpectation(
    val runId: String,
    val backend: String,
    val profile: AndroidInteropProfile,
    val expectedBindPort: Int? = null,
)

internal fun androidInteropProfiles(suite: String): List<AndroidInteropProfile> = when (suite) {
    SUITE_COMPAT -> listOf(ALL_ANDROID_INTEROP_PROFILES.first())
    SUITE_FULL -> ALL_ANDROID_INTEROP_PROFILES.toList()
    else -> throw InteropFailure("Android interoperability suite must be compat or full")
}

internal fun validateAndroidInteropResult(
    result: AndroidInteropResult,
    expectation: AndroidInteropResultExpectation,
) {
    val profile = expectation.profile
    if (result.schemaVersion != ANDROID_INTEROP_SCHEMA_VERSION ||
        result.runId != expectation.runId || !SAFE_RUN_ID.matches(result.runId) ||
        result.backend != expectation.backend ||
        result.caseId != profile.caseId || result.scenario != profile.scenario ||
        result.wireProtocol != profile.wireProtocol ||
        result.useEncryption != profile.useEncryption ||
        result.useCompression != profile.useCompression ||
        result.status != STATUS_PASSED || result.failedStage != null || result.failureCode != null ||
        result.semanticOutcome != profile.expectedSemanticOutcome ||
        result.bindPort !in 1..65_535 ||
        (expectation.expectedBindPort != null && result.bindPort != expectation.expectedBindPort) ||
        result.checks != profile.expectedChecks
    ) {
        throw InteropFailure("${expectation.backend} returned an invalid semantic result")
    }
}

internal fun isValidAppE2eRecoveryIdentity(
    backend: String,
    initialGeneration: Long?,
    initialControlEpoch: Long?,
    recoveredGeneration: Long?,
    recoveredControlEpoch: Long?,
): Boolean {
    val initialGenerationValue = initialGeneration ?: return false
    val initialControlEpochValue = initialControlEpoch ?: return false
    val recoveredGenerationValue = recoveredGeneration ?: return false
    val recoveredControlEpochValue = recoveredControlEpoch ?: return false
    if (initialGenerationValue <= 0L || initialControlEpochValue <= 0L ||
        recoveredGenerationValue <= 0L || recoveredControlEpochValue <= 0L
    ) {
        return false
    }
    return when (backend) {
        BACKEND_KOTLIN -> recoveredGenerationValue == initialGenerationValue &&
            recoveredControlEpochValue > initialControlEpochValue
        BACKEND_GO_MOBILE -> recoveredGenerationValue > initialGenerationValue ||
            (recoveredGenerationValue == initialGenerationValue &&
                recoveredControlEpochValue > initialControlEpochValue)
        else -> false
    }
}

internal fun requireEquivalentAndroidInteropResults(
    goMobile: AndroidInteropResult,
    kotlin: AndroidInteropResult,
) {
    val profile = ANDROID_INTEROP_PROFILES_BY_CASE[goMobile.caseId]
        ?: throw InteropFailure("GoMobile and Kotlin Android results were not semantically equivalent")
    validateAndroidInteropResult(
        goMobile,
        AndroidInteropResultExpectation(
            runId = goMobile.runId,
            backend = BACKEND_GO_MOBILE,
            profile = profile,
        ),
    )
    validateAndroidInteropResult(
        kotlin,
        AndroidInteropResultExpectation(
            runId = goMobile.runId,
            backend = BACKEND_KOTLIN,
            profile = profile,
            expectedBindPort = goMobile.bindPort,
        ),
    )
}

internal fun buildAndroidInteropSuiteSummary(
    suite: String,
    apiLevel: Int,
    abi: String,
    pageSize: Int,
    completedProfiles: List<AndroidInteropCompletedProfile>,
): AndroidInteropSuiteSummary {
    val expectedProfiles = androidInteropProfiles(suite)
    if (apiLevel < 26 || abi !in ANDROID_INTEROP_SUPPORTED_ABIS || pageSize <= 0 ||
        completedProfiles.map { it.profile } != expectedProfiles
    ) {
        throw InteropFailure("Android interoperability summary inputs were invalid")
    }
    val profiles = completedProfiles.map { completed ->
        requireEquivalentAndroidInteropResults(completed.goMobile, completed.kotlin)
        if (completed.goMobile.caseId != completed.profile.caseId) {
            throw InteropFailure("Android interoperability summary profile identity was invalid")
        }
        AndroidInteropProfileSummary(
            caseId = completed.profile.caseId,
            scenario = completed.profile.scenario.kind,
            wireProtocol = completed.profile.wireProtocol,
            useEncryption = completed.profile.useEncryption,
            useCompression = completed.profile.useCompression,
            backends = listOf(
                AndroidInteropBackendSummary(
                    BACKEND_GO_MOBILE,
                    completed.goMobile.semanticOutcome ?: throw InteropFailure("GoMobile semantic outcome was missing"),
                    completed.goMobile.checks,
                ),
                AndroidInteropBackendSummary(
                    BACKEND_KOTLIN,
                    completed.kotlin.semanticOutcome ?: throw InteropFailure("Kotlin semantic outcome was missing"),
                    completed.kotlin.checks,
                ),
            ),
            equivalent = true,
        )
    }
    return AndroidInteropSuiteSummary(
        schemaVersion = ANDROID_INTEROP_SUMMARY_SCHEMA_VERSION,
        suite = suite,
        device = AndroidInteropSummaryDevice(apiLevel, abi, pageSize),
        backendOrder = listOf(BACKEND_GO_MOBILE, BACKEND_KOTLIN),
        profiles = profiles,
        authorizesKotlinDefault = false,
    )
}

internal fun writeAndroidInteropSuiteSummary(path: Path, summary: AndroidInteropSuiteSummary) {
    val target = path.toAbsolutePath().normalize()
    val parent = target.parent ?: throw InteropFailure("Android interoperability summary path was invalid")
    if (!Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(parent) ||
        Files.exists(target, LinkOption.NOFOLLOW_LINKS)
    ) {
        throw InteropFailure("Android interoperability summary destination was invalid")
    }
    val bytes = ANDROID_INTEROP_JSON.encodeToString(summary).toByteArray(StandardCharsets.UTF_8)
    if (bytes.size !in 1..MAXIMUM_SUMMARY_BYTES) {
        bytes.fill(0)
        throw InteropFailure("Android interoperability summary exceeded its bounded schema")
    }
    val temporary = parent.resolve(".${target.fileName}.${UUID.randomUUID()}.tmp")
    try {
        writePrivateFile(temporary, bytes)
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE)
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(target) ||
            Files.size(target) !in 1..MAXIMUM_SUMMARY_BYTES
        ) {
            throw InteropFailure("Android interoperability summary was not published safely")
        }
    } catch (failure: InteropFailure) {
        throw failure
    } catch (_: Exception) {
        throw InteropFailure("Android interoperability summary could not be published atomically")
    } finally {
        bytes.fill(0)
        Files.deleteIfExists(temporary)
    }
}

private const val ANDROID_INTEROP_SCHEMA_VERSION = 2
private const val ANDROID_INTEROP_SUMMARY_SCHEMA_VERSION = 1
private const val MAXIMUM_SUMMARY_BYTES = 64 * 1024
private const val MAXIMUM_RESTART_MARKER_BYTES = 2 * 1024
private const val RESTART_MARKER_SCHEMA_VERSION = 1
private const val SUITE_COMPAT = "compat"
private const val SUITE_FULL = "full"
private const val BACKEND_GO_MOBILE = "gomobile"
private const val BACKEND_KOTLIN = "kotlin"
private const val STATUS_PASSED = "passed"
private const val STATUS_FAILED = "failed"
private const val SCENARIO_SUCCESS = "success"
private const val SCENARIO_WRONG_TOKEN = "wrong_token"
private const val SCENARIO_WRONG_SECRET = "wrong_secret"
private const val SCENARIO_BIND_CONFLICT = "bind_conflict"
private const val SCENARIO_RESTART = "restart"
private const val OUTCOME_SUCCESS = "success"
private const val OUTCOME_CONTROL_FAILURE = "control_failure"
private const val OUTCOME_STCP_SECRET_REJECTED = "stcp_secret_rejected"
private const val OUTCOME_BIND_CONFLICT = "bind_conflict"
private const val OUTCOME_RECOVERED = "recovered"
private val ALL_SEMANTIC_OUTCOMES = setOf(
    OUTCOME_SUCCESS,
    OUTCOME_CONTROL_FAILURE,
    OUTCOME_STCP_SECRET_REJECTED,
    OUTCOME_BIND_CONFLICT,
    OUTCOME_RECOVERED,
)

private fun semanticOutcomeForScenario(scenario: String): String = when (scenario) {
    SCENARIO_SUCCESS -> OUTCOME_SUCCESS
    SCENARIO_WRONG_TOKEN -> OUTCOME_CONTROL_FAILURE
    SCENARIO_WRONG_SECRET -> OUTCOME_STCP_SECRET_REJECTED
    SCENARIO_BIND_CONFLICT -> OUTCOME_BIND_CONFLICT
    SCENARIO_RESTART -> OUTCOME_RECOVERED
    else -> throw InteropFailure("Android interoperability scenario was invalid")
}

private val ANDROID_INTEROP_JSON = Json {
    ignoreUnknownKeys = false
    explicitNulls = false
}
private val ANDROID_INTEROP_SUPPORTED_ABIS = setOf("arm64-v8a", "armeabi-v7a", "x86", "x86_64")
private val SAFE_RUN_ID = Regex("[a-f0-9-]{36}")
private val SUCCESS_CHECKS = listOf(
    "open_state",
    "health",
    "global_sse",
    "project_sse",
    "concurrent_rest_sse",
    "closed",
    "port_released",
)
private val WRONG_TOKEN_CHECKS = listOf(
    "control_rejected",
    "session_failed",
    "listener_not_bound",
    "closed",
    "port_released",
)
private val WRONG_SECRET_CHECKS = listOf(
    "open_state",
    "health_rejected",
    "listener_consistent",
    "closed",
    "port_released",
)
private val BIND_CONFLICT_CHECKS = listOf(
    "bind_rejected",
    "visitor_failed",
    "listener_not_bound",
    "closed",
    "port_released",
)
private val RESTART_CHECKS = listOf(
    "initial_open_state",
    "initial_health",
    "initial_global_sse",
    "initial_project_sse",
    "sse_disconnected",
    "control_unavailable",
    "control_epoch_advanced",
    "recovered_open_state",
    "health",
    "global_sse",
    "project_sse",
    "concurrent_rest_sse",
    "closed",
    "port_released",
)
private val ALL_ANDROID_INTEROP_PROFILES = listOf(
    AndroidInteropProfile(
        "success-v1-00",
        AndroidInteropScenario(SCENARIO_SUCCESS),
        "v1",
        false,
        false,
        SUCCESS_CHECKS,
    ),
    AndroidInteropProfile(
        "success-v1-01",
        AndroidInteropScenario(SCENARIO_SUCCESS),
        "v1",
        false,
        true,
        SUCCESS_CHECKS,
    ),
    AndroidInteropProfile(
        "success-v1-10",
        AndroidInteropScenario(SCENARIO_SUCCESS),
        "v1",
        true,
        false,
        SUCCESS_CHECKS,
    ),
    AndroidInteropProfile(
        "success-v1-11",
        AndroidInteropScenario(SCENARIO_SUCCESS),
        "v1",
        true,
        true,
        SUCCESS_CHECKS,
    ),
    AndroidInteropProfile(
        "success-v2-00",
        AndroidInteropScenario(SCENARIO_SUCCESS),
        "v2",
        false,
        false,
        SUCCESS_CHECKS,
    ),
    AndroidInteropProfile(
        "success-v2-01",
        AndroidInteropScenario(SCENARIO_SUCCESS),
        "v2",
        false,
        true,
        SUCCESS_CHECKS,
    ),
    AndroidInteropProfile(
        "success-v2-10",
        AndroidInteropScenario(SCENARIO_SUCCESS),
        "v2",
        true,
        false,
        SUCCESS_CHECKS,
    ),
    AndroidInteropProfile(
        "success-v2-11",
        AndroidInteropScenario(SCENARIO_SUCCESS),
        "v2",
        true,
        true,
        SUCCESS_CHECKS,
    ),
    AndroidInteropProfile(
        "negative-wrong-token-v2-00",
        AndroidInteropScenario(SCENARIO_WRONG_TOKEN),
        "v2",
        false,
        false,
        WRONG_TOKEN_CHECKS,
    ),
    AndroidInteropProfile(
        "negative-wrong-secret-v2-00",
        AndroidInteropScenario(SCENARIO_WRONG_SECRET),
        "v2",
        false,
        false,
        WRONG_SECRET_CHECKS,
    ),
    AndroidInteropProfile(
        "negative-bind-conflict-v2-00",
        AndroidInteropScenario(SCENARIO_BIND_CONFLICT),
        "v2",
        false,
        false,
        BIND_CONFLICT_CHECKS,
    ),
    AndroidInteropProfile(
        "restart-v2-11",
        AndroidInteropScenario(SCENARIO_RESTART),
        "v2",
        true,
        true,
        RESTART_CHECKS,
    ),
)
private val ANDROID_INTEROP_PROFILES_BY_CASE = ALL_ANDROID_INTEROP_PROFILES.associateBy(AndroidInteropProfile::caseId)

object FrpcAndroidInteropHost {
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            if (args.isNotEmpty()) throw InteropFailure("frpcAndroidInteropTest does not accept arguments")
            FrpcAndroidInteropRunner().run()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: InteropFailure) {
            System.err.println("frpc Android interop failed: ${failure.message.orEmpty()}")
            exitProcess(1)
        } catch (_: Exception) {
            System.err.println("frpc Android interop failed: unexpected host coordinator failure")
            exitProcess(1)
        }
    }
}

object FrpcAppE2eHost {
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            if (args.isNotEmpty()) throw InteropFailure("frpcAppE2eTest does not accept arguments")
            FrpcAndroidInteropRunner().runAppE2e()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: InteropFailure) {
            System.err.println("frpc App E2E failed: ${failure.message.orEmpty()}")
            exitProcess(1)
        } catch (_: Exception) {
            System.err.println("frpc App E2E failed: unexpected host coordinator failure")
            exitProcess(1)
        }
    }
}

private class FrpcAndroidInteropRunner {
    private val secureRandom = SecureRandom()
    private var stage = "initialization"
    private var redactor = InteropRedactor()
    private val appE2eBackend: AppE2eBackendSpec by lazy(LazyThreadSafetyMode.NONE) {
        configuredAppE2eBackend()
    }

    fun run() {
        if (!System.getenv("ADB_SERVER_SOCKET").isNullOrBlank()) {
            throw InteropFailure("remote adb servers are not supported by the Android interoperability gate")
        }
        val resources = AndroidInteropResourceOwner()
        var selectedFailure: Throwable? = null
        val shutdownHook = Thread(
            { resources.cleanup() },
            "frpc-android-interop-shutdown-cleanup",
        ).apply { isDaemon = false }
        Runtime.getRuntime().addShutdownHook(shutdownHook)
        try {
            stage = "Android interoperability suite selection"
            val suite = configuredSuite()
            val profiles = androidInteropProfiles(suite)
            val summaryFile = configuredSummaryFile()

            stage = "temporary workspace creation"
            val temporaryDirectory = resources.createTemporaryWorkspace()
            val cacheDirectory = configuredCacheDirectory()
            val secrets = AndroidInteropSecrets.create(secureRandom)
            redactor = InteropRedactor(
                temporaryDirectory.toString(),
                cacheDirectory.toString(),
                secrets.authToken,
                secrets.stcpSecret,
                secrets.wrongAuthToken,
                secrets.wrongStcpSecret,
            )

            stage = "official frp provisioning"
            val binaries = FrpReleaseProvisioner(cacheDirectory).provision()

            stage = "synthetic OpenCode server startup"
            val syntheticServer = resources.createSyntheticServer()
            val frpsPort = allocateLoopbackPort()
            val frpsConfig = temporaryDirectory.resolve("frps-android.toml")
            val providerConfig = temporaryDirectory.resolve("frpc-provider-android.toml")
            stage = "temporary TLS material creation"
            val tlsMaterial = InteropTlsMaterial.create(temporaryDirectory, secureRandom)
            writePrivateFile(frpsConfig, frpsToml(frpsPort, secrets.authToken, tlsMaterial))
            writePrivateFile(
                providerConfig,
                providerToml(frpsPort, syntheticServer.port, secrets.authToken, secrets.stcpSecret),
            )
            redactor = redactor.including(
                binaries.frpc.toString(),
                binaries.frps.toString(),
                frpsConfig.toString(),
                providerConfig.toString(),
                tlsMaterial.certificatePath.toString(),
                tlsMaterial.privateKeyPath.toString(),
            )
            val frpsLaunch = AndroidInteropProcessLaunch(
                label = "android-frps",
                executable = binaries.frps,
                arguments = listOf("-c", frpsConfig.toString()),
                workingDirectory = temporaryDirectory,
                redactor = redactor,
            )
            val providerLaunch = AndroidInteropProcessLaunch(
                label = "android-provider-frpc",
                executable = binaries.frpc,
                arguments = listOf("-c", providerConfig.toString()),
                workingDirectory = temporaryDirectory,
                redactor = redactor,
            )

            stage = "frps startup"
            val frps = resources.startFrps(frpsLaunch::start)
            awaitFrpsReady(frpsPort, frps)

            stage = "provider frpc startup"
            val provider = resources.startProvider(providerLaunch::start)
            provider.awaitOutput(PROVIDER_PROXY_READY_PATTERN, PROCESS_READY_TIMEOUT_MILLIS, stage)

            stage = "Android test APK discovery"
            val testApk = configuredTestApk()
            redactor = redactor.including(testApk.toString())

            stage = "Android device discovery"
            val adbExecutable = configuredAdbExecutable()
            val discovery = AdbCommandRunner(adbExecutable, serial = null, redactor = redactor)
            val serial = selectDevice(discovery, DEVICE_SERIAL_PROPERTY)
            val adb = AdbCommandRunner(adbExecutable, serial, redactor)
            resources.configureAdb(adb)
            val device = readDeviceMetadata(adb)
            stage = "instrumentation package preflight"
            requirePackageAbsent(adb)

            stage = "instrumentation APK installation"
            resources.markPackageInstallAttempted()
            val install = adb.run(
                label = "instrumentation APK installation",
                arguments = listOf("install", "--no-streaming", "-t", testApk.toString()),
                timeoutMillis = APK_INSTALL_TIMEOUT_MILLIS,
            )
            if (!install.standardOutput.lineSequence().any { it.trim() == "Success" }) {
                throw InteropFailure("instrumentation APK installation did not report success")
            }
            requirePackageInstalled(adb)
            resources.markPackageInstalled()

            stage = "adb reverse creation"
            val reverseBaseline = adb.run(
                label = "adb reverse ownership preflight",
                arguments = listOf("reverse", "--list"),
                timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
            )
            resources.registerReverseTarget(
                frpsPort,
                reversePortsForTarget(reverseBaseline.standardOutput, frpsPort).toSet(),
            )
            val reverse = adb.run(
                label = "adb reverse creation",
                arguments = listOf("reverse", "tcp:0", "tcp:$frpsPort"),
                timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
            )
            val deviceFrpsPort = parseReversePort(reverse.standardOutput)
            resources.registerReverse(deviceFrpsPort)

            val runId = UUID.randomUUID().toString()
            val completedProfiles = ArrayList<AndroidInteropCompletedProfile>(profiles.size)
            profiles.forEach { profile ->
                stage = "${profile.caseId} GoMobile Android interoperability scenario"
                val goMobile = runBackend(
                    resources = resources,
                    adb = adb,
                    syntheticServer = syntheticServer,
                    runId = runId,
                    profile = profile,
                    backend = BACKEND_GO_MOBILE,
                    serverPort = deviceFrpsPort,
                    bindPort = 0,
                    secrets = secrets,
                    frpsPort = frpsPort,
                    frpsLaunch = frpsLaunch,
                    provider = provider,
                )
                stage = "${profile.caseId} Kotlin Android interoperability scenario"
                val kotlin = runBackend(
                    resources = resources,
                    adb = adb,
                    syntheticServer = syntheticServer,
                    runId = runId,
                    profile = profile,
                    backend = BACKEND_KOTLIN,
                    serverPort = deviceFrpsPort,
                    bindPort = goMobile.bindPort,
                    secrets = secrets,
                    frpsPort = frpsPort,
                    frpsLaunch = frpsLaunch,
                    provider = provider,
                )
                stage = "${profile.caseId} backend result comparison"
                requireEquivalentAndroidInteropResults(goMobile, kotlin)
                completedProfiles += AndroidInteropCompletedProfile(profile, goMobile, kotlin)
                syntheticServer.requireHealthy()
                resources.requireFrpsAlive(stage)
                provider.requireAlive(stage)
                println("frpc Android interop profile passed: case=${profile.caseId} order=gomobile,kotlin")
            }
            syntheticServer.requireHealthy()
            resources.requireFrpsAlive(stage)
            provider.requireAlive(stage)
            stage = "Android interoperability suite summary"
            summaryFile?.let { output ->
                writeAndroidInteropSuiteSummary(
                    output,
                    buildAndroidInteropSuiteSummary(
                        suite = suite,
                        apiLevel = device.apiLevel,
                        abi = device.abi,
                        pageSize = device.pageSize,
                        completedProfiles = completedProfiles,
                    ),
                )
            }
            println(
                "frpc Android interop passed: api=${device.apiLevel} abi=${device.abi} " +
                    "pageSize=${device.pageSize} suite=$suite profiles=${profiles.size} order=gomobile,kotlin",
            )
        } catch (failure: Throwable) {
            selectedFailure = when (failure) {
                is Error, is CancellationException -> failure
                is Exception -> safeFailure(stage, failure, resources)
                else -> failure
            }
        } finally {
            stage = "resource cleanup"
            val cleanupFailure = resources.cleanup()
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook)
            } catch (_: IllegalStateException) {
                // JVM shutdown has already started; the hook owns cleanup.
            }
            if (cleanupFailure != null) {
                val candidate = when (cleanupFailure) {
                    is Error, is CancellationException -> cleanupFailure
                    else -> InteropFailure("Android interoperability resource cleanup failed")
                }
                selectedFailure = preferFailure(selectedFailure, candidate)
            }
        }
        selectedFailure?.let { throw it }
    }

    fun runAppE2e() {
        if (!System.getenv("ADB_SERVER_SOCKET").isNullOrBlank()) {
            throw InteropFailure("remote adb servers are not supported by the App E2E gate")
        }
        val backend = appE2eBackend
        val resources = AndroidInteropResourceOwner(
            testPackage = backend.testPackage,
            managedPackages = listOf(backend.targetPackage, backend.testPackage),
            appFixtureEnabled = true,
            workspacePrefix = "ocdeck-frp-app-e2e-",
        )
        var selectedFailure: Throwable? = null
        var summaryOutput: Path? = null
        var pendingSummary: AppE2eSummary? = null
        var passMessage: String? = null
        val shutdownHook = Thread(
            { resources.cleanup() },
            "frpc-app-e2e-shutdown-cleanup",
        ).apply { isDaemon = false }
        Runtime.getRuntime().addShutdownHook(shutdownHook)
        try {
            stage = "App E2E temporary workspace creation"
            val temporaryDirectory = resources.createTemporaryWorkspace()
            val cacheDirectory = configuredCacheDirectory()
            val summaryFile = configuredAppE2eSummaryFile()
            summaryOutput = summaryFile
            val repositoryIdentity = summaryFile?.let {
                stage = "App E2E repository identity"
                readAppE2eRepositoryIdentity(Path.of(""))
            }
            val suite = configuredAppE2eSuite()
            if (suite == APP_E2E_SUITE_NETWORK && backend.backend != BACKEND_KOTLIN) {
                throw InteropFailure("App E2E network suite requires the Kotlin backend")
            }
            val outageMillis = configuredAppE2eOutageMillis(suite)
            val workload = configuredAppE2eWorkload(suite)
            stage = "App E2E external configuration"
            val externalConfig = configuredAppE2eExternalConfig(suite)
            val secrets = AndroidInteropSecrets.create(secureRandom)
            val authToken = externalConfig?.authToken ?: secrets.authToken
            redactor = InteropRedactor(
                temporaryDirectory.toString(),
                cacheDirectory.toString(),
                authToken,
                secrets.stcpSecret,
                externalConfig?.frpsHost.orEmpty(),
                externalConfig?.tlsServerCertificateSha256.orEmpty(),
                externalConfig?.tlsTrustedCaCertificatePem.orEmpty(),
                System.getenv(APP_E2E_EXTERNAL_CONFIG_FILE_ENVIRONMENT).orEmpty(),
                System.getProperty(APP_E2E_DEVICE_SERIAL_PROPERTY).orEmpty(),
            )

            stage = "App E2E official frp provisioning"
            val binaries = FrpReleaseProvisioner(cacheDirectory).provision()

            stage = "App E2E synthetic OpenCode server startup"
            val syntheticServer = resources.createSyntheticServer()
            val frpsHost = externalConfig?.frpsHost ?: IPV4_LOOPBACK_TEXT
            val frpsPort = externalConfig?.frpsPort ?: allocateLoopbackPort()
            val providerConfig = temporaryDirectory.resolve("frpc-provider-app-e2e.toml")
            val providerTrustedCaFile = externalConfig?.let { config ->
                val path = temporaryDirectory.resolve("frpc-provider-app-e2e-ca.pem")
                val bytes = appE2eTrustedCaCertificateBytes(config)
                try {
                    writePrivateFile(path, bytes)
                } finally {
                    bytes.fill(0)
                }
                path
            }
            writePrivateFile(
                providerConfig,
                providerToml(
                    frpsPort = frpsPort,
                    syntheticPort = syntheticServer.port,
                    authToken = authToken,
                    stcpSecret = secrets.stcpSecret,
                    frpsHost = frpsHost,
                    trustedCaFile = providerTrustedCaFile,
                ),
            )
            redactor = redactor.including(
                binaries.frpc.toString(),
                providerConfig.toString(),
                providerTrustedCaFile?.toString().orEmpty(),
            )
            val frpsLaunch = if (externalConfig == null) {
                stage = "App E2E temporary TLS material creation"
                val frpsConfig = temporaryDirectory.resolve("frps-app-e2e.toml")
                val tlsMaterial = InteropTlsMaterial.create(temporaryDirectory, secureRandom)
                writePrivateFile(frpsConfig, frpsToml(frpsPort, authToken, tlsMaterial))
                redactor = redactor.including(
                    binaries.frps.toString(),
                    frpsConfig.toString(),
                    tlsMaterial.certificatePath.toString(),
                    tlsMaterial.privateKeyPath.toString(),
                )
                AndroidInteropProcessLaunch(
                    label = "app-e2e-frps",
                    executable = binaries.frps,
                    arguments = listOf("-c", frpsConfig.toString()),
                    workingDirectory = temporaryDirectory,
                    redactor = redactor,
                )
            } else {
                null
            }
            val providerLaunch = AndroidInteropProcessLaunch(
                label = "app-e2e-provider-frpc",
                executable = binaries.frpc,
                arguments = listOf("-c", providerConfig.toString()),
                workingDirectory = temporaryDirectory,
                redactor = redactor,
            )

            frpsLaunch?.let { launch ->
                stage = "App E2E frps startup"
                val frps = resources.startFrps(launch::start)
                awaitFrpsReady(frpsPort, frps)
            }

            stage = "App E2E provider frpc startup"
            val provider = resources.startProvider(providerLaunch::start)
            provider.awaitOutput(PROVIDER_PROXY_READY_PATTERN, PROCESS_READY_TIMEOUT_MILLIS, stage)

            stage = "App E2E Android device discovery"
            val adbExecutable = configuredAppE2eAdbExecutable()
            val discovery = AdbCommandRunner(adbExecutable, serial = null, redactor = redactor)
            val serial = selectDevice(discovery, APP_E2E_DEVICE_SERIAL_PROPERTY)
            val adb = AdbCommandRunner(adbExecutable, serial, redactor)
            resources.configureAdb(adb)
            val device = readDeviceMetadata(adb)
            if (suite == APP_E2E_SUITE_NETWORK) {
                stage = "App E2E external network preflight"
                requireExternalNetworkDevice(adb, device.abi)
                resources.registerNetworkBaseline(wifiEnabled = true, mobileDataEnabled = true)
            }

            stage = "App E2E APK discovery"
            val appApk = configuredAppE2eTargetApk(device.abi)
            val testApk = configuredAppE2eTestApk()
            redactor = redactor.including(appApk.toString(), testApk.toString())
            val localTargetApkSha256 = summaryFile?.let {
                stage = "App E2E target APK identity"
                sha256AppE2eArtifact(appApk)
            }
            val localTestApkSha256 = summaryFile?.let {
                stage = "App E2E instrumentation APK identity"
                sha256AppE2eArtifact(testApk)
            }

            stage = "App E2E package preflight"
            requirePackageAbsent(adb, backend.targetPackage, "target App package")
            requirePackageAbsent(adb, backend.testPackage, "App E2E instrumentation package")

            stage = "App E2E target APK installation"
            installPackage(resources, adb, backend.targetPackage, appApk, "target App", testOnly = false)
            val targetApkIdentity = localTargetApkSha256?.let { localSha256 ->
                stage = "App E2E installed target APK identity"
                readInstalledAppE2eArtifactIdentity(adb, backend.targetPackage, localSha256, "target App")
            }
            stage = "App E2E instrumentation APK installation"
            installPackage(resources, adb, backend.testPackage, testApk, "App E2E instrumentation", testOnly = true)
            val testApkIdentity = localTestApkSha256?.let { localSha256 ->
                stage = "App E2E installed instrumentation APK identity"
                readInstalledAppE2eArtifactIdentity(adb, backend.testPackage, localSha256, "App E2E instrumentation")
            }

            val deviceFrpsHost: String
            val deviceFrpsPort: Int
            if (suite == APP_E2E_SUITE_NETWORK) {
                stage = "App E2E external reverse rejection"
                requireNoAdbReverse(adb)
                deviceFrpsHost = frpsHost
                deviceFrpsPort = frpsPort
            } else {
                stage = "App E2E adb reverse creation"
                val reverseBaseline = adb.run(
                    label = "App E2E adb reverse ownership preflight",
                    arguments = listOf("reverse", "--list"),
                    timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                )
                resources.registerReverseTarget(
                    frpsPort,
                    reversePortsForTarget(reverseBaseline.standardOutput, frpsPort).toSet(),
                )
                val reverse = adb.run(
                    label = "App E2E adb reverse creation",
                    arguments = listOf("reverse", "tcp:0", "tcp:$frpsPort"),
                    timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                )
                deviceFrpsHost = IPV4_LOOPBACK_TEXT
                deviceFrpsPort = parseReversePort(reverse.standardOutput)
                resources.registerReverse(deviceFrpsPort)
            }

            stage = "App E2E instrumentation"
            val runId = UUID.randomUUID().toString()
            val files = AppE2ePrivateFiles.create(runId)
            resources.registerPrivateFiles(files.allRelativePaths)
            val invocation = AppE2eInvocation(
                schemaVersion = APP_E2E_SCHEMA_VERSION,
                runId = runId,
                backend = backend.backend,
                suite = suite,
                phase = if (suite == APP_E2E_SUITE_PROCESS) APP_E2E_PHASE_PROCESS_PREPARE else APP_E2E_PHASE_SINGLE,
                outageMillis = outageMillis,
                frpsHost = deviceFrpsHost,
                frpsPort = deviceFrpsPort,
                authToken = authToken,
                tlsServerCertificateSha256 = externalConfig?.tlsServerCertificateSha256,
                user = VISITOR_USER,
                serverUser = PROVIDER_USER,
                serverName = PROVIDER_PROXY_NAME,
                secretKey = secrets.stcpSecret,
                restartMarkers = files.restartMarkers,
                workload = workload,
            )
            writeAppE2eInvocation(adb, files.configFile, invocation)
            val execution = if (suite == APP_E2E_SUITE_PROCESS) {
                runAppE2eProcessRecovery(
                    adb = adb,
                    invocation = invocation,
                    files = files,
                )
            } else {
                val instrumentation = runAppE2eSuiteInstrumentation(
                    resources = resources,
                    adb = adb,
                    invocation = invocation,
                    files = files,
                    frpsPort = frpsPort,
                    frpsLaunch = frpsLaunch,
                    provider = provider,
                    providerLaunch = providerLaunch,
                    syntheticServer = syntheticServer,
                )
                val result = readAppE2eResult(instrumentation)
                validateAppE2eResult(result, invocation)
                requireInstrumentationSucceeded(instrumentation, "App E2E")
                AppE2eExecution(
                    checks = result.checks,
                    initialGeneration = result.initialGeneration
                        ?: throw InteropFailure("App E2E result omitted its STCP generation"),
                    initialControlEpoch = result.initialControlEpoch
                        ?: throw InteropFailure("App E2E result omitted its control epoch"),
                    recoveredGeneration = result.recoveredGeneration
                        ?: throw InteropFailure("App E2E result omitted its recovered STCP generation"),
                    recoveredControlEpoch = result.recoveredControlEpoch
                        ?: throw InteropFailure("App E2E result omitted its recovered control epoch"),
                    processRecreated = false,
                    runtimeInitializations = listOfNotNull(result.runtimeInitialization),
                    resources = listOf(
                        result.resources ?: throw InteropFailure("App E2E result omitted its resource evidence"),
                    ),
                    workloads = listOfNotNull(result.workload),
                )
            }
            syntheticServer.requireHealthy()
            if (frpsLaunch != null) resources.requireFrpsAlive(stage)
            resources.requireProviderAlive(stage)

            summaryFile?.let {
                pendingSummary = AppE2eSummary(
                        schemaVersion = APP_E2E_SUMMARY_SCHEMA_VERSION,
                        repository = repositoryIdentity
                            ?: throw InteropFailure("App E2E repository identity was missing"),
                        device = AndroidInteropSummaryDevice(device.apiLevel, device.abi, device.pageSize),
                        targetApk = targetApkIdentity
                            ?: throw InteropFailure("App E2E target APK identity was missing"),
                        testApk = testApkIdentity
                            ?: throw InteropFailure("App E2E instrumentation APK identity was missing"),
                        backend = backend.backend,
                        suite = suite,
                        topology = if (suite == APP_E2E_SUITE_NETWORK) TOPOLOGY_EXTERNAL else TOPOLOGY_ADB_REVERSE,
                        networkSwitchProven = suite == APP_E2E_SUITE_NETWORK,
                        outageMillis = outageMillis,
                        status = STATUS_PASSED,
                        checks = execution.checks,
                        hostChecks = if (suite == APP_E2E_SUITE_NETWORK) APP_E2E_NETWORK_HOST_CHECKS else emptyList(),
                        initialGeneration = execution.initialGeneration,
                        initialControlEpoch = execution.initialControlEpoch,
                        recoveredGeneration = execution.recoveredGeneration,
                        recoveredControlEpoch = execution.recoveredControlEpoch,
                        processRecreated = execution.processRecreated,
                        runtimeInitializations = execution.runtimeInitializations,
                        resources = execution.resources,
                        workload = workload,
                        workloads = execution.workloads,
                        cleanupVerified = false,
                        authorizesKotlinDefault = false,
                    )
            }
            passMessage =
                "frpc App E2E passed: api=${device.apiLevel} abi=${device.abi} " +
                    "pageSize=${device.pageSize} suite=$suite checks=${execution.checks.size}"
        } catch (failure: Throwable) {
            selectedFailure = when (failure) {
                is Error, is CancellationException -> failure
                is Exception -> safeFailure(stage, failure, resources)
                else -> failure
            }
        } finally {
            stage = "App E2E resource cleanup"
            val cleanupFailure = resources.cleanup()
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook)
            } catch (_: IllegalStateException) {
                // JVM shutdown has already started; the hook owns cleanup.
            }
            if (cleanupFailure != null) {
                val candidate = when (cleanupFailure) {
                    is Error, is CancellationException -> cleanupFailure
                    else -> InteropFailure("App E2E resource cleanup failed")
                }
                selectedFailure = preferFailure(selectedFailure, candidate)
            }
        }
        selectedFailure?.let { throw it }
        val output = summaryOutput
        val summary = pendingSummary
        if ((output == null) != (summary == null)) {
            throw InteropFailure("App E2E evidence summary state was incomplete")
        }
        if (output != null && summary != null) {
            stage = "App E2E summary"
            writeAppE2eSummary(output, summary.copy(cleanupVerified = true))
        }
        println(passMessage ?: throw InteropFailure("App E2E completion state was missing"))
    }

    private fun runBackend(
        resources: AndroidInteropResourceOwner,
        adb: AdbCommandRunner,
        syntheticServer: SyntheticOpenCodeServer,
        runId: String,
        profile: AndroidInteropProfile,
        backend: String,
        serverPort: Int,
        bindPort: Int,
        secrets: AndroidInteropSecrets,
        frpsPort: Int,
        frpsLaunch: AndroidInteropProcessLaunch,
        provider: ManagedInteropProcess,
    ): AndroidInteropResult {
        val files = AndroidInteropPrivateFiles.create(profile, backend, runId)
        resources.registerPrivateFiles(files.allNames)
        val authToken = if (profile.scenario.kind == SCENARIO_WRONG_TOKEN) {
            secrets.wrongAuthToken
        } else {
            secrets.authToken
        }
        val stcpSecret = if (profile.scenario.kind == SCENARIO_WRONG_SECRET) {
            secrets.wrongStcpSecret
        } else {
            secrets.stcpSecret
        }
        val invocation = AndroidInteropInvocation(
            schemaVersion = ANDROID_INTEROP_SCHEMA_VERSION,
            runId = runId,
            backend = backend,
            caseId = profile.caseId,
            scenario = profile.scenario,
            serverPort = serverPort,
            authToken = authToken,
            stcpSecret = stcpSecret,
            bindPort = bindPort,
            wireProtocol = profile.wireProtocol,
            useEncryption = profile.useEncryption,
            useCompression = profile.useCompression,
            restartMarkers = files.restartMarkers,
        )
        var gate: ConcurrentTrafficGate? = null
        var result: AndroidInteropResult? = null
        var selectedFailure: Throwable? = null
        try {
            val configBytes = ANDROID_INTEROP_JSON.encodeToString(invocation).toByteArray(StandardCharsets.UTF_8)
            try {
                writeDevicePrivateFile(adb, backend, files.configFile, configBytes)
            } finally {
                configBytes.fill(0)
            }
            if (profile.usesConcurrentTraffic) {
                gate = syntheticServer.beginConcurrentTraffic(
                    CONCURRENT_TRAFFIC_REQUESTS,
                    publishSseCheckpointOnSuccess = true,
                )
            }
            val instrumentation = if (profile.isRestart) {
                runRestartInstrumentation(
                    resources = resources,
                    adb = adb,
                    invocation = invocation,
                    files = files,
                    frpsPort = frpsPort,
                    frpsLaunch = frpsLaunch,
                    provider = provider,
                )
            } else {
                runInstrumentation(adb, invocation, files)
            }
            val decoded = readResult(adb, backend, files.resultFile)
            validateResultEnvelope(decoded, invocation, profile)
            if (decoded.status == STATUS_FAILED) {
                throw InteropFailure(
                    "$backend Android scenario failed at ${decoded.failedStage} with ${decoded.failureCode}",
                )
            }
            validateAndroidInteropResult(
                decoded,
                AndroidInteropResultExpectation(
                    runId = runId,
                    backend = backend,
                    profile = profile,
                    expectedBindPort = bindPort.takeIf { it != 0 },
                ),
            )
            requireInstrumentationSucceeded(instrumentation, backend)
            gate?.requireSatisfied()
            syntheticServer.requireHealthy()
            result = decoded
        } catch (failure: Throwable) {
            selectedFailure = failure
        }
        gate?.let { activeGate ->
            try {
                syntheticServer.endConcurrentTraffic(activeGate)
            } catch (failure: Throwable) {
                selectedFailure = preferFailure(selectedFailure, failure)
            }
        }
        try {
            resources.forceStopAndDeletePrivateFiles()
        } catch (failure: Throwable) {
            selectedFailure = preferFailure(selectedFailure, failure)
        }
        selectedFailure?.let { throw it }
        return result ?: throw InteropFailure("$backend did not return a semantic result")
    }

    private fun installPackage(
        resources: AndroidInteropResourceOwner,
        adb: AdbCommandRunner,
        packageName: String,
        apk: Path,
        label: String,
        testOnly: Boolean,
    ) {
        resources.markPackageInstallAttempted(packageName)
        val arguments = buildList {
            addAll(listOf("install", "--no-streaming"))
            if (testOnly) add("-t")
            add(apk.toString())
        }
        val install = adb.run(
            label = "$label APK installation",
            arguments = arguments,
            timeoutMillis = APK_INSTALL_TIMEOUT_MILLIS,
        )
        if (!install.standardOutput.lineSequence().any { it.trim() == "Success" }) {
            throw InteropFailure("$label APK installation did not report success")
        }
        requirePackageInstalled(adb, packageName, "$label package")
        resources.markPackageInstalled(packageName)
    }

    private fun readInstalledAppE2eArtifactIdentity(
        adb: AdbCommandRunner,
        packageName: String,
        localSha256: String,
        label: String,
    ): AppE2eArtifactIdentity {
        val packagePath = parseInstalledAppE2eApkPath(
            adb.run(
                label = "$label installed APK path",
                arguments = listOf("shell", "pm", "path", packageName),
                timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
            ).standardOutput,
        )
        val installedSha256 = parseInstalledAppE2eSha256(
            adb.run(
                label = "$label installed APK digest",
                arguments = listOf("shell", "toybox", "sha256sum", packagePath),
                timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
            ).standardOutput,
        )
        return buildAppE2eArtifactIdentity(localSha256, installedSha256)
    }

    private fun writeAppE2eInvocation(adb: AdbCommandRunner, fileName: String, invocation: AppE2eInvocation) {
        val bytes = ANDROID_INTEROP_JSON.encodeToString(invocation).toByteArray(StandardCharsets.UTF_8)
        try {
            writeAppE2ePrivateFile(adb, "App E2E invocation", fileName, bytes)
        } finally {
            bytes.fill(0)
        }
    }

    private fun writeAppE2ePrivateFile(
        adb: AdbCommandRunner,
        label: String,
        fileName: String,
        bytes: ByteArray,
    ) {
        if (!APP_E2E_SAFE_FILE.matches(fileName) || bytes.size !in 1..MAXIMUM_PRIVATE_FILE_BYTES) {
            throw InteropFailure("App E2E private file write was invalid")
        }
        adb.run(
            label = "$label directory preparation",
            arguments = listOf("shell", "run-as", appE2eBackend.testPackage, "mkdir", "-p", "files"),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        )
        adb.run(
            label = "$label write",
            arguments = listOf(
                "shell",
                "-T",
                "run-as",
                appE2eBackend.testPackage,
                "dd",
                "of=files/$fileName",
                "bs=1",
                "count=${bytes.size}",
            ),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
            standardInput = bytes,
        )
        adb.run(
            label = "$label permission",
            arguments = listOf(
                "shell",
                "run-as",
                appE2eBackend.testPackage,
                "chmod",
                "600",
                "files/$fileName",
            ),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        )
    }

    private fun runAppE2eInstrumentation(
        adb: AdbCommandRunner,
        invocation: AppE2eInvocation,
        files: AppE2ePrivateFiles,
    ): AdbCommandResult {
        val runtimeDiagnosticArguments = if (configuredAppE2eRuntimeDiagnosticEnabled()) {
            listOf(
                "-e",
                "ocdeck.appE2e.runtimeDiagnostic.enabled",
                "true",
            )
        } else {
            emptyList()
        }
        return adb.run(
            label = "App E2E instrumentation",
            arguments = listOf(
                "shell",
                "am",
                "instrument",
                "-w",
                "-r",
                "-e",
                "class",
                "$APP_E2E_TEST_CLASS#$APP_E2E_TEST_METHOD",
                "-e",
                "ocdeck.appE2e.enabled",
                "true",
                "-e",
                "ocdeck.appE2e.runId",
                invocation.runId,
                "-e",
                "ocdeck.appE2e.configFile",
                files.configFile,
            ) + runtimeDiagnosticArguments + listOf(
                "${appE2eBackend.testPackage}/$TEST_RUNNER",
            ),
            timeoutMillis = appE2eInstrumentationTimeoutMillis(invocation.workload),
            requireSuccess = false,
        )
    }

    private fun runAppE2eSuiteInstrumentation(
        resources: AndroidInteropResourceOwner,
        adb: AdbCommandRunner,
        invocation: AppE2eInvocation,
        files: AppE2ePrivateFiles,
        frpsPort: Int,
        frpsLaunch: AndroidInteropProcessLaunch?,
        provider: ManagedInteropProcess,
        providerLaunch: AndroidInteropProcessLaunch,
        syntheticServer: SyntheticOpenCodeServer,
    ): AdbCommandResult = when (invocation.suite) {
        APP_E2E_SUITE_CORE -> runAppE2eRestartInstrumentation(
            resources = resources,
            adb = adb,
            invocation = invocation,
            files = files,
            frpsPort = frpsPort,
            frpsLaunch = frpsLaunch ?: throw InteropFailure("App E2E core suite omitted local frps"),
            provider = provider,
            syntheticServer = syntheticServer,
        )
        APP_E2E_SUITE_LIFECYCLE -> runAppE2eLifecycleInstrumentation(
            resources = resources,
            adb = adb,
            invocation = invocation,
            files = files,
            frpsPort = frpsPort,
            frpsLaunch = frpsLaunch ?: throw InteropFailure("App E2E lifecycle suite omitted local frps"),
            providerLaunch = providerLaunch,
            syntheticServer = syntheticServer,
        )
        APP_E2E_SUITE_NETWORK -> runAppE2eNetworkInstrumentation(
            resources = resources,
            adb = adb,
            invocation = invocation,
            files = files,
            providerLaunch = providerLaunch,
            syntheticServer = syntheticServer,
        )
        else -> throw InteropFailure("App E2E suite was invalid")
    }

    private fun runAppE2eProcessRecovery(
        adb: AdbCommandRunner,
        invocation: AppE2eInvocation,
        files: AppE2ePrivateFiles,
    ): AppE2eExecution {
        stage = "App E2E process prepare instrumentation"
        val prepareInstrumentation = runAppE2eInstrumentation(adb, invocation, files)
        val prepare = readAppE2eResult(prepareInstrumentation)
        validateAppE2eResult(prepare, invocation)
        requireInstrumentationSucceeded(prepareInstrumentation, "App E2E process prepare")

        stage = "App E2E ordinary target process startup"
        launchAppE2eActivity(adb)
        val prepareProcessId = awaitTargetProcess(adb)

        stage = "App E2E process background"
        adb.run(
            label = "App E2E process background",
            arguments = listOf("shell", "input", "keyevent", "KEYCODE_HOME"),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        )
        sleepBounded(TARGET_PROCESS_BACKGROUND_SETTLE_MILLIS)
        if (readTargetProcessId(adb) != prepareProcessId) {
            throw InteropFailure("App E2E target process identity changed before am kill")
        }

        stage = "App E2E target process termination"
        adb.run(
            label = "App E2E target process termination",
            arguments = listOf("shell", "am", "kill", appE2eBackend.targetPackage),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        )
        awaitTargetProcessAbsent(adb)

        stage = "App E2E process recovery instrumentation"
        val recoveryInvocation = invocation.copy(phase = APP_E2E_PHASE_PROCESS_RECOVER)
        writeAppE2eInvocation(adb, files.configFile, recoveryInvocation)
        val recoveryInstrumentation = runAppE2eInstrumentation(adb, recoveryInvocation, files)
        val recovery = readAppE2eResult(recoveryInstrumentation)
        validateAppE2eResult(recovery, recoveryInvocation)
        requireInstrumentationSucceeded(recoveryInstrumentation, "App E2E process recovery")
        val recoveryProcessId = recovery.processId
            ?.takeIf { it > 0 }
            ?: throw InteropFailure("App E2E process recovery result omitted its process identity")
        if (recoveryProcessId == prepareProcessId) {
            throw InteropFailure("App E2E target process identity was not recreated")
        }

        val recoveredGeneration = recovery.initialGeneration
            ?: throw InteropFailure("App E2E process recovery result omitted its STCP generation")
        val recoveredControlEpoch = recovery.initialControlEpoch
            ?: throw InteropFailure("App E2E process recovery result omitted its control epoch")
        return AppE2eExecution(
            checks = prepare.checks + APP_E2E_PROCESS_TERMINATED_CHECK + recovery.checks,
            initialGeneration = prepare.initialGeneration
                ?: throw InteropFailure("App E2E process prepare result omitted its STCP generation"),
            initialControlEpoch = prepare.initialControlEpoch
                ?: throw InteropFailure("App E2E process prepare result omitted its control epoch"),
            recoveredGeneration = recoveredGeneration,
            recoveredControlEpoch = recoveredControlEpoch,
            processRecreated = true,
            runtimeInitializations = listOfNotNull(prepare.runtimeInitialization, recovery.runtimeInitialization),
            resources = listOf(
                prepare.resources ?: throw InteropFailure("App E2E process prepare omitted its resource evidence"),
                recovery.resources ?: throw InteropFailure("App E2E process recovery omitted its resource evidence"),
            ),
            workloads = emptyList(),
        )
    }

    private fun awaitTargetProcess(adb: AdbCommandRunner): Int {
        val deadline = InteropDeadline.afterMillis(TARGET_PROCESS_TIMEOUT_MILLIS)
        while (!deadline.isExpired()) {
            readTargetProcessId(adb)?.let { return it }
            sleepBounded(MARKER_POLL_MILLIS)
        }
        throw InteropFailure("App E2E target process was not observed within its deadline")
    }

    private fun awaitTargetProcessAbsent(adb: AdbCommandRunner) {
        val deadline = InteropDeadline.afterMillis(TARGET_PROCESS_TIMEOUT_MILLIS)
        while (!deadline.isExpired()) {
            if (readTargetProcessId(adb) == null) return
            sleepBounded(MARKER_POLL_MILLIS)
        }
        throw InteropFailure("App E2E target process did not terminate after am kill")
    }

    private fun readTargetProcessId(adb: AdbCommandRunner): Int? {
        val result = adb.run(
            label = "App E2E target process discovery",
            arguments = listOf("shell", "pidof", appE2eBackend.targetPackage),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
            requireSuccess = false,
        )
        val output = result.standardOutput.trim()
        if (result.exitCode != 0) {
            if (output.isEmpty()) return null
            throw InteropFailure("App E2E target process discovery failed")
        }
        val candidates = output.split(Regex("\\s+")).filter(String::isNotEmpty)
        return candidates.singleOrNull()?.toIntOrNull()?.takeIf { it > 0 }
            ?: throw InteropFailure("App E2E target process discovery was ambiguous")
    }

    private fun runAppE2eRestartInstrumentation(
        resources: AndroidInteropResourceOwner,
        adb: AdbCommandRunner,
        invocation: AppE2eInvocation,
        files: AppE2ePrivateFiles,
        frpsPort: Int,
        frpsLaunch: AndroidInteropProcessLaunch,
        provider: ManagedInteropProcess,
        syntheticServer: SyntheticOpenCodeServer,
    ): AdbCommandResult {
        val executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "frpc-app-e2e-instrumentation").apply { isDaemon = true }
        }
        val asyncFailure = AtomicReference<Throwable?>(null)
        val future = try {
            executor.submit<AdbCommandResult> {
                try {
                    runAppE2eInstrumentation(adb, invocation, files)
                } catch (failure: Throwable) {
                    asyncFailure.compareAndSet(null, failure)
                    throw failure
                }
            }
        } catch (failure: Throwable) {
            executor.shutdownNow()
            throw failure
        }
        val deadline = InteropDeadline.afterMillis(appE2eInstrumentationTimeoutMillis(invocation.workload))
        var result: AdbCommandResult? = null
        var selectedFailure: Throwable? = null
        try {
            stage = "App E2E restart device ready"
            awaitAppE2eMarker(
                adb,
                invocation.restartMarkers.deviceReadyFile,
                future,
                appE2eInstrumentationTimeoutMillis(invocation.workload),
            )
            val providerReadyRevision = provider.outputRevision()

            stage = "App E2E frps stop"
            resources.stopFrps()

            stage = "App E2E disconnect retention"
            awaitAppE2eMarker(
                adb,
                invocation.restartMarkers.deviceDisconnectedFile,
                future,
            )

            stage = "App E2E fixture advance"
            syntheticServer.advanceAppFixture()
            stage = "App E2E frps restart"
            val restarted = resources.startFrps(frpsLaunch::start)
            awaitFrpsReady(frpsPort, restarted)
            provider.awaitOutputAfter(
                PROVIDER_PROXY_READY_PATTERN,
                providerReadyRevision,
                PROVIDER_RECONNECT_TIMEOUT_MILLIS,
                stage,
            )
            writeAppE2ePrivateFile(
                adb = adb,
                label = "App E2E frps restarted marker",
                fileName = invocation.restartMarkers.frpsRestartedFile,
                bytes = MARKER_FRPS_RESTARTED.toByteArray(StandardCharsets.UTF_8),
            )

            stage = "App E2E instrumentation completion"
            result = awaitInstrumentation(future, deadline.remainingMillis())
        } catch (failure: Throwable) {
            selectedFailure = failure
        }
        if (selectedFailure != null) {
            stopAsyncInstrumentation(resources, future)?.let { cleanupFailure ->
                selectedFailure = preferFailure(selectedFailure, cleanupFailure)
            }
        }
        shutdownInstrumentationExecutor(executor, future, selectedFailure != null)?.let { cleanupFailure ->
            selectedFailure = preferFailure(selectedFailure, cleanupFailure)
        }
        asyncFailure.get()?.let { failure ->
            selectedFailure = preferFailure(selectedFailure, failure)
        }
        selectedFailure?.let { throw it }
        return result ?: throw InteropFailure("App E2E restart instrumentation did not return a result")
    }

    private fun runAppE2eLifecycleInstrumentation(
        resources: AndroidInteropResourceOwner,
        adb: AdbCommandRunner,
        invocation: AppE2eInvocation,
        files: AppE2ePrivateFiles,
        frpsPort: Int,
        frpsLaunch: AndroidInteropProcessLaunch,
        providerLaunch: AndroidInteropProcessLaunch,
        syntheticServer: SyntheticOpenCodeServer,
    ): AdbCommandResult {
        val executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "frpc-app-e2e-lifecycle-instrumentation").apply { isDaemon = true }
        }
        val asyncFailure = AtomicReference<Throwable?>(null)
        val future = try {
            executor.submit<AdbCommandResult> {
                try {
                    runAppE2eInstrumentation(adb, invocation, files)
                } catch (failure: Throwable) {
                    asyncFailure.compareAndSet(null, failure)
                    throw failure
                }
            }
        } catch (failure: Throwable) {
            executor.shutdownNow()
            throw failure
        }
        val deadline = InteropDeadline.afterMillis(appE2eInstrumentationTimeoutMillis(invocation.workload))
        var result: AdbCommandResult? = null
        var selectedFailure: Throwable? = null
        try {
            stage = "App E2E lifecycle device ready"
            awaitAppE2eMarker(adb, invocation.restartMarkers.deviceReadyFile, future)

            stage = "App E2E lifecycle provider stop"
            resources.stopProvider()
            stage = "App E2E lifecycle frps stop"
            resources.stopFrps()

            stage = "App E2E lifecycle disconnect retention"
            awaitAppE2eMarker(adb, invocation.restartMarkers.deviceDisconnectedFile, future)

            stage = "App E2E lifecycle long outage"
            awaitAppE2eOutage(future, invocation.outageMillis)
            syntheticServer.advanceAppFixture()

            stage = "App E2E lifecycle frps-only recovery"
            val restartedFrps = resources.startFrps(frpsLaunch::start)
            awaitFrpsReady(frpsPort, restartedFrps)
            writeAppE2ePrivateFile(
                adb = adb,
                label = "App E2E frps-only marker",
                fileName = invocation.restartMarkers.frpsOnlyFile,
                bytes = MARKER_FRPS_ONLY.toByteArray(StandardCharsets.UTF_8),
            )
            launchAppE2eActivity(adb)
            awaitAppE2eMarker(adb, invocation.restartMarkers.providerWindowObservedFile, future)

            stage = "App E2E lifecycle provider recovery"
            val restartedProvider = resources.startProvider(providerLaunch::start)
            restartedProvider.awaitOutput(PROVIDER_PROXY_READY_PATTERN, PROCESS_READY_TIMEOUT_MILLIS, stage)
            writeAppE2ePrivateFile(
                adb = adb,
                label = "App E2E fully restarted marker",
                fileName = invocation.restartMarkers.frpsRestartedFile,
                bytes = MARKER_FRPS_RESTARTED.toByteArray(StandardCharsets.UTF_8),
            )

            stage = "App E2E lifecycle instrumentation completion"
            result = awaitInstrumentation(future, deadline.remainingMillis())
        } catch (failure: Throwable) {
            selectedFailure = failure
        }
        if (selectedFailure != null) {
            stopAsyncInstrumentation(resources, future)?.let { cleanupFailure ->
                selectedFailure = preferFailure(selectedFailure, cleanupFailure)
            }
        }
        shutdownInstrumentationExecutor(executor, future, selectedFailure != null)?.let { cleanupFailure ->
            selectedFailure = preferFailure(selectedFailure, cleanupFailure)
        }
        asyncFailure.get()?.let { failure ->
            selectedFailure = preferFailure(selectedFailure, failure)
        }
        selectedFailure?.let { throw it }
        return result ?: throw InteropFailure("App E2E lifecycle instrumentation did not return a result")
    }

    private fun runAppE2eNetworkInstrumentation(
        resources: AndroidInteropResourceOwner,
        adb: AdbCommandRunner,
        invocation: AppE2eInvocation,
        files: AppE2ePrivateFiles,
        providerLaunch: AndroidInteropProcessLaunch,
        syntheticServer: SyntheticOpenCodeServer,
    ): AdbCommandResult {
        val executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "frpc-app-e2e-network-instrumentation").apply { isDaemon = true }
        }
        val asyncFailure = AtomicReference<Throwable?>(null)
        val future = try {
            executor.submit<AdbCommandResult> {
                try {
                    runAppE2eInstrumentation(adb, invocation, files)
                } catch (failure: Throwable) {
                    asyncFailure.compareAndSet(null, failure)
                    throw failure
                }
            }
        } catch (failure: Throwable) {
            executor.shutdownNow()
            throw failure
        }
        val deadline = InteropDeadline.afterMillis(appE2eInstrumentationTimeoutMillis(invocation.workload))
        var result: AdbCommandResult? = null
        var selectedFailure: Throwable? = null
        try {
            stage = "App E2E network device ready"
            awaitAppE2eMarker(adb, invocation.restartMarkers.deviceReadyFile, future)

            stage = "App E2E cellular disconnect"
            resources.stopProvider()
            setWifiEnabled(adb, enabled = false)
            awaitHostValidatedTransport(adb, HOST_TRANSPORT_CELLULAR)
            writeAppE2ePrivateFile(
                adb = adb,
                label = "App E2E cellular disconnected marker",
                fileName = invocation.restartMarkers.frpsOnlyFile,
                bytes = MARKER_NETWORK_CELLULAR_DISCONNECTED.toByteArray(StandardCharsets.UTF_8),
            )
            awaitAppE2eMarker(adb, invocation.restartMarkers.deviceDisconnectedFile, future)

            stage = "App E2E cellular recovery"
            syntheticServer.advanceAppFixture()
            val cellularProvider = resources.startProvider(providerLaunch::start)
            cellularProvider.awaitOutput(PROVIDER_PROXY_READY_PATTERN, PROCESS_READY_TIMEOUT_MILLIS, stage)
            writeAppE2ePrivateFile(
                adb = adb,
                label = "App E2E cellular recovered marker",
                fileName = invocation.restartMarkers.frpsRestartedFile,
                bytes = MARKER_NETWORK_CELLULAR_RECOVERED.toByteArray(StandardCharsets.UTF_8),
            )
            awaitAppE2eMarker(adb, invocation.restartMarkers.providerWindowObservedFile, future)

            stage = "App E2E Wi-Fi disconnect"
            resources.stopProvider()
            setWifiEnabled(adb, enabled = true)
            awaitHostValidatedTransport(adb, HOST_TRANSPORT_WIFI)
            setMobileDataEnabled(adb, enabled = false)
            awaitHostValidatedTransport(adb, HOST_TRANSPORT_WIFI)
            writeAppE2ePrivateFile(
                adb = adb,
                label = "App E2E Wi-Fi disconnected marker",
                fileName = invocation.restartMarkers.wifiDisconnectedFile,
                bytes = MARKER_NETWORK_WIFI_DISCONNECTED.toByteArray(StandardCharsets.UTF_8),
            )
            awaitAppE2eMarker(adb, invocation.restartMarkers.wifiRetentionObservedFile, future)

            stage = "App E2E Wi-Fi recovery"
            syntheticServer.advanceAppFixture()
            val wifiProvider = resources.startProvider(providerLaunch::start)
            wifiProvider.awaitOutput(PROVIDER_PROXY_READY_PATTERN, PROCESS_READY_TIMEOUT_MILLIS, stage)
            writeAppE2ePrivateFile(
                adb = adb,
                label = "App E2E Wi-Fi recovered marker",
                fileName = invocation.restartMarkers.wifiRecoveredFile,
                bytes = MARKER_NETWORK_WIFI_RECOVERED.toByteArray(StandardCharsets.UTF_8),
            )
            awaitAppE2eMarker(adb, invocation.restartMarkers.wifiRecoveryObservedFile, future)

            stage = "App E2E mobile data restoration"
            setMobileDataEnabled(adb, enabled = true)
            awaitHostValidatedTransport(adb, HOST_TRANSPORT_WIFI)

            stage = "App E2E network instrumentation completion"
            result = awaitInstrumentation(future, deadline.remainingMillis())
        } catch (failure: Throwable) {
            selectedFailure = failure
        }
        if (selectedFailure != null) {
            stopAsyncInstrumentation(resources, future)?.let { cleanupFailure ->
                selectedFailure = preferFailure(selectedFailure, cleanupFailure)
            }
        }
        shutdownInstrumentationExecutor(executor, future, selectedFailure != null)?.let { cleanupFailure ->
            selectedFailure = preferFailure(selectedFailure, cleanupFailure)
        }
        asyncFailure.get()?.let { failure ->
            selectedFailure = preferFailure(selectedFailure, failure)
        }
        selectedFailure?.let { throw it }
        return result ?: throw InteropFailure("App E2E network instrumentation did not return a result")
    }

    private fun awaitAppE2eOutage(instrumentation: Future<AdbCommandResult>, outageMillis: Long) {
        val deadline = InteropDeadline.afterMillis(outageMillis)
        while (!deadline.isExpired()) {
            if (instrumentation.isDone) {
                awaitInstrumentation(instrumentation, ASYNC_OBSERVE_TIMEOUT_MILLIS)
                throw InteropFailure("App E2E instrumentation ended during the long outage")
            }
            sleepBounded(minOf(OUTAGE_POLL_MILLIS, deadline.remainingMillis()))
        }
    }

    private fun awaitAppE2eMarker(
        adb: AdbCommandRunner,
        fileName: String,
        instrumentation: Future<AdbCommandResult>,
        timeoutMillis: Long = MARKER_WAIT_TIMEOUT_MILLIS,
    ) {
        val deadline = InteropDeadline.afterMillis(timeoutMillis)
        while (!deadline.isExpired()) {
            val result = adb.run(
                label = "App E2E restart marker read",
                arguments = listOf("shell", "run-as", appE2eBackend.testPackage, "ls", "files/$fileName"),
                timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                requireSuccess = false,
            )
            if (result.exitCode == 0) return
            if (instrumentation.isDone) {
                val completed = awaitInstrumentation(instrumentation, ASYNC_OBSERVE_TIMEOUT_MILLIS)
                val semantic = readAppE2eResult(completed)
                val failedStage = semantic.failedStage?.takeIf(SAFE_RESULT_VALUE::matches) ?: "none"
                val failureCode = semantic.failureCode?.takeIf(SAFE_RESULT_VALUE::matches) ?: "none"
                throw InteropFailure(
                    "App E2E instrumentation ended before its restart marker: " +
                        "status=${semantic.status} stage=$failedStage code=$failureCode " +
                        "checkCount=${semantic.checks.size}",
                )
            }
            sleepBounded(MARKER_POLL_MILLIS)
        }
        throw InteropFailure("App E2E restart marker was not observed within its deadline")
    }

    private fun launchAppE2eActivity(adb: AdbCommandRunner) {
        adb.run(
            label = "App E2E lifecycle foreground",
            arguments = listOf(
                "shell",
                "am",
                "start",
                "-W",
                "-n",
                "${appE2eBackend.targetPackage}/$APP_MAIN_ACTIVITY_CLASS",
            ),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        )
    }

    private fun requireExternalNetworkDevice(adb: AdbCommandRunner, abi: String) {
        if (System.getProperty(APP_E2E_DEVICE_SERIAL_PROPERTY).isNullOrBlank() || abi != "arm64-v8a") {
            throw InteropFailure("App E2E external network suite requires an explicit physical ARM device")
        }
        val qemu = adbShellValue(adb, "App E2E emulator preflight", "getprop", "ro.kernel.qemu")
        val usbConfig = adbShellValue(adb, "App E2E USB preflight", "getprop", "sys.usb.config")
        if (qemu !in setOf("", "0") || "adb" !in usbConfig.split(',')) {
            throw InteropFailure("App E2E external network suite requires USB adb on a physical device")
        }
        requireNoAdbReverse(adb)
        if (readToggleState(adb, "wifi_on") != true || readToggleState(adb, "mobile_data") != true) {
            throw InteropFailure("App E2E external network suite requires Wi-Fi and mobile data enabled")
        }
        awaitHostValidatedTransport(adb, HOST_TRANSPORT_WIFI)
        val snapshot = readHostConnectivitySnapshot(adb)
        if (!hasValidatedTransport(snapshot, HOST_TRANSPORT_CELLULAR)) {
            throw InteropFailure("App E2E external network suite requires validated cellular Internet")
        }
    }

    private fun requireNoAdbReverse(adb: AdbCommandRunner) {
        val reverse = adb.run(
            label = "App E2E external adb reverse preflight",
            arguments = listOf("reverse", "--list"),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        )
        if (reverse.standardOutput.isNotBlank()) {
            throw InteropFailure("App E2E external network suite forbids adb reverse mappings")
        }
    }

    private fun setWifiEnabled(adb: AdbCommandRunner, enabled: Boolean) {
        adb.run(
            label = "App E2E Wi-Fi state change",
            arguments = listOf("shell", "svc", "wifi", if (enabled) "enable" else "disable"),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        )
    }

    private fun setMobileDataEnabled(adb: AdbCommandRunner, enabled: Boolean) {
        adb.run(
            label = "App E2E mobile data state change",
            arguments = listOf("shell", "svc", "data", if (enabled) "enable" else "disable"),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        )
    }

    private fun readToggleState(adb: AdbCommandRunner, setting: String): Boolean? =
        when (adbShellValue(adb, "App E2E network setting read", "settings", "get", "global", setting)) {
            "1" -> true
            "0" -> false
            else -> null
        }

    private fun awaitToggleState(adb: AdbCommandRunner, setting: String, expected: Boolean) {
        val deadline = InteropDeadline.afterMillis(NETWORK_SWITCH_TIMEOUT_MILLIS)
        while (!deadline.isExpired()) {
            if (readToggleState(adb, setting) == expected) return
            sleepBounded(minOf(NETWORK_SWITCH_POLL_MILLIS, deadline.remainingMillis()))
        }
        throw InteropFailure("Android network toggle state was not restored")
    }

    private fun adbShellValue(adb: AdbCommandRunner, label: String, vararg command: String): String =
        adb.run(
            label = label,
            arguments = listOf("shell") + command,
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        ).standardOutput.trim()

    private fun awaitHostValidatedTransport(adb: AdbCommandRunner, transport: String) {
        val deadline = InteropDeadline.afterMillis(NETWORK_SWITCH_TIMEOUT_MILLIS)
        while (!deadline.isExpired()) {
            val snapshot = readHostConnectivitySnapshot(adb)
            if (hasValidatedActiveTransport(snapshot, transport)) {
                return
            }
            sleepBounded(minOf(NETWORK_SWITCH_POLL_MILLIS, deadline.remainingMillis()))
        }
        throw InteropFailure("App E2E validated network transport did not change within its deadline")
    }

    private fun readHostConnectivitySnapshot(adb: AdbCommandRunner): String = adb.run(
        label = "App E2E connectivity state read",
        arguments = listOf("shell", "dumpsys", "connectivity"),
        timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
    ).standardOutput

    private fun readAppE2eResult(instrumentation: AdbCommandResult): AppE2eResult {
        val prefix = "INSTRUMENTATION_STATUS: $APP_E2E_RESULT_STATUS_KEY="
        val candidates = instrumentation.standardOutput.lineSequence()
            .filter { it.startsWith(prefix) }
            .map { it.removePrefix(prefix) }
            .toList()
        if (candidates.size != 1) {
            requireInstrumentationSucceeded(instrumentation, "App E2E")
            throw InteropFailure("App E2E instrumentation omitted its bounded semantic result")
        }
        val output = candidates.single()
        val byteCount = output.toByteArray(StandardCharsets.UTF_8).size
        if (byteCount !in 1..MAXIMUM_APP_E2E_RESULT_BYTES) {
            throw InteropFailure("App E2E returned an invalid bounded result")
        }
        return try {
            ANDROID_INTEROP_JSON.decodeFromString(output)
        } catch (_: Exception) {
            val diagnostic = try {
                val value = ANDROID_INTEROP_JSON.parseToJsonElement(output).jsonObject
                val status = value["status"]?.jsonPrimitive?.content?.takeIf(SAFE_RESULT_VALUE::matches)
                val failedStage = value["failedStage"]?.jsonPrimitive?.content?.takeIf(SAFE_RESULT_VALUE::matches)
                val failureCode = value["failureCode"]?.jsonPrimitive?.content?.takeIf(SAFE_RESULT_VALUE::matches)
                " status=${status ?: "invalid"} stage=${failedStage ?: "none"} code=${failureCode ?: "none"}"
            } catch (_: Exception) {
                val bounded = output
                    .replace(UUID_TEXT, "<redacted>")
                    .map { character -> if (character.code in 0x20..0x7e) character else '?' }
                    .joinToString("")
                    .take(256)
                " content=$bounded"
            }
            throw InteropFailure("App E2E result did not match the bounded JSON schema ($byteCount bytes)$diagnostic")
        }
    }

    private fun validateAppE2eResult(result: AppE2eResult, invocation: AppE2eInvocation) {
        val invocationPinValid = if (invocation.suite == APP_E2E_SUITE_NETWORK) {
            invocation.tlsServerCertificateSha256?.matches(APP_E2E_TLS_CERTIFICATE_PIN) == true
        } else {
            invocation.tlsServerCertificateSha256 == null
        }
        if (!invocationPinValid) {
            throw InteropFailure("App E2E invocation TLS certificate pin was invalid")
        }
        val expectedChecks = appE2eChecksForBackend(when {
            invocation.suite == APP_E2E_SUITE_CORE -> APP_E2E_CORE_EXPECTED_CHECKS
            invocation.suite == APP_E2E_SUITE_LIFECYCLE -> APP_E2E_LIFECYCLE_EXPECTED_CHECKS
            invocation.suite == APP_E2E_SUITE_NETWORK -> APP_E2E_NETWORK_EXPECTED_CHECKS
            invocation.suite == APP_E2E_SUITE_PROCESS && invocation.phase == APP_E2E_PHASE_PROCESS_PREPARE ->
                APP_E2E_PROCESS_PREPARE_EXPECTED_CHECKS
            invocation.suite == APP_E2E_SUITE_PROCESS && invocation.phase == APP_E2E_PHASE_PROCESS_RECOVER ->
                APP_E2E_PROCESS_RECOVER_EXPECTED_CHECKS
            else -> throw InteropFailure("App E2E invocation suite or phase was invalid")
        }, invocation.backend)
        val processResultValid = if (invocation.suite == APP_E2E_SUITE_PROCESS) {
            result.recoveredGeneration == null && result.recoveredControlEpoch == null &&
                (result.processId ?: 0) > 0
        } else {
            isValidAppE2eRecoveryIdentity(
                backend = invocation.backend,
                initialGeneration = result.initialGeneration,
                initialControlEpoch = result.initialControlEpoch,
                recoveredGeneration = result.recoveredGeneration,
                recoveredControlEpoch = result.recoveredControlEpoch,
            )
        }
        val expectedTopology = if (invocation.suite == APP_E2E_SUITE_NETWORK) {
            TOPOLOGY_EXTERNAL
        } else {
            TOPOLOGY_ADB_REVERSE
        }
        val expectedNetworkSwitchProof = invocation.suite == APP_E2E_SUITE_NETWORK
        val expectedWorkload = if (invocation.workload.profile == APP_E2E_PROFILE_FUNCTIONAL) {
            result.workload == null
        } else {
            val actual = result.workload
            actual != null && actual.profile == invocation.workload.profile &&
                actual.completedIterations in 1..invocation.workload.iterations &&
                (invocation.workload.profile == APP_E2E_PROFILE_SOAK ||
                    actual.completedIterations == invocation.workload.iterations) &&
                actual.reconnectCount == if (invocation.workload.reconnectEveryIterations == 0) {
                    0
                } else {
                    actual.completedIterations / invocation.workload.reconnectEveryIterations
                }
        }
        if (result.schemaVersion != APP_E2E_SCHEMA_VERSION || result.runId != invocation.runId ||
            result.backend != invocation.backend ||
            result.status != STATUS_PASSED || result.checks != expectedChecks ||
            result.bindPort !in 1..65_535 || (result.initialGeneration ?: 0L) <= 0L ||
            (result.initialControlEpoch ?: 0L) <= 0L ||
            !processResultValid ||
            result.suite != invocation.suite || result.topology != expectedTopology ||
            result.networkSwitchProven != expectedNetworkSwitchProof || result.phase != invocation.phase ||
            result.failedStage != null || result.failureCode != null ||
            result.runtimeInitialization?.let(::isValidAppE2eRuntimeInitialization) == false ||
            result.resources?.let {
                isValidAppE2eResourceEvidence(it, invocation.workload, result.workload)
            } != true || !expectedWorkload
        ) {
            val stage = result.failedStage?.takeIf(SAFE_RESULT_VALUE::matches) ?: "none"
            val code = result.failureCode?.takeIf(SAFE_RESULT_VALUE::matches) ?: "none"
            val status = result.status.takeIf(SAFE_RESULT_VALUE::matches) ?: "invalid"
            throw InteropFailure(
                "App E2E returned an invalid semantic result: " +
                    "status=$status stage=$stage code=$code checkCount=${result.checks.size}",
            )
        }
    }

    private fun appE2eChecksForBackend(checks: List<String>, backend: String): List<String> {
        val backendCheck = when (backend) {
            BACKEND_KOTLIN -> APP_E2E_KOTLIN_BACKEND_CHECK
            BACKEND_GO_MOBILE -> APP_E2E_GO_MOBILE_BACKEND_CHECK
            else -> throw InteropFailure("App E2E invocation backend was invalid")
        }
        return checks.map { check ->
            when {
                check == APP_E2E_KOTLIN_BACKEND_CHECK -> backendCheck
                backend == BACKEND_GO_MOBILE && check == APP_E2E_CONTROL_EPOCH_ADVANCED_CHECK ->
                    APP_E2E_TRANSPORT_IDENTITY_ADVANCED_CHECK
                else -> check
            }
        }
    }

    private fun writeAppE2eSummary(path: Path, summary: AppE2eSummary) {
        val target = path.toAbsolutePath().normalize()
        val parent = target.parent ?: throw InteropFailure("App E2E summary path was invalid")
        if (!Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(parent) ||
            Files.exists(target, LinkOption.NOFOLLOW_LINKS)
        ) {
            throw InteropFailure("App E2E summary destination was invalid")
        }
        val bytes = ANDROID_INTEROP_JSON.encodeToString(summary).toByteArray(StandardCharsets.UTF_8)
        if (bytes.size !in 1..MAXIMUM_SUMMARY_BYTES) {
            bytes.fill(0)
            throw InteropFailure("App E2E summary exceeded its bounded schema")
        }
        val temporary = parent.resolve(".${target.fileName}.${UUID.randomUUID()}.tmp")
        try {
            writePrivateFile(temporary, bytes)
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE)
            if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(target) ||
                Files.size(target) !in 1..MAXIMUM_SUMMARY_BYTES
            ) {
                throw InteropFailure("App E2E summary was not published safely")
            }
        } catch (failure: InteropFailure) {
            throw failure
        } catch (_: Exception) {
            throw InteropFailure("App E2E summary could not be published atomically")
        } finally {
            bytes.fill(0)
            Files.deleteIfExists(temporary)
        }
    }

    private fun runInstrumentation(
        adb: AdbCommandRunner,
        invocation: AndroidInteropInvocation,
        files: AndroidInteropPrivateFiles,
    ): AdbCommandResult = adb.run(
        label = "${invocation.backend} instrumentation",
        arguments = listOf(
            "shell",
            "am",
            "instrument",
            "-w",
            "-r",
            "-e",
            "class",
            "$TEST_CLASS#$TEST_METHOD",
            "-e",
            "frpcAbEnabled",
            "true",
            "-e",
            "runId",
            invocation.runId,
            "-e",
            "backend",
            invocation.backend,
            "-e",
            "configFile",
            files.configFile,
            "-e",
            "resultFile",
            files.resultFile,
            "$TEST_PACKAGE/$TEST_RUNNER",
        ),
        timeoutMillis = INSTRUMENTATION_TIMEOUT_MILLIS,
        requireSuccess = false,
    )

    private fun runRestartInstrumentation(
        resources: AndroidInteropResourceOwner,
        adb: AdbCommandRunner,
        invocation: AndroidInteropInvocation,
        files: AndroidInteropPrivateFiles,
        frpsPort: Int,
        frpsLaunch: AndroidInteropProcessLaunch,
        provider: ManagedInteropProcess,
    ): AdbCommandResult {
        val markers = invocation.restartMarkers
            ?: throw InteropFailure("restart Android scenario did not configure markers")
        val executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "frpc-android-interop-instrumentation").apply { isDaemon = true }
        }
        val asyncFailure = AtomicReference<Throwable?>(null)
        val future = try {
            executor.submit<AdbCommandResult> {
                try {
                    runInstrumentation(adb, invocation, files)
                } catch (failure: Throwable) {
                    asyncFailure.compareAndSet(null, failure)
                    throw failure
                }
            }
        } catch (failure: Throwable) {
            executor.shutdownNow()
            throw failure
        }
        val deadline = InteropDeadline.afterMillis(INSTRUMENTATION_TIMEOUT_MILLIS)
        var result: AdbCommandResult? = null
        var selectedFailure: Throwable? = null
        try {
            stage = "${invocation.caseId} ${invocation.backend} restart device ready"
            awaitRestartMarker(
                adb = adb,
                fileName = markers.deviceReadyFile,
                expected = restartMarker(invocation, MARKER_DEVICE_READY),
                instrumentation = future,
            )
            val providerReadyRevision = provider.outputRevision()

            stage = "${invocation.caseId} ${invocation.backend} frps stop"
            resources.stopFrps()
            writeRestartMarker(
                adb,
                invocation.backend,
                markers.frpsStoppedFile,
                restartMarker(invocation, MARKER_FRPS_STOPPED),
            )

            stage = "${invocation.caseId} ${invocation.backend} device disconnect"
            awaitRestartMarker(
                adb = adb,
                fileName = markers.deviceDisconnectedFile,
                expected = restartMarker(invocation, MARKER_DEVICE_DISCONNECTED),
                instrumentation = future,
            )

            stage = "${invocation.caseId} ${invocation.backend} frps restart"
            val restarted = resources.startFrps(frpsLaunch::start)
            awaitFrpsReady(frpsPort, restarted)
            provider.awaitOutputAfter(
                PROVIDER_PROXY_READY_PATTERN,
                providerReadyRevision,
                PROVIDER_RECONNECT_TIMEOUT_MILLIS,
                stage,
            )
            writeRestartMarker(
                adb,
                invocation.backend,
                markers.frpsRestartedFile,
                restartMarker(invocation, MARKER_FRPS_RESTARTED),
            )

            stage = "${invocation.caseId} ${invocation.backend} instrumentation completion"
            result = awaitInstrumentation(future, deadline.remainingMillis())
        } catch (failure: Throwable) {
            selectedFailure = failure
        }
        if (selectedFailure != null) {
            stopAsyncInstrumentation(resources, future)?.let { cleanupFailure ->
                selectedFailure = preferFailure(selectedFailure, cleanupFailure)
            }
        }
        shutdownInstrumentationExecutor(executor, future, selectedFailure != null)?.let { cleanupFailure ->
            selectedFailure = preferFailure(selectedFailure, cleanupFailure)
        }
        asyncFailure.get()?.let { failure ->
            selectedFailure = preferFailure(selectedFailure, failure)
        }
        selectedFailure?.let { throw it }
        return result ?: throw InteropFailure("restart instrumentation did not return a result")
    }

    private fun awaitRestartMarker(
        adb: AdbCommandRunner,
        fileName: String,
        expected: AndroidInteropRestartMarker,
        instrumentation: Future<AdbCommandResult>,
    ) {
        awaitAndroidInteropRestartMarker(
            expected = expected,
            readMarker = {
                adb.run(
                    label = "restart marker read",
                    arguments = listOf("exec-out", "run-as", TEST_PACKAGE, "cat", "files/$fileName"),
                    timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                    requireSuccess = false,
                )
            },
            instrumentationIsDone = { instrumentation.isDone },
            observeInstrumentation = {
                awaitInstrumentation(instrumentation, ASYNC_OBSERVE_TIMEOUT_MILLIS)
            },
            timeoutMillis = MARKER_WAIT_TIMEOUT_MILLIS,
            pollMillis = MARKER_POLL_MILLIS,
        )
    }

    private fun writeRestartMarker(
        adb: AdbCommandRunner,
        backend: String,
        fileName: String,
        marker: AndroidInteropRestartMarker,
    ) {
        val bytes = ANDROID_INTEROP_JSON.encodeToString(marker).toByteArray(StandardCharsets.UTF_8)
        if (bytes.size !in 1..MAXIMUM_RESTART_MARKER_BYTES) {
            bytes.fill(0)
            throw InteropFailure("host restart marker exceeded its bounded schema")
        }
        val temporaryFile = temporaryPrivateFileName(fileName)
        try {
            writeDevicePrivateFile(adb, "$backend restart marker", temporaryFile, bytes)
            adb.run(
                label = "$backend restart marker publish",
                arguments = listOf(
                    "shell",
                    "run-as",
                    TEST_PACKAGE,
                    "mv",
                    "files/$temporaryFile",
                    "files/$fileName",
                ),
                timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
            )
        } finally {
            bytes.fill(0)
        }
    }

    private fun restartMarker(
        invocation: AndroidInteropInvocation,
        step: String,
    ): AndroidInteropRestartMarker = AndroidInteropRestartMarker(
        schemaVersion = RESTART_MARKER_SCHEMA_VERSION,
        runId = invocation.runId,
        caseId = invocation.caseId,
        backend = invocation.backend,
        step = step,
    )

    private fun writeDevicePrivateFile(
        adb: AdbCommandRunner,
        label: String,
        fileName: String,
        bytes: ByteArray,
    ) {
        if (!SAFE_PRIVATE_FILE.matches(fileName) || bytes.size !in 1..MAXIMUM_PRIVATE_FILE_BYTES) {
            throw InteropFailure("Android private file write was invalid")
        }
        adb.run(
            label = "$label directory preparation",
            arguments = listOf("shell", "run-as", TEST_PACKAGE, "mkdir", "-p", "files"),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        )
        adb.run(
            label = "$label write",
            arguments = listOf(
                "shell",
                "-T",
                "run-as",
                TEST_PACKAGE,
                "dd",
                "of=files/$fileName",
                "bs=1",
                "count=${bytes.size}",
            ),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
            standardInput = bytes,
        )
        adb.run(
            label = "$label permission",
            arguments = listOf("shell", "run-as", TEST_PACKAGE, "chmod", "600", "files/$fileName"),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        )
    }

    private fun readResult(
        adb: AdbCommandRunner,
        backend: String,
        resultFile: String,
    ): AndroidInteropResult {
        val output = adb.run(
            label = "$backend result read",
            arguments = listOf("exec-out", "run-as", TEST_PACKAGE, "cat", "files/$resultFile"),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        ).standardOutput
        val byteCount = output.toByteArray(StandardCharsets.UTF_8).size
        if (byteCount == 0) {
            throw InteropFailure("$backend returned an empty result")
        }
        if (byteCount > MAXIMUM_RESULT_BYTES) {
            throw InteropFailure("$backend result exceeded its bounded size")
        }
        return try {
            ANDROID_INTEROP_JSON.decodeFromString(output)
        } catch (_: Exception) {
            throw InteropFailure("$backend result did not match the bounded JSON schema ($byteCount bytes)")
        }
    }

    private fun validateResultEnvelope(
        result: AndroidInteropResult,
        invocation: AndroidInteropInvocation,
        profile: AndroidInteropProfile,
    ) {
        val failed = result.status == STATUS_FAILED
        if (result.schemaVersion != ANDROID_INTEROP_SCHEMA_VERSION ||
            result.runId != invocation.runId || result.backend != invocation.backend ||
            result.caseId != profile.caseId || result.scenario != profile.scenario ||
            result.wireProtocol != profile.wireProtocol ||
            result.useEncryption != profile.useEncryption ||
            result.useCompression != profile.useCompression ||
            result.status !in setOf(STATUS_PASSED, STATUS_FAILED) || result.bindPort !in 0..65_535 ||
            (failed && (result.failedStage == null || result.failureCode == null)) ||
            (!failed && (result.failedStage != null || result.failureCode != null)) ||
            (failed && result.semanticOutcome != null) ||
            (!failed && result.semanticOutcome !in ALL_SEMANTIC_OUTCOMES) ||
            (result.failedStage != null && !SAFE_RESULT_VALUE.matches(result.failedStage)) ||
            (result.failureCode != null && !SAFE_RESULT_VALUE.matches(result.failureCode)) ||
            !isOrderedSubset(result.checks, profile.expectedChecks)
        ) {
            throw InteropFailure("${invocation.backend} returned an invalid structural result")
        }
    }

    private fun isOrderedSubset(actual: List<String>, expected: List<String>): Boolean {
        if (actual.size > expected.size || actual.distinct().size != actual.size) return false
        var expectedIndex = 0
        actual.forEach { check ->
            val matchingIndex = expected.indexOfFirstFrom(expectedIndex) { it == check }
            if (matchingIndex < 0) return false
            expectedIndex = matchingIndex + 1
        }
        return true
    }

    private inline fun <T> List<T>.indexOfFirstFrom(startIndex: Int, predicate: (T) -> Boolean): Int {
        for (index in startIndex until size) {
            if (predicate(this[index])) return index
        }
        return -1
    }

    private fun requireInstrumentationSucceeded(result: AdbCommandResult, backend: String) {
        if (result.exitCode != 0 ||
            "INSTRUMENTATION_CODE: -1" !in result.standardOutput ||
            "FAILURES!!!" in result.standardOutput ||
            "INSTRUMENTATION_FAILED" in result.standardOutput
        ) {
            val lines = result.standardOutput.lineSequence().map(String::trim).toList()
            val exceptionType = lines.firstNotNullOfOrNull { line ->
                if (!line.startsWith("INSTRUMENTATION_STATUS: stack=")) return@firstNotNullOfOrNull null
                INSTRUMENTATION_EXCEPTION.find(line.substringAfter('='))?.value
            }
            val testLine = INSTRUMENTATION_TEST_LINE.find(result.standardOutput)?.value
            val diagnostic = lines.asSequence()
                .filter { line ->
                    line.startsWith("INSTRUMENTATION_STATUS: class=") ||
                        line.startsWith("INSTRUMENTATION_STATUS: test=") ||
                        line.startsWith("INSTRUMENTATION_STATUS: shortMsg=") ||
                        line.startsWith("INSTRUMENTATION_STATUS_CODE:") ||
                        line.startsWith("INSTRUMENTATION_RESULT: shortMsg=") ||
                        line.startsWith("INSTRUMENTATION_FAILED:") ||
                        line.startsWith("INSTRUMENTATION_CODE:")
                }
                .map { line -> line.replace(UUID_TEXT, "<redacted>") }
                .take(8)
                .joinToString(" | ")
                .take(512)
                .ifEmpty { "unavailable" }
            val exception = exceptionType?.let { " exception=$it" }.orEmpty()
            val location = testLine?.let { " location=$it" }.orEmpty()
            throw InteropFailure(
                "$backend instrumentation did not complete successfully:$exception$location $diagnostic",
            )
        }
    }

    private fun awaitInstrumentation(
        future: Future<AdbCommandResult>,
        timeoutMillis: Long,
    ): AdbCommandResult {
        if (timeoutMillis <= 0L) throw InteropFailure("Android instrumentation exceeded its deadline")
        return try {
            future.get(timeoutMillis, TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            throw InteropFailure("Android instrumentation exceeded its deadline")
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            throw InteropFailure("Android instrumentation wait was interrupted")
        } catch (failure: ExecutionException) {
            throw normalizedAsyncFailure(failure.cause)
        } catch (failure: CancellationException) {
            throw failure
        }
    }

    private fun stopAsyncInstrumentation(
        resources: AndroidInteropResourceOwner,
        future: Future<AdbCommandResult>,
    ): Throwable? {
        var selectedFailure: Throwable? = null
        try {
            resources.forceStopInstrumentation()
        } catch (failure: Throwable) {
            selectedFailure = failure
        }
        try {
            future.get(ASYNC_INSTRUMENTATION_STOP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            future.cancel(true)
        } catch (_: CancellationException) {
            // Cancellation is expected only after the host has force-stopped the instrumentation.
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            future.cancel(true)
            selectedFailure = preferFailure(
                selectedFailure,
                InteropFailure("Android instrumentation cleanup wait was interrupted"),
            )
        } catch (failure: ExecutionException) {
            selectedFailure = preferFailure(selectedFailure, normalizedAsyncFailure(failure.cause))
        }
        if (!future.isDone) future.cancel(true)
        return selectedFailure
    }

    private fun shutdownInstrumentationExecutor(
        executor: ExecutorService,
        future: Future<AdbCommandResult>,
        failed: Boolean,
    ): Throwable? {
        if (failed) {
            future.cancel(true)
            executor.shutdownNow()
        } else {
            executor.shutdown()
        }
        return try {
            if (executor.awaitTermination(ASYNC_EXECUTOR_STOP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                null
            } else {
                future.cancel(true)
                executor.shutdownNow()
                if (executor.awaitTermination(ASYNC_EXECUTOR_FORCE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    null
                } else {
                    InteropFailure("Android instrumentation executor did not terminate")
                }
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            future.cancel(true)
            executor.shutdownNow()
            InteropFailure("Android instrumentation executor cleanup was interrupted")
        }
    }

    private fun normalizedAsyncFailure(failure: Throwable?): Throwable = when (failure) {
        is Error -> failure
        is CancellationException -> failure
        is InteropFailure -> failure
        else -> InteropFailure("Android instrumentation failed unexpectedly")
    }

    private fun preferFailure(current: Throwable?, candidate: Throwable): Throwable = when {
        current == null -> candidate
        current is Error -> current
        candidate is Error -> candidate
        current is CancellationException -> current
        candidate is CancellationException -> candidate
        else -> current
    }

    private fun selectDevice(discovery: AdbCommandRunner, serialProperty: String): String {
        val output = discovery.run(
            label = "Android device discovery",
            arguments = listOf("devices", "-l"),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        ).standardOutput
        val authorized = output.lineSequence()
            .drop(1)
            .map(String::trim)
            .filter(String::isNotBlank)
            .mapNotNull { line ->
                val fields = line.split(Regex("\\s+"), limit = 3)
                fields.takeIf { it.size >= 2 && it[1] == "device" }?.first()
            }
            .toList()
        val requested = System.getProperty(serialProperty)?.takeIf(String::isNotBlank)
        if (requested != null) {
            if (!SAFE_DEVICE_SERIAL.matches(requested) || requested !in authorized) {
                throw InteropFailure("configured Android device was not uniquely authorized")
            }
            return requested
        }
        if (authorized.size != 1) {
            throw InteropFailure("exactly one authorized Android device is required")
        }
        return authorized.single()
    }

    private fun readDeviceMetadata(adb: AdbCommandRunner): AndroidDeviceMetadata {
        fun property(name: String): String = adb.run(
            label = "Android device metadata",
            arguments = listOf("shell", "getprop", name),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        ).standardOutput.trim()

        val api = property("ro.build.version.sdk").toIntOrNull()
            ?.takeIf { it >= 26 }
            ?: throw InteropFailure("Android device API level was unsupported")
        val abi = property("ro.product.cpu.abi").takeIf(ANDROID_INTEROP_SUPPORTED_ABIS::contains)
            ?: throw InteropFailure("Android device ABI was unsupported by the retained GoMobile bridge")
        // Android's x86_64 16 KiB test images expose the runtime ABI page size through sysconf,
        // while /proc/self/smaps can still report the underlying 4 KiB MMU page size.
        val getconfPageSize = adb.run(
            label = "Android device page size",
            arguments = listOf("shell", "getconf", "PAGE_SIZE"),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
            requireSuccess = false,
        )
        val pageSize = resolveAndroidRuntimePageSize(getconfPageSize) {
            adb.run(
                label = "Android legacy device page size",
                arguments = listOf("shell", "-T", "sh"),
                timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                standardInput = PAGE_SIZE_SHELL.toByteArray(StandardCharsets.UTF_8),
            ).standardOutput
        }
        return AndroidDeviceMetadata(api, abi, pageSize)
    }

    private fun requirePackageAbsent(adb: AdbCommandRunner) =
        requirePackageAbsent(adb, TEST_PACKAGE, "instrumentation package")

    private fun requirePackageAbsent(adb: AdbCommandRunner, packageName: String, label: String) {
        val result = adb.run(
            label = "$label preflight",
            arguments = listOf("shell", "pm", "path", packageName),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
            requireSuccess = false,
        )
        if (result.standardOutput.lineSequence().any { it.trim().startsWith("package:") }) {
            throw InteropFailure("$label was already installed; refusing to overwrite it")
        }
    }

    private fun requirePackageInstalled(adb: AdbCommandRunner) =
        requirePackageInstalled(adb, TEST_PACKAGE, "instrumentation package")

    private fun requirePackageInstalled(adb: AdbCommandRunner, packageName: String, label: String) {
        val result = adb.run(
            label = "$label verification",
            arguments = listOf("shell", "pm", "path", packageName),
            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
        )
        if (result.standardOutput.lineSequence().none { it.trim().startsWith("package:") }) {
            throw InteropFailure("$label verification did not find the expected package")
        }
    }

    private fun parseReversePort(output: String): Int {
        val normalized = output.trim().removePrefix("tcp:")
        return normalized.toIntOrNull()?.takeIf { it in 1..65_535 }
            ?: throw InteropFailure("adb reverse did not return a valid dynamic device port")
    }

    private fun configuredSuite(): String {
        val suite = System.getProperty(SUITE_PROPERTY)
            ?: throw InteropFailure("Android interoperability suite was not configured")
        androidInteropProfiles(suite)
        return suite
    }

    private fun configuredSummaryFile(): Path? {
        val configured = System.getProperty(SUMMARY_FILE_PROPERTY)?.takeIf(String::isNotBlank) ?: return null
        return try {
            Path.of(configured).toAbsolutePath().normalize()
        } catch (_: Exception) {
            throw InteropFailure("configured Android interoperability summary path was invalid")
        }
    }

    private fun configuredAppE2eSummaryFile(): Path? = configuredOptionalPath(
        APP_E2E_SUMMARY_FILE_PROPERTY,
        "App E2E summary",
    )

    private fun configuredAppE2eBackend(): AppE2eBackendSpec = when (
        System.getProperty(APP_E2E_BACKEND_PROPERTY)?.takeIf(String::isNotBlank) ?: BACKEND_KOTLIN
    ) {
        BACKEND_KOTLIN -> APP_E2E_KOTLIN_BACKEND
        BACKEND_GO_MOBILE -> APP_E2E_GO_MOBILE_BACKEND
        else -> throw InteropFailure("configured App E2E backend was invalid")
    }

    private fun configuredAppE2eWorkload(suite: String): AppE2eWorkload {
        return parseAppE2eWorkload(
            suite = suite,
            profileValue = System.getProperty(APP_E2E_PROFILE_PROPERTY),
            iterationsValue = System.getProperty(APP_E2E_ITERATIONS_PROPERTY),
            concurrencyValue = System.getProperty(APP_E2E_CONCURRENCY_PROPERTY),
            durationMillisValue = System.getProperty(APP_E2E_DURATION_MILLIS_PROPERTY),
            reconnectEveryIterationsValue = System.getProperty(APP_E2E_RECONNECT_EVERY_ITERATIONS_PROPERTY),
            sampleEveryIterationsValue = System.getProperty(APP_E2E_SAMPLE_EVERY_ITERATIONS_PROPERTY),
        )
    }

    private fun configuredAppE2eSuite(): String {
        val suite = System.getProperty(APP_E2E_SUITE_PROPERTY)
            ?.takeIf(String::isNotBlank)
            ?: APP_E2E_SUITE_CORE
        if (suite !in setOf(
                APP_E2E_SUITE_CORE,
                APP_E2E_SUITE_LIFECYCLE,
                APP_E2E_SUITE_PROCESS,
                APP_E2E_SUITE_NETWORK,
            )
        ) {
            throw InteropFailure("configured App E2E suite was invalid")
        }
        return suite
    }

    private fun configuredAppE2eOutageMillis(suite: String): Long {
        val configured = System.getProperty(APP_E2E_OUTAGE_MILLIS_PROPERTY)?.takeIf(String::isNotBlank)
        if (suite == APP_E2E_SUITE_NETWORK) {
            if (configured != null) throw InteropFailure("App E2E outage duration is not valid for the network suite")
            return 0L
        }
        val value = configured?.toLongOrNull() ?: DEFAULT_APP_E2E_OUTAGE_MILLIS
        if (value !in MINIMUM_APP_E2E_OUTAGE_MILLIS..MAXIMUM_APP_E2E_OUTAGE_MILLIS) {
            throw InteropFailure("configured App E2E outage duration was invalid")
        }
        return when (suite) {
            APP_E2E_SUITE_LIFECYCLE -> value
            else -> DEFAULT_APP_E2E_OUTAGE_MILLIS
        }
    }

    private fun configuredAppE2eRuntimeDiagnosticEnabled(): Boolean {
        val configured = System.getProperty(APP_E2E_RUNTIME_DIAGNOSTIC_ENABLED_PROPERTY)
            ?.takeIf(String::isNotBlank)
            ?: return false
        return when (configured) {
            "true" -> true
            "false" -> false
            else -> throw InteropFailure("configured App E2E runtime diagnostic flag was invalid")
        }
    }

    private fun configuredAppE2eExternalConfig(suite: String): AppE2eExternalConfig? {
        val path = configuredOptionalEnvironmentPath(
            APP_E2E_EXTERNAL_CONFIG_FILE_ENVIRONMENT,
            "App E2E external configuration",
        )
        if (suite == APP_E2E_SUITE_NETWORK) {
            return path?.let(::loadAppE2eExternalConfig)
                ?: throw InteropFailure("App E2E network suite requires an external configuration file")
        }
        if (path != null) {
            throw InteropFailure("App E2E external configuration is only valid for the network suite")
        }
        return null
    }

    private fun configuredOptionalEnvironmentPath(environment: String, label: String): Path? {
        val configured = System.getenv(environment)?.takeIf(String::isNotBlank) ?: return null
        return try {
            Path.of(configured).toAbsolutePath().normalize()
        } catch (_: Exception) {
            throw InteropFailure("configured $label path was invalid")
        }
    }

    private fun configuredOptionalPath(property: String, label: String): Path? {
        val configured = System.getProperty(property)?.takeIf(String::isNotBlank) ?: return null
        return try {
            Path.of(configured).toAbsolutePath().normalize()
        } catch (_: Exception) {
            throw InteropFailure("configured $label path was invalid")
        }
    }

    private fun configuredAdbExecutable(): Path = try {
        System.getProperty(ADB_PROPERTY)
            ?.takeIf(String::isNotBlank)
            ?.let(Path::of)
            ?: throw InteropFailure("Gradle did not provide the Android SDK adb executable")
    } catch (failure: InteropFailure) {
        throw failure
    } catch (_: Exception) {
        throw InteropFailure("configured adb executable path was invalid")
    }

    private fun configuredAppE2eAdbExecutable(): Path = configuredExecutablePath(
        APP_E2E_ADB_PROPERTY,
        "Gradle did not provide the Android SDK adb executable for App E2E",
    )

    private fun configuredExecutablePath(property: String, missingMessage: String): Path = try {
        System.getProperty(property)
            ?.takeIf(String::isNotBlank)
            ?.let(Path::of)
            ?: throw InteropFailure(missingMessage)
    } catch (failure: InteropFailure) {
        throw failure
    } catch (_: Exception) {
        throw InteropFailure("configured executable path was invalid")
    }

    private fun configuredTestApk(): Path {
        val directory = try {
            System.getProperty(APK_DIRECTORY_PROPERTY)
                ?.takeIf(String::isNotBlank)
                ?.let(Path::of)
                ?.toAbsolutePath()
                ?.normalize()
                ?: throw InteropFailure("Gradle did not provide the Android test APK directory")
        } catch (failure: InteropFailure) {
            throw failure
        } catch (_: Exception) {
            throw InteropFailure("configured Android test APK directory was invalid")
        }
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(directory)) {
            throw InteropFailure("Android test APK directory was missing")
        }
        val candidates = ArrayList<Path>()
        Files.newDirectoryStream(directory, "*.apk").use { entries: DirectoryStream<Path> ->
            entries.forEach { path ->
                if (candidates.size >= MAXIMUM_APK_CANDIDATES) {
                    throw InteropFailure("Android test APK directory contained too many entries")
                }
                if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(path)) {
                    candidates.add(path.toAbsolutePath().normalize())
                }
            }
        }
        return candidates.singleOrNull { it.fileName.toString() == TEST_APK_FILE_NAME }
            ?: throw InteropFailure("the expected Android test APK was missing or ambiguous")
    }

    private fun configuredAppE2eTargetApk(abi: String): Path {
        val candidates = configuredApkCandidates(APP_E2E_APP_APK_DIRECTORY_PROPERTY, "App E2E target APK")
        return candidates.singleOrNull { path ->
            val name = path.fileName.toString()
            appE2eBackend.targetApkMarker in name && name.endsWith("_$abi.apk")
        } ?: throw InteropFailure("the ABI-matched App E2E target APK was missing or ambiguous")
    }

    private fun configuredAppE2eTestApk(): Path {
        val candidates = configuredApkCandidates(APP_E2E_TEST_APK_DIRECTORY_PROPERTY, "App E2E test APK")
        return candidates.singleOrNull { it.fileName.toString() == appE2eBackend.testApkFileName }
            ?: throw InteropFailure("the expected App E2E test APK was missing or ambiguous")
    }

    private fun configuredApkCandidates(property: String, label: String): List<Path> {
        val directory = try {
            System.getProperty(property)
                ?.takeIf(String::isNotBlank)
                ?.let(Path::of)
                ?.toAbsolutePath()
                ?.normalize()
                ?: throw InteropFailure("Gradle did not provide the $label directory")
        } catch (failure: InteropFailure) {
            throw failure
        } catch (_: Exception) {
            throw InteropFailure("configured $label directory was invalid")
        }
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(directory)) {
            throw InteropFailure("$label directory was missing")
        }
        val candidates = ArrayList<Path>()
        Files.newDirectoryStream(directory, "*.apk").use { entries: DirectoryStream<Path> ->
            entries.forEach { path ->
                if (candidates.size >= MAXIMUM_APK_CANDIDATES) {
                    throw InteropFailure("$label directory contained too many entries")
                }
                if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(path)) {
                    candidates.add(path.toAbsolutePath().normalize())
                }
            }
        }
        return candidates
    }

    private fun configuredCacheDirectory(): Path = try {
        val configured = System.getProperty(FRP_CACHE_PROPERTY)
            ?.takeIf(String::isNotBlank)
            ?: Path.of(
                System.getProperty("user.home"),
                ".gradle",
                "caches",
                "ocdeck",
                "frp-interop",
                "v0.69.1",
            ).toString()
        Path.of(configured).toAbsolutePath().normalize()
    } catch (_: Exception) {
        throw InteropFailure("frp interoperability cache configuration was invalid")
    }

    private fun allocateLoopbackPort(): Int = try {
        ServerSocket().use { socket ->
            socket.reuseAddress = false
            socket.bind(InetSocketAddress(IPV4_LOOPBACK, 0))
            socket.localPort.takeIf { it in 1..65_535 }
                ?: throw InteropFailure("host loopback port allocation failed")
        }
    } catch (failure: InteropFailure) {
        throw failure
    } catch (_: Exception) {
        throw InteropFailure("host loopback port allocation failed")
    }

    private fun awaitFrpsReady(port: Int, process: ManagedInteropProcess) {
        process.awaitOutput(FRPS_READY_PATTERN, PROCESS_READY_TIMEOUT_MILLIS, stage)
        val deadline = InteropDeadline.afterMillis(PROCESS_READY_TIMEOUT_MILLIS)
        while (!deadline.isExpired()) {
            process.requireAlive(stage)
            val connected = try {
                Socket().use { socket ->
                    socket.connect(InetSocketAddress(IPV4_LOOPBACK, port), PORT_PROBE_TIMEOUT_MILLIS)
                }
                true
            } catch (_: Exception) {
                false
            }
            if (connected) return
            sleepBounded(minOf(25L, deadline.remainingMillis()))
        }
        throw InteropFailure("frps did not bind its loopback port within the deadline")
    }

    private fun frpsToml(port: Int, authToken: String, tls: InteropTlsMaterial): String = """
        bindAddr = "127.0.0.1"
        bindPort = $port
        proxyBindAddr = "127.0.0.1"
        detailedErrorsToClient = false

        log.to = "console"
        log.level = "info"
        log.maxDays = 1
        log.disablePrintColor = true

        transport.tcpMux = true
        transport.tls.force = true
        transport.tls.certFile = "${tls.certificatePath.fileName}"
        transport.tls.keyFile = "${tls.privateKeyPath.fileName}"

        auth.method = "token"
        auth.additionalScopes = ["HeartBeats", "NewWorkConns"]
        auth.token = "${tomlValue(authToken)}"
    """.trimIndent() + "\n"

    private fun providerToml(
        frpsPort: Int,
        syntheticPort: Int,
        authToken: String,
        stcpSecret: String,
        frpsHost: String = "127.0.0.1",
        trustedCaFile: Path? = null,
    ): String {
        val externalTls = trustedCaFile?.let { path ->
            """
            transport.tls.trustedCaFile = "${path.fileName}"
            transport.tls.serverName = "${tomlHost(frpsHost)}"
            """.trimIndent()
        }.orEmpty()
        return """
        user = "$PROVIDER_USER"
        serverAddr = "${tomlHost(frpsHost)}"
        serverPort = $frpsPort
        loginFailExit = true

        log.to = "console"
        log.level = "info"
        log.maxDays = 1
        log.disablePrintColor = true

        auth.method = "token"
        auth.additionalScopes = ["HeartBeats", "NewWorkConns"]
        auth.token = "${tomlValue(authToken)}"

        transport.protocol = "tcp"
        transport.wireProtocol = "v2"
        transport.tcpMux = true
        transport.tls.enable = true
        transport.tls.disableCustomTLSFirstByte = true
        $externalTls

        [[proxies]]
        name = "$PROVIDER_PROXY_NAME"
        type = "stcp"
        secretKey = "${tomlValue(stcpSecret)}"
        localIP = "127.0.0.1"
        localPort = $syntheticPort
        allowUsers = ["$VISITOR_USER"]
        transport.useEncryption = false
        transport.useCompression = false
        """.trimIndent() + "\n"
    }

    private fun tomlValue(value: String): String = value.takeIf(SAFE_TOML_VALUE::matches)
        ?: throw InteropFailure("runtime credential generation was invalid")

    private fun tomlHost(value: String): String = value.takeIf(SAFE_TOML_HOST::matches)
        ?: throw InteropFailure("frps host configuration was invalid")

    private fun safeFailure(
        currentStage: String,
        failure: Exception,
        resources: AndroidInteropResourceOwner,
    ): InteropFailure {
        val summary = when (failure) {
            is InteropFailure -> "$currentStage: ${redactor.redact(failure.message.orEmpty())}"
            else -> "$currentStage failed with ${failure.javaClass.simpleName}"
        }
        val diagnostics = resources.processDiagnostics()
        return InteropFailure(
            if (diagnostics.isBlank()) summary else "$summary\n${redactor.redact(diagnostics)}",
        )
    }

    private inner class AndroidInteropResourceOwner(
        private val testPackage: String = TEST_PACKAGE,
        private val managedPackages: List<String> = listOf(TEST_PACKAGE),
        private val appFixtureEnabled: Boolean = false,
        private val workspacePrefix: String = "ocdeck-frp-android-interop-",
    ) {
        private val lock = Any()
        private val cleanupComplete = CountDownLatch(1)
        private var cleanupStarted = false
        @Volatile
        private var cleanupFailure: Throwable? = null
        private var temporaryDirectory: Path? = null
        private var syntheticServer: SyntheticOpenCodeServer? = null
        private var frps: ManagedInteropProcess? = null
        private var provider: ManagedInteropProcess? = null
        private var adb: AdbCommandRunner? = null
        private val packageInstallAttempted = LinkedHashSet<String>()
        private val packageInstalled = LinkedHashSet<String>()
        private var reversePort: Int? = null
        private var reverseTargetPort: Int? = null
        private var reverseBaselinePorts = emptySet<Int>()
        private var networkBaseline: Pair<Boolean, Boolean>? = null
        private val privateFiles = LinkedHashSet<String>()

        fun createTemporaryWorkspace(): Path = synchronized(lock) {
            requireActive()
            Files.createTempDirectory(workspacePrefix).also { created ->
                try {
                    createPrivateDirectory(created)
                } catch (failure: Throwable) {
                    deleteTreeBestEffort(created)
                    throw failure
                }
                temporaryDirectory = created
            }
        }

        fun createSyntheticServer(): SyntheticOpenCodeServer = synchronized(lock) {
            requireActive()
            SyntheticOpenCodeServer(appFixtureEnabled = appFixtureEnabled).also { syntheticServer = it }
        }

        fun startFrps(factory: () -> ManagedInteropProcess): ManagedInteropProcess = synchronized(lock) {
            requireActive()
            if (frps != null) throw InteropFailure("frps was already registered")
            factory().also { frps = it }
        }

        fun stopFrps() = synchronized(lock) {
            requireActive()
            val current = frps ?: throw InteropFailure("frps was not running")
            current.stop()
            frps = null
        }

        fun requireFrpsAlive(currentStage: String) = synchronized(lock) {
            requireActive()
            frps?.requireAlive(currentStage) ?: throw InteropFailure("$currentStage: frps was not running")
        }

        fun startProvider(factory: () -> ManagedInteropProcess): ManagedInteropProcess = synchronized(lock) {
            requireActive()
            if (provider != null) throw InteropFailure("provider frpc was already registered")
            factory().also { provider = it }
        }

        fun stopProvider() = synchronized(lock) {
            requireActive()
            val current = provider ?: throw InteropFailure("provider frpc was not running")
            current.stop()
            provider = null
        }

        fun requireProviderAlive(currentStage: String) = synchronized(lock) {
            requireActive()
            provider?.requireAlive(currentStage)
                ?: throw InteropFailure("$currentStage: provider frpc was not running")
        }

        fun configureAdb(runner: AdbCommandRunner) = synchronized(lock) {
            requireActive()
            if (adb != null) throw InteropFailure("adb was already registered")
            adb = runner
        }

        fun registerNetworkBaseline(wifiEnabled: Boolean, mobileDataEnabled: Boolean) = synchronized(lock) {
            requireActive()
            if (networkBaseline != null) throw InteropFailure("Android network baseline was already registered")
            networkBaseline = wifiEnabled to mobileDataEnabled
        }

        fun markPackageInstallAttempted(packageName: String = testPackage) = synchronized(lock) {
            requireActive()
            if (packageName !in managedPackages || !packageInstallAttempted.add(packageName)) {
                throw InteropFailure("Android package installation ownership was invalid")
            }
        }

        fun markPackageInstalled(packageName: String = testPackage) = synchronized(lock) {
            requireActive()
            if (packageName !in packageInstallAttempted || !packageInstalled.add(packageName)) {
                throw InteropFailure("Android package installation state was invalid")
            }
        }

        fun registerReverseTarget(port: Int, baselinePorts: Set<Int>) = synchronized(lock) {
            requireActive()
            if (reverseTargetPort != null) throw InteropFailure("adb reverse target was already registered")
            reverseTargetPort = port
            reverseBaselinePorts = baselinePorts.toSet()
        }

        fun registerReverse(port: Int) = synchronized(lock) {
            requireActive()
            check(reverseTargetPort != null)
            if (reversePort != null) throw InteropFailure("adb reverse was already registered")
            reversePort = port
        }

        fun registerPrivateFiles(names: List<String>) = synchronized(lock) {
            requireActive()
            if (names.isEmpty() || names.distinct().size != names.size ||
                names.any { !SAFE_PRIVATE_PATH.matches(it) || it in privateFiles }
            ) {
                throw InteropFailure("private file names were invalid or not unique")
            }
            privateFiles.addAll(names)
        }

        fun forceStopInstrumentation() {
            val (runner, packages) = synchronized(lock) {
                requireActive()
                Pair(adb, managedPackages.asReversed().filter(packageInstalled::contains))
            }
            val configuredRunner = runner ?: throw InteropFailure("adb was not configured")
            packages.forEachIndexed { index, packageName ->
                configuredRunner.run(
                    label = "instrumentation force stop ${index + 1}",
                    arguments = listOf("shell", "am", "force-stop", packageName),
                    timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                )
            }
        }

        fun forceStopAndDeletePrivateFiles() {
            var selectedFailure: Throwable? = null
            try {
                forceStopInstrumentation()
            } catch (failure: Throwable) {
                selectedFailure = failure
            }
            val runner = synchronized(lock) {
                requireActive()
                adb
            } ?: throw InteropFailure("adb was not configured")
            val files = synchronized(lock) { privateFiles.toList() }
            if (files.isNotEmpty()) {
                try {
                    runner.run(
                        label = "instrumentation private file cleanup",
                        arguments = listOf("shell", "run-as", testPackage, "rm", "-f") +
                            files.map { "files/$it" },
                        timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                    )
                    synchronized(lock) { privateFiles.removeAll(files.toSet()) }
                } catch (failure: Throwable) {
                    selectedFailure = preferFailure(selectedFailure, failure)
                }
            }
            selectedFailure?.let { throw it }
        }

        fun processDiagnostics(): String = synchronized(lock) {
            listOfNotNull(
                frps?.diagnosticSummary()?.takeIf(String::isNotBlank),
                provider?.diagnosticSummary()?.takeIf(String::isNotBlank),
            ).joinToString("\n")
        }

        fun cleanup(): Throwable? {
            val owner = synchronized(lock) {
                if (cleanupStarted) false else {
                    cleanupStarted = true
                    true
                }
            }
            if (!owner) return awaitCleanupCompletion()

            var failure: Throwable? = null
            try {
                val runner = synchronized(lock) { adb }
                if (runner != null) {
                    val installedPackages = synchronized(lock) {
                        managedPackages.asReversed().filter(packageInstalled::contains)
                    }
                    installedPackages.forEachIndexed { index, packageName ->
                        failure = cleanupStep(failure) {
                            runner.run(
                                label = "instrumentation cleanup force stop ${index + 1}",
                                arguments = listOf("shell", "am", "force-stop", packageName),
                                timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                            )
                        }
                    }
                    synchronized(lock) { networkBaseline }?.let { (wifiEnabled, mobileDataEnabled) ->
                        failure = cleanupStep(failure) { setWifiEnabled(runner, wifiEnabled) }
                        failure = cleanupStep(failure) { setMobileDataEnabled(runner, mobileDataEnabled) }
                        if (wifiEnabled) {
                            failure = cleanupStep(failure) {
                                awaitHostValidatedTransport(runner, HOST_TRANSPORT_WIFI)
                            }
                        }
                        failure = cleanupStep(failure) {
                            awaitToggleState(runner, "mobile_data", mobileDataEnabled)
                        }
                    }
                    val files = synchronized(lock) {
                        if (testPackage in packageInstalled) privateFiles.toList() else emptyList()
                    }
                    if (files.isNotEmpty()) {
                        failure = cleanupStep(failure) {
                            runner.run(
                                label = "instrumentation cleanup private files",
                                arguments = listOf("shell", "run-as", testPackage, "rm", "-f") +
                                    files.map { "files/$it" },
                                timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                            )
                        }
                    }
                }
                val (ownedReverse, ownedReverseTarget, baselineReversePorts) = synchronized(lock) {
                    Triple(reversePort, reverseTargetPort, reverseBaselinePorts)
                }
                if (runner != null && ownedReverse != null) {
                    failure = cleanupStep(failure) {
                        runner.run(
                            label = "adb reverse cleanup",
                            arguments = listOf("reverse", "--remove", "tcp:$ownedReverse"),
                            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                        )
                    }
                } else if (runner != null && ownedReverseTarget != null) {
                    failure = cleanupStep(failure) {
                        val listed = runner.run(
                            label = "adb reverse ownership recovery",
                            arguments = listOf("reverse", "--list"),
                            timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                        )
                        recoverOwnedReversePort(
                            listed.standardOutput,
                            ownedReverseTarget,
                            baselineReversePorts,
                        )?.let { port ->
                            runner.run(
                                label = "adb reverse cleanup",
                                arguments = listOf("reverse", "--remove", "tcp:$port"),
                                timeoutMillis = ADB_SHORT_TIMEOUT_MILLIS,
                            )
                        }
                    }
                }
                if (runner != null) {
                    managedPackages.asReversed().filter(packageInstallAttempted::contains).forEach { packageName ->
                        val label = if (packageName == testPackage) "instrumentation" else "target App"
                        failure = cleanupStep(failure) {
                            runner.run(
                                label = "$label APK uninstall",
                                arguments = listOf("uninstall", packageName),
                                timeoutMillis = APK_INSTALL_TIMEOUT_MILLIS,
                                requireSuccess = packageName in packageInstalled,
                            )
                        }
                    }
                }
                failure = cleanupStep(failure) { provider?.stop() }
                failure = cleanupStep(failure) { frps?.stop() }
                failure = cleanupStep(failure) { syntheticServer?.close() }
                failure = cleanupStep(failure) { deleteTreeChecked(temporaryDirectory) }
                return failure
            } finally {
                cleanupFailure = failure
                cleanupComplete.countDown()
            }
        }

        private fun cleanupStep(current: Throwable?, block: () -> Unit): Throwable? = try {
            block()
            current
        } catch (candidate: Throwable) {
            when {
                current == null -> candidate
                current is Error -> current
                candidate is Error -> candidate
                current is CancellationException -> current
                candidate is CancellationException -> candidate
                else -> current
            }
        }

        private fun awaitCleanupCompletion(): Throwable? = try {
            if (!cleanupComplete.await(RESOURCE_CLEANUP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                InteropFailure("Android interoperability cleanup did not complete within its deadline")
            } else {
                cleanupFailure
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            InteropFailure("Android interoperability cleanup wait was interrupted")
        }

        private fun requireActive() {
            if (cleanupStarted) throw InteropFailure("Android interoperability cleanup had already started")
        }
    }

    private data class AndroidDeviceMetadata(
        val apiLevel: Int,
        val abi: String,
        val pageSize: Int,
    )

    private data class AndroidInteropProcessLaunch(
        val label: String,
        val executable: Path,
        val arguments: List<String>,
        val workingDirectory: Path,
        val redactor: InteropRedactor,
    ) {
        fun start(): ManagedInteropProcess = ManagedInteropProcess.start(
            label = label,
            executable = executable,
            arguments = arguments,
            workingDirectory = workingDirectory,
            redactor = redactor,
        )

        override fun toString(): String = "AndroidInteropProcessLaunch(label=$label)"
    }

    private data class AndroidInteropPrivateFiles(
        val configFile: String,
        val resultFile: String,
        val temporaryResultFile: String,
        val restartMarkers: AndroidInteropRestartMarkers?,
        val allNames: List<String>,
    ) {
        override fun toString(): String =
            "AndroidInteropPrivateFiles(count=${allNames.size}, restart=${restartMarkers != null})"

        companion object {
            fun create(
                profile: AndroidInteropProfile,
                backend: String,
                runId: String,
            ): AndroidInteropPrivateFiles {
                if (backend !in setOf(BACKEND_GO_MOBILE, BACKEND_KOTLIN) || !SAFE_RUN_ID.matches(runId)) {
                    throw InteropFailure("Android private file identity was invalid")
                }
                val stem = "frpc-ab-${profile.caseId}-$backend-$runId"
                val configFile = "$stem-config.json"
                val resultFile = "$stem-result.json"
                val temporaryResultFile = temporaryPrivateFileName(resultFile)
                val restartMarkers = if (profile.isRestart) {
                    AndroidInteropRestartMarkers(
                        deviceReadyFile = "$stem-device-ready.marker",
                        frpsStoppedFile = "$stem-frps-stopped.marker",
                        deviceDisconnectedFile = "$stem-device-disconnected.marker",
                        frpsRestartedFile = "$stem-frps-restarted.marker",
                    )
                } else {
                    null
                }
                val names = buildList {
                    add(configFile)
                    add(resultFile)
                    add(temporaryResultFile)
                    restartMarkers?.fileNames?.forEach { marker ->
                        add(marker)
                        add(temporaryPrivateFileName(marker))
                    }
                }
                if (names.distinct().size != names.size || names.any { !SAFE_PRIVATE_FILE.matches(it) }) {
                    throw InteropFailure("Android private file names were invalid or not unique")
                }
                return AndroidInteropPrivateFiles(
                    configFile = configFile,
                    resultFile = resultFile,
                    temporaryResultFile = temporaryResultFile,
                    restartMarkers = restartMarkers,
                    allNames = names,
                )
            }
        }
    }

    private data class AppE2ePrivateFiles(
        val configFile: String,
        val restartMarkers: AppE2eRestartMarkers,
        val allRelativePaths: List<String>,
    ) {
        override fun toString(): String = "AppE2ePrivateFiles(count=${allRelativePaths.size})"

        companion object {
            fun create(runId: String): AppE2ePrivateFiles {
                if (!SAFE_RUN_ID.matches(runId)) throw InteropFailure("App E2E private file identity was invalid")
                val configFile = "app-e2e-$runId-config.json"
                val restartMarkers = AppE2eRestartMarkers(
                    deviceReadyFile = "app-e2e-$runId-device-ready.json",
                    deviceDisconnectedFile = "app-e2e-$runId-device-disconnected.json",
                    frpsOnlyFile = "app-e2e-$runId-frps-only.json",
                    providerWindowObservedFile = "app-e2e-$runId-provider-window.json",
                    frpsRestartedFile = "app-e2e-$runId-frps-restarted.json",
                    wifiDisconnectedFile = "app-e2e-$runId-wifi-disconnected.json",
                    wifiRetentionObservedFile = "app-e2e-$runId-wifi-retention.json",
                    wifiRecoveredFile = "app-e2e-$runId-wifi-recovered.json",
                    wifiRecoveryObservedFile = "app-e2e-$runId-wifi-recovery.json",
                )
                val names = listOf(configFile) + restartMarkers.allFiles
                if (names.any { !APP_E2E_SAFE_FILE.matches(it) }) {
                    throw InteropFailure("App E2E private file names were invalid")
                }
                return AppE2ePrivateFiles(
                    configFile = configFile,
                    restartMarkers = restartMarkers,
                    allRelativePaths = names,
                )
            }
        }
    }

    private data class AndroidInteropSecrets(
        val authToken: String,
        val stcpSecret: String,
        val wrongAuthToken: String,
        val wrongStcpSecret: String,
    ) {
        override fun toString(): String =
            "AndroidInteropSecrets(authToken=<redacted>, stcpSecret=<redacted>, " +
                "wrongAuthToken=<redacted>, wrongStcpSecret=<redacted>)"

        companion object {
            fun create(random: SecureRandom): AndroidInteropSecrets = AndroidInteropSecrets(
                authToken = randomValue(random, "auth"),
                stcpSecret = randomValue(random, "stcp"),
                wrongAuthToken = randomValue(random, "wrong-auth"),
                wrongStcpSecret = randomValue(random, "wrong-stcp"),
            )

            private fun randomValue(random: SecureRandom, prefix: String): String {
                val bytes = ByteArray(32)
                random.nextBytes(bytes)
                return try {
                    "$prefix-${Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)}"
                } finally {
                    bytes.fill(0)
                }
            }
        }
    }

    private companion object {
        val IPV4_LOOPBACK: InetAddress = InetAddress.getByAddress(byteArrayOf(127, 0, 0, 1))
        val SAFE_TOML_VALUE = Regex("[A-Za-z0-9_-]{16,256}")
        val SAFE_TOML_HOST = Regex("[A-Za-z0-9.:-]{1,253}")
        val APP_E2E_TLS_CERTIFICATE_PIN = Regex("[0-9a-f]{64}")
        val SAFE_DEVICE_SERIAL = Regex("[A-Za-z0-9._:-]{1,128}")
        val SAFE_PRIVATE_FILE = Regex("[A-Za-z0-9_-]{1,112}\\.(json|marker)")
        val APP_E2E_SAFE_FILE = Regex("[a-z0-9][a-z0-9._-]{0,127}\\.json")
        val SAFE_PRIVATE_PATH = Regex("(?:[A-Za-z0-9_-]{1,112}\\.(?:json|marker)|app-e2e/[a-z0-9][a-z0-9._-]{0,127}\\.json)")
        val SAFE_RESULT_VALUE = Regex("[A-Za-z0-9_]{1,96}")
        val UUID_TEXT = Regex("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}")
        val INSTRUMENTATION_EXCEPTION = Regex("[A-Za-z_$][A-Za-z0-9_.$]*(?:Exception|Error)")
        val INSTRUMENTATION_TEST_LINE = Regex("ReleaseLikeStcpAppE2eTest\\.kt:[0-9]{1,5}")
        val FRPS_READY_PATTERN = Regex("(?m)frps started successfully")
        val PROVIDER_PROXY_READY_PATTERN = Regex("(?m)\\[$PROVIDER_PROXY_NAME] start proxy success")
        const val TEST_PACKAGE = "io.github.ycfeng.ocdeck.frpcstcpvisitor.test"
        const val TEST_APK_FILE_NAME = "frpc-stcp-visitor-debug-androidTest.apk"
        const val TEST_RUNNER = "androidx.test.runner.AndroidJUnitRunner"
        const val TEST_CLASS =
            "io.github.ycfeng.ocdeck.frpcstcpvisitor.interop.FrpcStcpVisitorAndroidInteropTest"
        const val TEST_METHOD = "runConfiguredBackend"
        const val APP_MAIN_ACTIVITY_CLASS = "io.github.ycfeng.ocdeck.MainActivity"
        const val APP_E2E_TEST_CLASS = "io.github.ycfeng.ocdeck.interop.ReleaseLikeStcpAppE2eTest"
        const val APP_E2E_TEST_METHOD = "runConfiguredAppFlow"
        const val APP_E2E_RESULT_STATUS_KEY = "ocdeckAppE2eResult"
        const val APP_E2E_SCHEMA_VERSION = 12
        const val APP_E2E_SUMMARY_SCHEMA_VERSION = 10
        const val BACKEND_GO_MOBILE = "gomobile"
        const val BACKEND_KOTLIN = "kotlin"
        const val APP_E2E_KOTLIN_BACKEND_CHECK = "kotlin_backend"
        const val APP_E2E_GO_MOBILE_BACKEND_CHECK = "gomobile_backend"
        const val APP_E2E_CONTROL_EPOCH_ADVANCED_CHECK = "control_epoch_advanced"
        const val APP_E2E_TRANSPORT_IDENTITY_ADVANCED_CHECK = "transport_identity_advanced"
        val APP_E2E_KOTLIN_BACKEND = AppE2eBackendSpec(
            backend = BACKEND_KOTLIN,
            targetPackage = "io.github.ycfeng.ocdeck.kotlinrelease",
            testPackage = "io.github.ycfeng.ocdeck.kotlinrelease.test",
            targetApkMarker = "-kotlin-release_",
            testApkFileName = "app-kotlinRelease-androidTest.apk",
        )
        val APP_E2E_GO_MOBILE_BACKEND = AppE2eBackendSpec(
            backend = BACKEND_GO_MOBILE,
            targetPackage = "io.github.ycfeng.ocdeck.gomobilerelease",
            testPackage = "io.github.ycfeng.ocdeck.gomobilerelease.test",
            targetApkMarker = "-gomobile-release_",
            testApkFileName = "app-goMobileRelease-androidTest.apk",
        )
        const val APP_E2E_SUITE_CORE = "core"
        const val APP_E2E_SUITE_LIFECYCLE = "lifecycle"
        const val APP_E2E_SUITE_PROCESS = "process"
        const val APP_E2E_SUITE_NETWORK = "network"
        const val APP_E2E_PHASE_SINGLE = "single"
        const val APP_E2E_PHASE_PROCESS_PREPARE = "process_prepare"
        const val APP_E2E_PHASE_PROCESS_RECOVER = "process_recover"
        const val TOPOLOGY_ADB_REVERSE = "adb_reverse"
        const val TOPOLOGY_EXTERNAL = "external"
        const val IPV4_LOOPBACK_TEXT = "127.0.0.1"
        const val PROVIDER_USER = "interop-server"
        const val VISITOR_USER = "interop-visitor"
        const val PROVIDER_PROXY_NAME = "interop-opencode"
        const val ADB_PROPERTY = "ocdeck.frp.androidInterop.adb"
        const val APK_DIRECTORY_PROPERTY = "ocdeck.frp.androidInterop.apkDirectory"
        const val DEVICE_SERIAL_PROPERTY = "ocdeck.frp.androidInterop.deviceSerial"
        const val SUITE_PROPERTY = "ocdeck.frp.androidInterop.suite"
        const val SUMMARY_FILE_PROPERTY = "ocdeck.frp.androidInterop.summaryFile"
        const val APP_E2E_ADB_PROPERTY = "ocdeck.frp.appE2e.adb"
        const val APP_E2E_BACKEND_PROPERTY = "ocdeck.frp.appE2e.backend"
        const val APP_E2E_PROFILE_PROPERTY = "ocdeck.frp.appE2e.profile"
        const val APP_E2E_ITERATIONS_PROPERTY = "ocdeck.frp.appE2e.iterations"
        const val APP_E2E_CONCURRENCY_PROPERTY = "ocdeck.frp.appE2e.concurrency"
        const val APP_E2E_DURATION_MILLIS_PROPERTY = "ocdeck.frp.appE2e.durationMillis"
        const val APP_E2E_RECONNECT_EVERY_ITERATIONS_PROPERTY = "ocdeck.frp.appE2e.reconnectEveryIterations"
        const val APP_E2E_SAMPLE_EVERY_ITERATIONS_PROPERTY = "ocdeck.frp.appE2e.sampleEveryIterations"
        const val APP_E2E_APP_APK_DIRECTORY_PROPERTY = "ocdeck.frp.appE2e.appApkDirectory"
        const val APP_E2E_TEST_APK_DIRECTORY_PROPERTY = "ocdeck.frp.appE2e.testApkDirectory"
        const val APP_E2E_DEVICE_SERIAL_PROPERTY = "ocdeck.frp.appE2e.deviceSerial"
        const val APP_E2E_SUMMARY_FILE_PROPERTY = "ocdeck.frp.appE2e.summaryFile"
        const val APP_E2E_SUITE_PROPERTY = "ocdeck.frp.appE2e.suite"
        const val APP_E2E_OUTAGE_MILLIS_PROPERTY = "ocdeck.frp.appE2e.outageMillis"
        const val APP_E2E_RUNTIME_DIAGNOSTIC_ENABLED_PROPERTY = "ocdeck.frp.appE2e.runtimeDiagnostic.enabled"
        const val APP_E2E_EXTERNAL_CONFIG_FILE_ENVIRONMENT = "OCDECK_FRP_APP_E2E_EXTERNAL_CONFIG_FILE"
        const val FRP_CACHE_PROPERTY = "ocdeck.frp.interop.cacheDir"
        const val MARKER_DEVICE_READY = "device_ready"
        const val MARKER_FRPS_STOPPED = "frps_stopped"
        const val MARKER_DEVICE_DISCONNECTED = "device_disconnected"
        const val MARKER_FRPS_ONLY = "frps_only"
        const val MARKER_FRPS_RESTARTED = "frps_restarted"
        const val MARKER_NETWORK_CELLULAR_DISCONNECTED = "network_cellular_disconnected"
        const val MARKER_NETWORK_CELLULAR_RECOVERED = "network_cellular_recovered"
        const val MARKER_NETWORK_WIFI_DISCONNECTED = "network_wifi_disconnected"
        const val MARKER_NETWORK_WIFI_RECOVERED = "network_wifi_recovered"
        const val MAXIMUM_APK_CANDIDATES = 8
        const val CONCURRENT_TRAFFIC_REQUESTS = 5
        const val MAXIMUM_PRIVATE_FILE_BYTES = 16 * 1024
        const val MAXIMUM_RESULT_BYTES = 16 * 1024
        const val MAXIMUM_APP_E2E_RESULT_BYTES = 64 * 1024
        const val PORT_PROBE_TIMEOUT_MILLIS = 250
        const val PROCESS_READY_TIMEOUT_MILLIS = 15_000L
        const val PROVIDER_RECONNECT_TIMEOUT_MILLIS = 30_000L
        const val ADB_SHORT_TIMEOUT_MILLIS = 15_000L
        const val APK_INSTALL_TIMEOUT_MILLIS = 60_000L
        const val INSTRUMENTATION_TIMEOUT_MILLIS = 360_000L
        const val MARKER_WAIT_TIMEOUT_MILLIS = 90_000L
        const val MARKER_POLL_MILLIS = 100L
        const val ASYNC_OBSERVE_TIMEOUT_MILLIS = 1L
        const val ASYNC_INSTRUMENTATION_STOP_TIMEOUT_MILLIS = 15_000L
        const val ASYNC_EXECUTOR_STOP_TIMEOUT_MILLIS = 15_000L
        const val ASYNC_EXECUTOR_FORCE_TIMEOUT_MILLIS = 5_000L
        const val TARGET_PROCESS_TIMEOUT_MILLIS = 30_000L
        const val TARGET_PROCESS_BACKGROUND_SETTLE_MILLIS = 5_000L
        const val NETWORK_SWITCH_TIMEOUT_MILLIS = 90_000L
        const val NETWORK_SWITCH_POLL_MILLIS = 500L
        const val HOST_TRANSPORT_WIFI = "WIFI"
        const val HOST_TRANSPORT_CELLULAR = "CELLULAR"
        const val OUTAGE_POLL_MILLIS = 250L
        const val DEFAULT_APP_E2E_OUTAGE_MILLIS = 120_000L
        const val MINIMUM_APP_E2E_OUTAGE_MILLIS = 60_000L
        const val MAXIMUM_APP_E2E_OUTAGE_MILLIS = 300_000L
        const val RESOURCE_CLEANUP_TIMEOUT_MILLIS = 45_000L
        const val PAGE_SIZE_SHELL =
            "while read key value unit rest; do " +
                "case \"\$key\" in KernelPageSize:|MMUPageSize:) " +
                "case \"\$unit\" in kB) echo \$((value * 1024)); exit 0;; " +
                "B) echo \"\$value\"; exit 0;; esac;; esac; " +
                "done < /proc/self/smaps; exit 1"
        val APP_E2E_CORE_EXPECTED_CHECKS = listOf(
            "kotlin_backend",
            "stcp_ready",
            "snapshot_store",
            "global_sse_store",
            "project_sse_store",
            "foreground_reconciliation",
            "disconnect_data_retained",
            "control_epoch_advanced",
            "restart_reconciliation",
            "post_restart_foreground",
            "streams_closed",
            "visitor_closed",
            "port_released",
        )
        val APP_E2E_LIFECYCLE_EXPECTED_CHECKS = listOf(
            "kotlin_backend",
            "stcp_ready",
            "snapshot_store",
            "global_sse_store",
            "project_sse_store",
            "activity_foreground",
            "activity_background_data_retained",
            "activity_foreground_reconciliation",
            "doze_entered",
            "doze_exit_recovery",
            "disconnect_data_retained",
            "provider_race_data_retained",
            "long_outage_data_retained",
            "control_epoch_advanced",
            "restart_reconciliation",
            "post_restart_foreground",
            "streams_closed",
            "visitor_closed",
            "port_released",
        )
        val APP_E2E_PROCESS_PREPARE_EXPECTED_CHECKS = listOf(
            "kotlin_backend",
            "server_persisted",
            "stcp_ready",
            "snapshot_store",
            "initial_streams",
            "initial_activity_route",
            "runtime_closed",
            "port_released",
        )
        const val APP_E2E_PROCESS_TERMINATED_CHECK = "process_terminated"
        val APP_E2E_PROCESS_RECOVER_EXPECTED_CHECKS = listOf(
            "kotlin_backend",
            "persisted_server_loaded",
            "stcp_reconnected",
            "snapshot_restored",
            "streams_restored",
            "activity_route_restored",
            "streams_closed",
            "server_deleted",
            "visitor_closed",
            "port_released",
        )
        val APP_E2E_NETWORK_EXPECTED_CHECKS = listOf(
            "kotlin_backend",
            "initial_wifi_validated",
            "stcp_ready",
            "snapshot_store",
            "global_sse_store",
            "project_sse_store",
            "cellular_validated",
            "cellular_data_retained",
            "cellular_epoch_advanced",
            "cellular_reconciliation",
            "wifi_restored_validated",
            "wifi_return_data_retained",
            "wifi_return_epoch_advanced",
            "wifi_return_reconciliation",
            "streams_closed",
            "visitor_closed",
            "port_released",
        )
        val APP_E2E_NETWORK_HOST_CHECKS = listOf(
            "configured_pinned_public_server",
            "network_restored",
        )
    }
}

private fun temporaryPrivateFileName(name: String): String {
    val separator = name.lastIndexOf('.')
    if (separator <= 0 || separator == name.lastIndex) {
        throw InteropFailure("Android private file name was invalid")
    }
    return name.substring(0, separator) + "-tmp" + name.substring(separator)
}

internal fun recoverOwnedReversePort(
    output: String,
    targetPort: Int,
    baselinePorts: Set<Int>,
): Int? {
    val added = reversePortsForTarget(output, targetPort).filterNot(baselinePorts::contains)
    if (added.size > 1) throw InteropFailure("adb reverse ownership recovery was ambiguous")
    return added.singleOrNull()
}

private fun reversePortsForTarget(output: String, targetPort: Int): List<Int> = output.lineSequence()
    .map(String::trim)
    .filter(String::isNotEmpty)
    .mapNotNull { line ->
        val columns = line.split(Regex("\\s+"))
        if (columns.size != 3 || columns[2] != "tcp:$targetPort") return@mapNotNull null
        columns[1].removePrefix("tcp:").toIntOrNull()?.takeIf { it in 1..65_535 }
            ?: throw InteropFailure("adb reverse ownership output was invalid")
    }
    .distinct()
    .toList()
