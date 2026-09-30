package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.io.File
import kotlin.math.ceil
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull

internal const val APP_RESOURCE_POLICY_VERSION = "k7-resource-v4"
private const val MAXIMUM_EVIDENCE_BYTES = 64 * 1024L
private const val MAXIMUM_NORMAL_CPU_REGRESSION_RATIO = 1.75
private const val MAXIMUM_CONDITIONAL_CPU_REGRESSION_RATIO = 2.00
private const val MAXIMUM_CONDITIONAL_WALL_REGRESSION_RATIO = 1.05
private const val MAXIMUM_CONDITIONAL_PSS_REGRESSION_RATIO = 1.00
private const val MAXIMUM_WALL_REGRESSION_RATIO = 1.25
private const val MAXIMUM_PSS_REGRESSION_RATIO = 1.25
private const val PSS_REGRESSION_ALLOWANCE_KIB = 16 * 1024.0
private const val MINIMUM_COMPARATIVE_WALL_MILLIS = 60_000L
private const val MAXIMUM_STABLE_FD_COUNT = 256.0
private const val MAXIMUM_STABLE_THREAD_COUNT = 256.0
private const val MAXIMUM_FD_TREND = 4.0
private const val MAXIMUM_WARNING_FREE_QUIESCENCE_FD_DELTA = 4L
private const val MAXIMUM_QUIESCENCE_FD_DELTA = 8L
private const val MAXIMUM_KOTLIN_QUIESCENCE_THREAD_DELTA = 8L
private const val MAXIMUM_GOMOBILE_WARNING_FREE_QUIESCENCE_THREAD_DELTA = 8L
private const val MAXIMUM_GOMOBILE_QUIESCENCE_THREAD_DELTA = 16L
private const val MAXIMUM_THREAD_TREND = 8.0
private const val PSS_TREND_ALLOWANCE_KIB = 8 * 1024.0
private const val PSS_TREND_RATIO = 0.15
private const val HEAP_TREND_ALLOWANCE_BYTES = 8 * 1024 * 1024.0
private const val HEAP_TREND_RATIO = 0.25

internal data class AppResourcePolicySnapshot(
    val elapsedRealtimeMillis: Long,
    val processCpuMillis: Long,
    val pssKiB: Long,
    val javaHeapBytes: Long,
    val nativeHeapBytes: Long,
    val openFdCount: Long,
    val threadCount: Long,
    val sampleOrdinal: Int? = null,
)

internal data class AppResourcePolicyInput(
    val backend: String,
    val profile: String,
    val repository: JsonElement,
    val device: JsonElement,
    val workload: AppE2eWorkload,
    val workloadResult: AppE2eWorkloadResult,
    val wallDurationMillis: Long,
    val cpuDurationMillis: Long,
    val start: AppResourcePolicySnapshot,
    val visitorReady: AppResourcePolicySnapshot,
    val end: AppResourcePolicySnapshot,
    val samples: List<AppResourcePolicySnapshot>,
    val quiescence: AppResourcePolicySnapshot,
    val threadLifecycle: AppE2eThreadLifecycle,
    val threadCohorts: AppE2eThreadLifecycleCohorts,
)

internal data class AppResourcePolicyEvaluation(
    val profile: String,
    val cpuRegressionRatio: Double?,
    val wallRegressionRatio: Double?,
    val pssRegressionRatio: Double?,
    val failures: List<String>,
    val warnings: List<String>,
)

internal fun loadAppResourcePolicyInput(file: File, expectedBackend: String): AppResourcePolicyInput {
    require(file.isFile && file.length() in 1..MAXIMUM_EVIDENCE_BYTES) {
        "App resource evidence was missing or exceeded its bounded size."
    }
    val bytes = file.inputStream().buffered().use { input ->
        input.readNBytes((MAXIMUM_EVIDENCE_BYTES + 1L).toInt())
    }
    require(bytes.isNotEmpty() && bytes.size <= MAXIMUM_EVIDENCE_BYTES) {
        "App resource evidence was missing or exceeded its bounded size."
    }
    val root = Json.parseToJsonElement(bytes.toString(Charsets.UTF_8)).jsonObject
    require(root.int("schemaVersion") == 10L && root.string("backend") == expectedBackend &&
        root.string("suite") == "core" && root.string("status") == "passed" &&
        root.boolean("cleanupVerified") && !root.boolean("authorizesKotlinDefault")
    ) {
        "App resource evidence did not satisfy its bounded acceptance contract."
    }
    val repository = root.objectValue("repository")
    require(repository.boolean("worktreeClean")) { "App resource evidence did not use a clean candidate." }
    require(root.objectValue("targetApk").boolean("byteIdentical") &&
        root.objectValue("testApk").boolean("byteIdentical")
    ) {
        "App resource APK identities were not byte-identical."
    }
    val workloadObject = root.objectValue("workload")
    val profile = workloadObject.string("profile")
    require(profile in setOf("baseline", "stress", "leak", "soak")) {
        "App resource evidence used an unsupported profile."
    }
    val workload = parseAppE2eWorkload(
        suite = "core",
        profileValue = profile,
        iterationsValue = workloadObject.int("iterations").toString(),
        concurrencyValue = workloadObject.int("concurrency").toString(),
        durationMillisValue = workloadObject.int("durationMillis").toString(),
        reconnectEveryIterationsValue = workloadObject.int("reconnectEveryIterations").toString(),
        sampleEveryIterationsValue = workloadObject.int("sampleEveryIterations").toString(),
    )
    val workloadResults = root.arrayValue("workloads")
    val resources = root.arrayValue("resources")
    require(workloadResults.size == 1 && resources.size == 1 &&
        workloadResults.single().jsonObject.string("profile") == profile
    ) {
        "App resource evidence did not contain one matching workload and resource result."
    }
    val workloadResult = workloadResults.single().jsonObject
    val completedIterationsValue = workloadResult.positive("completedIterations")
    val reconnectCountValue = workloadResult.nonNegative("reconnectCount")
    require(completedIterationsValue <= Int.MAX_VALUE && reconnectCountValue <= Int.MAX_VALUE) {
        "App resource evidence did not complete its configured workload."
    }
    val completedIterations = completedIterationsValue.toInt()
    val reconnectCount = reconnectCountValue.toInt()
    val parsedWorkloadResult = AppE2eWorkloadResult(
        profile = workloadResult.string("profile"),
        completedIterations = completedIterations,
        reconnectCount = reconnectCount,
    )
    require(completedIterations in 1..workload.iterations &&
        (profile == APP_E2E_PROFILE_SOAK || completedIterations == workload.iterations) &&
        reconnectCount == expectedReconnectCount(workload, completedIterations)
    ) {
        "App resource evidence did not complete its configured workload."
    }
    val resource = resources.single().jsonObject
    val start = resource.objectValue("start").resourceSnapshot()
    val visitorReady = resource.objectValue("visitorReady").resourceSnapshot()
    val end = resource.objectValue("end").resourceSnapshot()
    val wallDurationMillis = resource.int("wallDurationMillis")
    val cpuDurationMillis = resource.int("cpuDurationMillis")
    require(start.sampleOrdinal == null && visitorReady.sampleOrdinal == null && end.sampleOrdinal == null &&
        visitorReady.elapsedRealtimeMillis in start.elapsedRealtimeMillis..end.elapsedRealtimeMillis &&
        visitorReady.processCpuMillis in start.processCpuMillis..end.processCpuMillis &&
        wallDurationMillis > 0L && cpuDurationMillis >= 0L &&
        cpuDurationMillis <= wallDurationMillis * 64L &&
        wallDurationMillis == end.elapsedRealtimeMillis - start.elapsedRealtimeMillis &&
        cpuDurationMillis == end.processCpuMillis - start.processCpuMillis
    ) {
        "App resource evidence contained invalid duration values."
    }
    val samples = resource.arrayValue("samples").map { it.jsonObject.resourceSnapshot() }
    val observedSampleCount = completedIterations / workload.sampleEveryIterations
    val retainedHeadSampleCount = minOf(observedSampleCount, APP_E2E_RESOURCE_HEAD_SAMPLES)
    val retainedTailSampleCount = minOf(
        maxOf(observedSampleCount - retainedHeadSampleCount, 0),
        APP_E2E_RESOURCE_TAIL_SAMPLES,
    )
    val expectedOrdinals = expectedSampleOrdinals(
        observedSampleCount,
        retainedHeadSampleCount,
        retainedTailSampleCount,
    )
    val retention = resource.objectValue("sampleRetention")
    require(samples.isNotEmpty() && samples.size == retainedHeadSampleCount + retainedTailSampleCount &&
        resource.int("droppedSampleCount") == (observedSampleCount - samples.size).toLong() &&
        retention.string("strategy") == APP_E2E_RESOURCE_SAMPLE_RETENTION_STRATEGY &&
        retention.int("observedSampleCount") == observedSampleCount.toLong() &&
        retention.int("retainedHeadSampleCount") == retainedHeadSampleCount.toLong() &&
        retention.int("retainedTailSampleCount") == retainedTailSampleCount.toLong() &&
        samples.map(AppResourcePolicySnapshot::sampleOrdinal) == expectedOrdinals &&
        samples.zipWithNext().all { (first, second) ->
            second.elapsedRealtimeMillis >= first.elapsedRealtimeMillis &&
                second.processCpuMillis >= first.processCpuMillis
        } && samples.all {
            it.elapsedRealtimeMillis in start.elapsedRealtimeMillis..end.elapsedRealtimeMillis &&
                it.processCpuMillis in start.processCpuMillis..end.processCpuMillis
        }
    ) {
        "App resource evidence did not satisfy its head-tail sample contract."
    }
    val quiescence = resource.objectValue("quiescence")
    val quiescenceDelayMillis = quiescence.int("requestedDelayMillis")
    val quiescenceSnapshot = quiescence.objectValue("snapshot").resourceSnapshot()
    require(quiescenceDelayMillis == APP_E2E_RESOURCE_QUIESCENCE_MILLIS &&
        quiescenceSnapshot.sampleOrdinal == null &&
        quiescenceSnapshot.elapsedRealtimeMillis - end.elapsedRealtimeMillis >= quiescenceDelayMillis &&
        quiescenceSnapshot.processCpuMillis >= end.processCpuMillis
    ) {
        "App resource evidence did not contain the required post-cleanup quiescence snapshot."
    }
    val lifecycleObject = resource.objectValue("threadLifecycle")
    val histogram = lifecycleObject.arrayValue("presenceHistogram").map { element ->
        val value = element.jsonPrimitive.longOrNull
            ?: throw IllegalArgumentException("App resource evidence thread lifecycle was invalid.")
        require(value in 0..APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT.toLong()) {
            "App resource evidence thread lifecycle was invalid."
        }
        value.toInt()
    }
    val lifecycleSchemaVersion = lifecycleObject.int("schemaVersion")
    val lifecycleCaptureLimit = lifecycleObject.int("captureLimit")
    require(lifecycleSchemaVersion == APP_E2E_THREAD_LIFECYCLE_SCHEMA_VERSION.toLong() &&
        lifecycleCaptureLimit == APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT.toLong()
    ) {
        "App resource evidence thread lifecycle was invalid."
    }
    val threadLifecycle = AppE2eThreadLifecycle(
        schemaVersion = lifecycleSchemaVersion.toInt(),
        classifier = lifecycleObject.string("classifier"),
        captureLimit = lifecycleCaptureLimit.toInt(),
        presenceHistogram = histogram,
    )
    require(isValidThreadLifecycle(threadLifecycle) &&
        start.threadCount == threadLifecycleCheckpointTotal(threadLifecycle, APP_E2E_THREAD_START).toLong() &&
        visitorReady.threadCount == threadLifecycleCheckpointTotal(
            threadLifecycle,
            APP_E2E_THREAD_VISITOR_READY,
        ).toLong() &&
        end.threadCount == threadLifecycleCheckpointTotal(threadLifecycle, APP_E2E_THREAD_POST_CLEANUP).toLong() &&
        quiescenceSnapshot.threadCount == threadLifecycleCheckpointTotal(
            threadLifecycle,
            APP_E2E_THREAD_QUIESCENCE,
        ).toLong()
    ) {
        "App resource evidence thread lifecycle was invalid."
    }
    return AppResourcePolicyInput(
        backend = expectedBackend,
        profile = profile,
        repository = repository,
        device = root.objectValue("device"),
        workload = workload,
        workloadResult = parsedWorkloadResult,
        wallDurationMillis = wallDurationMillis,
        cpuDurationMillis = cpuDurationMillis,
        start = start,
        visitorReady = visitorReady,
        end = end,
        samples = samples,
        quiescence = quiescenceSnapshot,
        threadLifecycle = threadLifecycle,
        threadCohorts = threadLifecycleCohorts(threadLifecycle),
    )
}

internal fun evaluateAppResourcePolicy(
    goMobile: AppResourcePolicyInput,
    kotlin: AppResourcePolicyInput,
): AppResourcePolicyEvaluation {
    val failures = mutableListOf<String>()
    val warnings = mutableListOf<String>()
    if (goMobile.backend != "gomobile" || kotlin.backend != "kotlin" ||
        goMobile.repository != kotlin.repository || goMobile.device != kotlin.device ||
        goMobile.workload != kotlin.workload || goMobile.workloadResult != kotlin.workloadResult ||
        goMobile.profile != kotlin.profile
    ) {
        failures += "GoMobile and Kotlin evidence did not use the same candidate, device, and workload."
    }
    val profile = kotlin.profile
    val cpuRatio = ratio(kotlin.cpuDurationMillis, goMobile.cpuDurationMillis)
    val wallRatio = ratio(kotlin.wallDurationMillis, goMobile.wallDurationMillis)
    val goPss = stableWindow(goMobile.samples).map { it.pssKiB }.average()
    val kotlinPss = stableWindow(kotlin.samples).map { it.pssKiB }.average()
    val pssRatio = if (goPss > 0.0) kotlinPss / goPss else null

    if (profile != "baseline" &&
        maxOf(goMobile.wallDurationMillis, kotlin.wallDurationMillis) >= MINIMUM_COMPARATIVE_WALL_MILLIS
    ) {
        if (cpuRatio == null || cpuRatio > MAXIMUM_CONDITIONAL_CPU_REGRESSION_RATIO) {
            failures += "Kotlin process CPU exceeded the 2.00x GoMobile hard limit."
        } else if (cpuRatio > MAXIMUM_NORMAL_CPU_REGRESSION_RATIO) {
            if (wallRatio == null || wallRatio > MAXIMUM_CONDITIONAL_WALL_REGRESSION_RATIO) {
                failures += "Kotlin process CPU above 1.75x required wall duration at or below 1.05x GoMobile."
            }
            if (pssRatio == null || pssRatio > MAXIMUM_CONDITIONAL_PSS_REGRESSION_RATIO) {
                failures += "Kotlin process CPU above 1.75x required stable-window PSS at or below GoMobile."
            }
        }
        if (wallRatio == null || wallRatio > MAXIMUM_WALL_REGRESSION_RATIO) {
            failures += "Kotlin wall duration exceeded the 1.25x GoMobile limit."
        }
        if (kotlinPss > goPss * MAXIMUM_PSS_REGRESSION_RATIO + PSS_REGRESSION_ALLOWANCE_KIB) {
            failures += "Kotlin stable-window PSS exceeded the GoMobile allowance."
        }
    }
    listOf(goMobile, kotlin).forEach { input ->
        val stable = stableWindow(input.samples)
        if (stable.map { it.openFdCount }.average() > MAXIMUM_STABLE_FD_COUNT) {
            failures += "${input.backend} stable-window FD count exceeded 256."
        }
        if (stable.map { it.threadCount }.average() > MAXIMUM_STABLE_THREAD_COUNT) {
            failures += "${input.backend} stable-window thread count exceeded 256."
        }
        val quiescenceFdDelta = input.quiescence.openFdCount - input.start.openFdCount
        if (quiescenceFdDelta > MAXIMUM_QUIESCENCE_FD_DELTA) {
            failures += "${input.backend} post-cleanup FD count remained more than eight above baseline."
        } else if (quiescenceFdDelta > MAXIMUM_WARNING_FREE_QUIESCENCE_FD_DELTA) {
            warnings += "${input.backend} post-cleanup FD count remained more than four above baseline."
        }
        if (profile == "leak" || profile == "soak") {
            evaluateTrend(input, failures)
        }
    }
    val kotlinQuiescenceThreadDelta = kotlin.quiescence.threadCount - kotlin.start.threadCount
    if (kotlinQuiescenceThreadDelta > MAXIMUM_KOTLIN_QUIESCENCE_THREAD_DELTA) {
        failures += "kotlin post-cleanup thread count remained more than eight above baseline."
    }
    val goMobileQuiescenceThreadDelta = goMobile.quiescence.threadCount - goMobile.start.threadCount
    if (goMobileQuiescenceThreadDelta > MAXIMUM_GOMOBILE_QUIESCENCE_THREAD_DELTA) {
        failures += "gomobile post-cleanup thread count remained more than sixteen above baseline."
    } else if (goMobileQuiescenceThreadDelta > MAXIMUM_GOMOBILE_WARNING_FREE_QUIESCENCE_THREAD_DELTA) {
        warnings += "gomobile post-cleanup thread count remained more than eight above baseline."
    }
    return AppResourcePolicyEvaluation(
        profile,
        cpuRatio,
        wallRatio,
        pssRatio,
        failures.distinct(),
        warnings.distinct(),
    )
}

private fun evaluateTrend(input: AppResourcePolicyInput, failures: MutableList<String>) {
    val first = firstWindow(input.samples)
    val last = stableWindow(input.samples)
    fun trend(selector: (AppResourcePolicySnapshot) -> Long): Double =
        last.map(selector).average() - first.map(selector).average()
    val firstPss = first.map { it.pssKiB }.average()
    val firstJava = first.map { it.javaHeapBytes }.average()
    val firstNative = first.map { it.nativeHeapBytes }.average()
    if (trend { it.pssKiB } > maxOf(PSS_TREND_ALLOWANCE_KIB, firstPss * PSS_TREND_RATIO)) {
        failures += "${input.backend} PSS increased beyond the stable-window trend allowance."
    }
    if (trend { it.javaHeapBytes } > maxOf(HEAP_TREND_ALLOWANCE_BYTES, firstJava * HEAP_TREND_RATIO)) {
        failures += "${input.backend} Java heap increased beyond the stable-window trend allowance."
    }
    if (trend { it.nativeHeapBytes } > maxOf(HEAP_TREND_ALLOWANCE_BYTES, firstNative * HEAP_TREND_RATIO)) {
        failures += "${input.backend} native heap increased beyond the stable-window trend allowance."
    }
    if (trend { it.openFdCount } > MAXIMUM_FD_TREND) {
        failures += "${input.backend} FD count increased by more than four across stable windows."
    }
    if (trend { it.threadCount } > MAXIMUM_THREAD_TREND) {
        failures += "${input.backend} thread count increased by more than eight across stable windows."
    }
}

private fun firstWindow(samples: List<AppResourcePolicySnapshot>): List<AppResourcePolicySnapshot> =
    samples.take(windowSize(samples))

private fun stableWindow(samples: List<AppResourcePolicySnapshot>): List<AppResourcePolicySnapshot> =
    samples.takeLast(windowSize(samples))

private fun windowSize(samples: List<AppResourcePolicySnapshot>): Int = ceil(samples.size / 4.0).toInt()

private fun ratio(numerator: Long, denominator: Long): Double? =
    if (denominator > 0L) numerator.toDouble() / denominator else null

private fun JsonObject.resourceSnapshot(): AppResourcePolicySnapshot {
    val openFdCount = positive("openFdCount")
    val threadCount = positive("threadCount")
    require(openFdCount <= MAXIMUM_APP_E2E_RESOURCE_COUNT && threadCount <= MAXIMUM_APP_E2E_RESOURCE_COUNT) {
        "App resource evidence contained an out-of-range resource count."
    }
    return AppResourcePolicySnapshot(
        elapsedRealtimeMillis = nonNegative("elapsedRealtimeMillis"),
        processCpuMillis = nonNegative("processCpuMillis"),
        pssKiB = nonNegative("pssKiB"),
        javaHeapBytes = nonNegative("javaHeapBytes"),
        nativeHeapBytes = nonNegative("nativeHeapBytes"),
        openFdCount = openFdCount,
        threadCount = threadCount,
        sampleOrdinal = optionalPositiveInt("sampleOrdinal"),
    )
}

private fun JsonObject.optionalPositiveInt(name: String): Int? {
    val value = this[name] ?: return null
    val parsed = value.jsonPrimitive.longOrNull
        ?: error("App resource evidence field $name was invalid.")
    require(parsed in 1..Int.MAX_VALUE.toLong()) { "App resource evidence field $name was invalid." }
    return parsed.toInt()
}

private fun JsonObject.objectValue(name: String): JsonObject =
    this[name] as? JsonObject ?: error("App resource evidence field $name was invalid.")

private fun JsonObject.arrayValue(name: String): JsonArray =
    this[name] as? JsonArray ?: error("App resource evidence field $name was invalid.")

private fun JsonObject.string(name: String): String =
    this[name]?.jsonPrimitive?.contentOrNull ?: error("App resource evidence field $name was invalid.")

private fun JsonObject.int(name: String): Long =
    this[name]?.jsonPrimitive?.longOrNull ?: error("App resource evidence field $name was invalid.")

private fun JsonObject.nonNegative(name: String): Long = int(name).also {
    require(it >= 0L) { "App resource evidence field $name was invalid." }
}

private fun JsonObject.positive(name: String): Long = int(name).also {
    require(it > 0L) { "App resource evidence field $name was invalid." }
}

private fun JsonObject.boolean(name: String): Boolean =
    this[name]?.jsonPrimitive?.booleanOrNull ?: error("App resource evidence field $name was invalid.")

fun main(args: Array<String>) {
    require(args.size == 1) { "Expected one App resource evidence directory." }
    val directory = File(args.single())
    val goMobile = loadAppResourcePolicyInput(directory.resolve("gomobile-app-resource.json"), "gomobile")
    val kotlin = loadAppResourcePolicyInput(directory.resolve("kotlin-app-resource.json"), "kotlin")
    val evaluation = evaluateAppResourcePolicy(
        goMobile,
        kotlin,
    )
    println(formatAppResourcePolicySummary(goMobile, kotlin, evaluation))
    if (evaluation.warnings.isNotEmpty()) {
        println("App resource evidence warnings: ${evaluation.warnings.joinToString(" ")}")
    }
    check(evaluation.failures.isEmpty()) {
        "App resource evidence failed $APP_RESOURCE_POLICY_VERSION: ${evaluation.failures.joinToString(" ")}"
    }
}

internal fun formatAppResourcePolicySummary(
    goMobile: AppResourcePolicyInput,
    kotlin: AppResourcePolicyInput,
    evaluation: AppResourcePolicyEvaluation,
): String =
        "policy=$APP_RESOURCE_POLICY_VERSION profile=${evaluation.profile} " +
            "cpuRatio=${evaluation.cpuRegressionRatio?.format()} " +
            "wallRatio=${evaluation.wallRegressionRatio?.format()} " +
            "pssRatio=${evaluation.pssRegressionRatio?.format()} " +
            "gomobileThreads=${goMobile.threadCohorts.format()} " +
            "kotlinThreads=${kotlin.threadCohorts.format()} " +
            "warnings=${evaluation.warnings.size} status=" +
            if (evaluation.failures.isEmpty()) "passed" else "failed"

private fun AppE2eThreadLifecycleCohorts.format(): String =
    "baseline:$quiescenceBaselineSurvivors,warmup:$warmupCreatedSurvivors," +
        "postWarmup:$postWarmupCreatedSurvivors"

private fun Double.format(): String = "%.3f".format(java.util.Locale.ROOT, this)
