package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import kotlinx.serialization.Serializable

@Serializable
internal data class AppE2eWorkload(
    val profile: String,
    val iterations: Int,
    val concurrency: Int,
    val durationMillis: Long,
    val reconnectEveryIterations: Int,
    val sampleEveryIterations: Int,
)

@Serializable
internal data class AppE2eWorkloadResult(
    val profile: String,
    val completedIterations: Int,
    val reconnectCount: Int,
)

@Serializable
internal data class AppE2eResourceSnapshot(
    val elapsedRealtimeMillis: Long,
    val processCpuMillis: Long,
    val pssKiB: Long,
    val javaHeapBytes: Long,
    val nativeHeapBytes: Long,
    val openFdCount: Int,
    val threadCount: Int,
    val sampleOrdinal: Int? = null,
)

@Serializable
internal data class AppE2eResourceSampleRetention(
    val strategy: String,
    val observedSampleCount: Int,
    val retainedHeadSampleCount: Int,
    val retainedTailSampleCount: Int,
)

@Serializable
internal data class AppE2eResourceQuiescence(
    val requestedDelayMillis: Long,
    val snapshot: AppE2eResourceSnapshot,
)

@Serializable
internal data class AppE2eThreadLifecycle(
    val schemaVersion: Int,
    val classifier: String,
    val captureLimit: Int,
    val presenceHistogram: List<Int>,
)

@Serializable
internal data class AppE2eResourceEvidence(
    val start: AppE2eResourceSnapshot,
    val visitorReady: AppE2eResourceSnapshot? = null,
    val end: AppE2eResourceSnapshot,
    val wallDurationMillis: Long,
    val cpuDurationMillis: Long,
    val samples: List<AppE2eResourceSnapshot>,
    val droppedSampleCount: Int,
    val sampleRetention: AppE2eResourceSampleRetention,
    val quiescence: AppE2eResourceQuiescence? = null,
    val threadLifecycle: AppE2eThreadLifecycle? = null,
)

internal data class AppE2eThreadLifecycleCohorts(
    val quiescenceBaselineSurvivors: Int,
    val warmupCreatedSurvivors: Int,
    val postWarmupCreatedSurvivors: Int,
)

internal fun parseAppE2eWorkload(
    suite: String,
    profileValue: String?,
    iterationsValue: String?,
    concurrencyValue: String?,
    durationMillisValue: String?,
    reconnectEveryIterationsValue: String?,
    sampleEveryIterationsValue: String?,
): AppE2eWorkload {
    val profile = profileValue?.takeIf(String::isNotBlank) ?: APP_E2E_PROFILE_FUNCTIONAL
    val defaults = when (profile) {
        APP_E2E_PROFILE_FUNCTIONAL -> AppE2eWorkload(profile, 0, 1, 0L, 0, 1)
        APP_E2E_PROFILE_BASELINE -> AppE2eWorkload(profile, 5, 1, 0L, 0, 1)
        APP_E2E_PROFILE_STRESS -> AppE2eWorkload(profile, 100, 8, 0L, 10, 5)
        APP_E2E_PROFILE_LEAK -> AppE2eWorkload(profile, 200, 1, 0L, 1, 5)
        APP_E2E_PROFILE_SOAK -> AppE2eWorkload(profile, 100_000, 4, 28_800_000L, 25, 25)
        else -> throw InteropFailure("configured App E2E resource profile was invalid")
    }
    fun integer(value: String?, default: Int): Int {
        if (value.isNullOrBlank()) return default
        return value.toIntOrNull() ?: throw InteropFailure("configured App E2E workload was invalid")
    }
    fun long(value: String?, default: Long): Long {
        if (value.isNullOrBlank()) return default
        return value.toLongOrNull() ?: throw InteropFailure("configured App E2E workload was invalid")
    }
    val workload = defaults.copy(
        iterations = integer(iterationsValue, defaults.iterations),
        concurrency = integer(concurrencyValue, defaults.concurrency),
        durationMillis = long(durationMillisValue, defaults.durationMillis),
        reconnectEveryIterations = integer(
            reconnectEveryIterationsValue,
            defaults.reconnectEveryIterations,
        ),
        sampleEveryIterations = integer(sampleEveryIterationsValue, defaults.sampleEveryIterations),
    )
    if (workload.iterations !in 0..MAXIMUM_APP_E2E_WORKLOAD_ITERATIONS ||
        workload.concurrency !in 1..MAXIMUM_APP_E2E_WORKLOAD_CONCURRENCY ||
        workload.reconnectEveryIterations !in 0..maxOf(1, workload.iterations) ||
        workload.sampleEveryIterations !in 1..maxOf(1, workload.iterations) ||
        (profile == APP_E2E_PROFILE_FUNCTIONAL && workload != defaults) ||
        (profile != APP_E2E_PROFILE_FUNCTIONAL && (suite != APP_E2E_WORKLOAD_SUITE_CORE || workload.iterations == 0)) ||
        (profile == APP_E2E_PROFILE_SOAK &&
            workload.durationMillis !in MINIMUM_APP_E2E_SOAK_MILLIS..MAXIMUM_APP_E2E_SOAK_MILLIS) ||
        (profile != APP_E2E_PROFILE_SOAK && workload.durationMillis != 0L) ||
        (profile == APP_E2E_PROFILE_LEAK && workload.reconnectEveryIterations == 0)
    ) {
        throw InteropFailure("configured App E2E workload was invalid")
    }
    return workload
}

internal fun isValidAppE2eResourceEvidence(
    evidence: AppE2eResourceEvidence,
    workload: AppE2eWorkload,
    workloadResult: AppE2eWorkloadResult?,
): Boolean {
    fun valid(snapshot: AppE2eResourceSnapshot): Boolean =
        snapshot.elapsedRealtimeMillis >= 0L && snapshot.processCpuMillis >= 0L && snapshot.pssKiB >= 0L &&
            snapshot.javaHeapBytes >= 0L && snapshot.nativeHeapBytes >= 0L &&
            snapshot.openFdCount in 1..MAXIMUM_APP_E2E_RESOURCE_COUNT &&
            snapshot.threadCount in 1..MAXIMUM_APP_E2E_RESOURCE_COUNT
    val scheduledSampleCount = when {
        workload.profile == APP_E2E_PROFILE_FUNCTIONAL && workloadResult == null -> 0
        workload.profile == APP_E2E_PROFILE_FUNCTIONAL || workloadResult == null -> return false
        workloadResult.profile != workload.profile ||
            workloadResult.completedIterations !in 1..workload.iterations ||
            (workload.profile != APP_E2E_PROFILE_SOAK &&
                workloadResult.completedIterations != workload.iterations) ||
            workloadResult.reconnectCount != expectedReconnectCount(workload, workloadResult.completedIterations) ->
            return false
        else -> workloadResult.completedIterations / workload.sampleEveryIterations
    }
    if (workload.profile != APP_E2E_PROFILE_FUNCTIONAL && scheduledSampleCount == 0) return false
    val expectedStoredSampleCount = minOf(scheduledSampleCount, MAXIMUM_APP_E2E_RESOURCE_SAMPLES)
    val expectedDroppedSampleCount = maxOf(scheduledSampleCount - MAXIMUM_APP_E2E_RESOURCE_SAMPLES, 0)
    val expectedHeadSampleCount = minOf(scheduledSampleCount, APP_E2E_RESOURCE_HEAD_SAMPLES)
    val expectedTailSampleCount = minOf(
        maxOf(scheduledSampleCount - expectedHeadSampleCount, 0),
        APP_E2E_RESOURCE_TAIL_SAMPLES,
    )
    val expectedOrdinals = expectedSampleOrdinals(
        scheduledSampleCount,
        expectedHeadSampleCount,
        expectedTailSampleCount,
    )
    val samplesValid = evidence.samples.size == expectedStoredSampleCount &&
        evidence.droppedSampleCount == expectedDroppedSampleCount && evidence.samples.all(::valid) &&
        evidence.sampleRetention == AppE2eResourceSampleRetention(
            strategy = APP_E2E_RESOURCE_SAMPLE_RETENTION_STRATEGY,
            observedSampleCount = scheduledSampleCount,
            retainedHeadSampleCount = expectedHeadSampleCount,
            retainedTailSampleCount = expectedTailSampleCount,
        ) && evidence.samples.map(AppE2eResourceSnapshot::sampleOrdinal) == expectedOrdinals &&
        evidence.samples.zipWithNext().all { (first, second) ->
            second.elapsedRealtimeMillis >= first.elapsedRealtimeMillis &&
                second.processCpuMillis >= first.processCpuMillis
        } && evidence.samples.all {
            it.elapsedRealtimeMillis in evidence.start.elapsedRealtimeMillis..evidence.end.elapsedRealtimeMillis &&
                it.processCpuMillis in evidence.start.processCpuMillis..evidence.end.processCpuMillis
        }
    val quiescenceValid = if (workload.profile == APP_E2E_PROFILE_FUNCTIONAL) {
        evidence.visitorReady == null && evidence.quiescence == null && evidence.threadLifecycle == null
    } else {
        val visitorReady = evidence.visitorReady
        val lifecycle = evidence.threadLifecycle
        evidence.quiescence?.let { quiescence ->
            val ready = visitorReady ?: return@let false
            val validLifecycle = lifecycle ?: return@let false
            quiescence.requestedDelayMillis == APP_E2E_RESOURCE_QUIESCENCE_MILLIS &&
                valid(quiescence.snapshot) &&
                quiescence.snapshot.sampleOrdinal == null &&
                quiescence.snapshot.elapsedRealtimeMillis - evidence.end.elapsedRealtimeMillis >=
                quiescence.requestedDelayMillis &&
                quiescence.snapshot.processCpuMillis >= evidence.end.processCpuMillis &&
                valid(ready) && ready.sampleOrdinal == null &&
                ready.elapsedRealtimeMillis in evidence.start.elapsedRealtimeMillis..evidence.end.elapsedRealtimeMillis &&
                ready.processCpuMillis in evidence.start.processCpuMillis..evidence.end.processCpuMillis &&
                isValidThreadLifecycle(validLifecycle) &&
                evidence.start.threadCount == threadLifecycleCheckpointTotal(validLifecycle, APP_E2E_THREAD_START) &&
                ready.threadCount == threadLifecycleCheckpointTotal(validLifecycle, APP_E2E_THREAD_VISITOR_READY) &&
                evidence.end.threadCount == threadLifecycleCheckpointTotal(validLifecycle, APP_E2E_THREAD_POST_CLEANUP) &&
                quiescence.snapshot.threadCount == threadLifecycleCheckpointTotal(
                    validLifecycle,
                    APP_E2E_THREAD_QUIESCENCE,
                )
        } == true
    }
    return valid(evidence.start) && evidence.start.sampleOrdinal == null &&
        valid(evidence.end) && evidence.end.sampleOrdinal == null && samplesValid && quiescenceValid &&
        evidence.wallDurationMillis == evidence.end.elapsedRealtimeMillis - evidence.start.elapsedRealtimeMillis &&
        evidence.cpuDurationMillis == evidence.end.processCpuMillis - evidence.start.processCpuMillis &&
        evidence.wallDurationMillis >= 0L && evidence.cpuDurationMillis >= 0L &&
        evidence.cpuDurationMillis <= evidence.wallDurationMillis * 64L
}

internal fun isValidThreadLifecycle(lifecycle: AppE2eThreadLifecycle?): Boolean {
    if (lifecycle == null || lifecycle.schemaVersion != APP_E2E_THREAD_LIFECYCLE_SCHEMA_VERSION ||
        lifecycle.classifier != APP_E2E_THREAD_LIFECYCLE_CLASSIFIER ||
        lifecycle.captureLimit != APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT ||
        lifecycle.presenceHistogram.size != APP_E2E_THREAD_LIFECYCLE_BUCKET_COUNT ||
        lifecycle.presenceHistogram[0] != 0 ||
        lifecycle.presenceHistogram.any { it !in 0..APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT }
    ) {
        return false
    }
    return lifecycle.presenceHistogram.sumOf(Int::toLong) <= APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT
}

internal fun threadLifecycleCheckpointTotal(lifecycle: AppE2eThreadLifecycle, checkpointBit: Int): Int =
    lifecycle.presenceHistogram.indices.sumOf { mask ->
        if (mask and checkpointBit != 0) lifecycle.presenceHistogram[mask] else 0
    }

internal fun threadLifecycleCohorts(lifecycle: AppE2eThreadLifecycle): AppE2eThreadLifecycleCohorts =
    AppE2eThreadLifecycleCohorts(
        quiescenceBaselineSurvivors = lifecycle.presenceHistogram.indices.sumOf { mask ->
            if (mask and APP_E2E_THREAD_START != 0 && mask and APP_E2E_THREAD_QUIESCENCE != 0) {
                lifecycle.presenceHistogram[mask]
            } else {
                0
            }
        },
        warmupCreatedSurvivors = lifecycle.presenceHistogram.indices.sumOf { mask ->
            if (mask and APP_E2E_THREAD_START == 0 && mask and APP_E2E_THREAD_VISITOR_READY != 0 &&
                mask and APP_E2E_THREAD_QUIESCENCE != 0
            ) {
                lifecycle.presenceHistogram[mask]
            } else {
                0
            }
        },
        postWarmupCreatedSurvivors = lifecycle.presenceHistogram.indices.sumOf { mask ->
            if (mask and APP_E2E_THREAD_START == 0 && mask and APP_E2E_THREAD_VISITOR_READY == 0 &&
                mask and APP_E2E_THREAD_QUIESCENCE != 0
            ) {
                lifecycle.presenceHistogram[mask]
            } else {
                0
            }
        },
    )

internal fun expectedReconnectCount(workload: AppE2eWorkload, completedIterations: Int): Int =
    if (workload.reconnectEveryIterations == 0) 0
    else completedIterations / workload.reconnectEveryIterations

internal fun expectedSampleOrdinals(
    observedSampleCount: Int,
    retainedHeadSampleCount: Int,
    retainedTailSampleCount: Int,
): List<Int> = buildList {
    if (retainedHeadSampleCount > 0) addAll(1..retainedHeadSampleCount)
    if (retainedTailSampleCount > 0) {
        addAll(observedSampleCount - retainedTailSampleCount + 1..observedSampleCount)
    }
}

internal fun appE2eInstrumentationTimeoutMillis(workload: AppE2eWorkload): Long {
    val workloadBudget = if (workload.profile == APP_E2E_PROFILE_SOAK) {
        workload.durationMillis
    } else {
        workload.iterations.toLong() * APP_E2E_ITERATION_TIMEOUT_BUDGET_MILLIS
    }
    return (APP_E2E_INSTRUMENTATION_BASE_TIMEOUT_MILLIS + workloadBudget)
        .coerceAtMost(MAXIMUM_APP_E2E_INSTRUMENTATION_TIMEOUT_MILLIS)
}

internal const val APP_E2E_PROFILE_FUNCTIONAL = "functional"
internal const val APP_E2E_PROFILE_BASELINE = "baseline"
internal const val APP_E2E_PROFILE_STRESS = "stress"
internal const val APP_E2E_PROFILE_LEAK = "leak"
internal const val APP_E2E_PROFILE_SOAK = "soak"
internal const val MAXIMUM_APP_E2E_RESOURCE_COUNT = 16_384
internal const val MAXIMUM_APP_E2E_RESOURCE_SAMPLES = 254
internal const val APP_E2E_RESOURCE_HEAD_SAMPLES = MAXIMUM_APP_E2E_RESOURCE_SAMPLES / 2
internal const val APP_E2E_RESOURCE_TAIL_SAMPLES =
    MAXIMUM_APP_E2E_RESOURCE_SAMPLES - APP_E2E_RESOURCE_HEAD_SAMPLES
internal const val APP_E2E_RESOURCE_SAMPLE_RETENTION_STRATEGY = "head-tail-v1"
internal const val APP_E2E_RESOURCE_QUIESCENCE_MILLIS = 90_000L
internal const val APP_E2E_THREAD_LIFECYCLE_SCHEMA_VERSION = 1
internal const val APP_E2E_THREAD_LIFECYCLE_CLASSIFIER = "proc-task-lifecycle-v1"
internal const val APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT = 16_384
internal const val APP_E2E_THREAD_LIFECYCLE_BUCKET_COUNT = 16
internal const val APP_E2E_THREAD_START = 1
internal const val APP_E2E_THREAD_VISITOR_READY = 2
internal const val APP_E2E_THREAD_POST_CLEANUP = 4
internal const val APP_E2E_THREAD_QUIESCENCE = 8
internal const val MAXIMUM_APP_E2E_WORKLOAD_ITERATIONS = 100_000
internal const val MAXIMUM_APP_E2E_WORKLOAD_CONCURRENCY = 32
internal const val MINIMUM_APP_E2E_SOAK_MILLIS = 60_000L
internal const val MAXIMUM_APP_E2E_SOAK_MILLIS = 86_400_000L
private const val APP_E2E_WORKLOAD_SUITE_CORE = "core"
private const val APP_E2E_ITERATION_TIMEOUT_BUDGET_MILLIS = 30_000L
private const val APP_E2E_INSTRUMENTATION_BASE_TIMEOUT_MILLIS =
    660_000L + APP_E2E_RESOURCE_QUIESCENCE_MILLIS
private const val MAXIMUM_APP_E2E_INSTRUMENTATION_TIMEOUT_MILLIS =
    MAXIMUM_APP_E2E_SOAK_MILLIS + APP_E2E_INSTRUMENTATION_BASE_TIMEOUT_MILLIS
