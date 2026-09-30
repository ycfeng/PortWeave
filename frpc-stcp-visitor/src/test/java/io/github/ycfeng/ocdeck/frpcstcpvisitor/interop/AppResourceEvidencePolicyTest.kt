package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Files
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class AppResourceEvidencePolicyTest {
    @Test
    fun matchingEvidenceWithinPolicyPasses() {
        val goMobile = input("gomobile", cpu = 100_000, pss = 80_000, threads = 100)
        val kotlin = input("kotlin", cpu = 140_000, pss = 60_000, threads = 104)
        val evaluation = evaluateAppResourcePolicy(goMobile, kotlin)

        assertTrue(evaluation.failures.isEmpty())
        assertTrue(evaluation.warnings.isEmpty())
        assertEquals(1.4, evaluation.cpuRegressionRatio!!, 0.001)
        assertTrue(
            formatAppResourcePolicySummary(goMobile, kotlin, evaluation).contains(
                "gomobileThreads=baseline:100,warmup:0,postWarmup:0 " +
                    "kotlinThreads=baseline:104,warmup:0,postWarmup:0 warnings=0 status=passed",
            ),
        )
    }

    @Test
    fun normalCpuBoundaryPasses() {
        val evaluation = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100),
            input("kotlin", cpu = 175_000, pss = 100_000, threads = 104),
        )

        assertTrue(evaluation.failures.isEmpty())
    }

    @Test
    fun conditionalCpuRangePassesAtWallAndPssBoundaries() {
        listOf(176_000L, 200_000L).forEach { kotlinCpu ->
            val evaluation = evaluateAppResourcePolicy(
                input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, wall = 120_000),
                input("kotlin", cpu = kotlinCpu, pss = 80_000, threads = 104, wall = 126_000),
            )

            assertTrue(evaluation.failures.toString(), evaluation.failures.isEmpty())
        }
    }

    @Test
    fun conditionalCpuRangeRequiresCompensatingWallAndPss() {
        val wallFailure = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, wall = 120_000),
            input("kotlin", cpu = 180_000, pss = 80_000, threads = 104, wall = 126_001),
        )
        val pssFailure = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100),
            input("kotlin", cpu = 180_000, pss = 80_001, threads = 104),
        )

        assertTrue(wallFailure.failures.any { it.contains("required wall duration") })
        assertTrue(pssFailure.failures.any { it.contains("required stable-window PSS") })
    }

    @Test
    fun cpuHardLimitFailsClosed() {
        val evaluation = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100),
            input("kotlin", cpu = 200_001, pss = 60_000, threads = 104),
        )

        assertTrue(evaluation.failures.any { it.contains("process CPU") })
    }

    @Test
    fun leakWindowThreadGrowthFailsClosed() {
        val evaluation = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100),
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100, threadGrowth = 12),
        )

        assertTrue(evaluation.failures.any { it.contains("thread count increased") })
    }

    @Test
    fun candidateMismatchFailsClosed() {
        val goMobile = input("gomobile", cpu = 100_000, pss = 80_000, threads = 100)
        val kotlin = input("kotlin", cpu = 140_000, pss = 60_000, threads = 104).copy(
            repository = buildJsonObject { put("candidate", "different") },
        )

        val evaluation = evaluateAppResourcePolicy(goMobile, kotlin)

        assertTrue(evaluation.failures.any { it.contains("same candidate") })
    }

    @Test
    fun kotlinPostCleanupThreadBudgetIsHard() {
        val goMobile = input("gomobile", cpu = 100_000, pss = 80_000, threads = 100)
        val passing = evaluateAppResourcePolicy(
            goMobile,
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100, quiescenceThreadDelta = 8),
        )
        val failing = evaluateAppResourcePolicy(
            goMobile,
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100, quiescenceThreadDelta = 9),
        )

        assertTrue(passing.failures.isEmpty())
        assertTrue(failing.failures.any { it.contains("kotlin post-cleanup thread") })
    }

    @Test
    fun goMobilePostCleanupThreadResidueWarnsThenFails() {
        val warningFree = evaluateAppResourcePolicy(
            input(
                "gomobile",
                cpu = 100_000,
                pss = 80_000,
                threads = 100,
                quiescenceThreadDelta = 8,
            ),
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100),
        )
        assertTrue(warningFree.failures.isEmpty())
        assertTrue(warningFree.warnings.isEmpty())

        listOf(9L, 16L).forEach { delta ->
            val goMobile = input(
                "gomobile",
                cpu = 100_000,
                pss = 80_000,
                threads = 100,
                quiescenceThreadDelta = delta,
            )
            val kotlin = input("kotlin", cpu = 140_000, pss = 60_000, threads = 100)
            val evaluation = evaluateAppResourcePolicy(goMobile, kotlin)

            assertTrue(evaluation.failures.isEmpty())
            assertTrue(evaluation.warnings.any { it.contains("gomobile post-cleanup thread") })
            assertTrue(formatAppResourcePolicySummary(goMobile, kotlin, evaluation).contains("warnings=1 status=passed"))
        }
        val failure = evaluateAppResourcePolicy(
            input(
                "gomobile",
                cpu = 100_000,
                pss = 80_000,
                threads = 100,
                quiescenceThreadDelta = 17,
            ),
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100),
        )

        assertTrue(failure.failures.any { it.contains("gomobile post-cleanup thread") })
    }

    @Test
    fun postCleanupFdResidueWarnsThenFailsForBothBackends() {
        listOf("gomobile", "kotlin").forEach { backend ->
            fun evaluation(delta: Long): AppResourcePolicyEvaluation {
                val goMobile = input(
                    "gomobile",
                    cpu = 100_000,
                    pss = 80_000,
                    threads = 100,
                    quiescenceFdDelta = if (backend == "gomobile") delta else 0,
                )
                val kotlin = input(
                    "kotlin",
                    cpu = 140_000,
                    pss = 60_000,
                    threads = 100,
                    quiescenceFdDelta = if (backend == "kotlin") delta else 0,
                )
                return evaluateAppResourcePolicy(goMobile, kotlin)
            }

            val warningFree = evaluation(4)
            assertTrue(warningFree.failures.isEmpty())
            assertTrue(warningFree.warnings.isEmpty())
            listOf(5L, 8L).forEach { delta ->
                val warning = evaluation(delta)
                assertTrue(warning.failures.isEmpty())
                assertTrue(warning.warnings.any { it.contains("$backend post-cleanup FD") })
            }
            assertTrue(evaluation(9).failures.any { it.contains("$backend post-cleanup FD") })
        }
    }

    @Test
    fun validFdWarningSummaryLoadsAndCliPasses() {
        val directory = Files.createTempDirectory("ocdeck-resource-warning")
        val originalOut = System.out
        val output = ByteArrayOutputStream()
        try {
            Files.write(
                directory.resolve("gomobile-app-resource.json"),
                evidenceJson(
                    backend = "gomobile",
                    endCpu = 100_000,
                    resourceCpu = 100_000,
                    quiescenceOpenFdCount = 105,
                ).toByteArray(),
            )
            Files.write(
                directory.resolve("kotlin-app-resource.json"),
                evidenceJson(
                    backend = "kotlin",
                    endCpu = 140_000,
                    resourceCpu = 140_000,
                ).toByteArray(),
            )
            System.setOut(PrintStream(output, true, Charsets.UTF_8.name()))

            main(arrayOf(directory.toString()))

            val text = output.toString(Charsets.UTF_8.name())
            assertTrue(text.contains("warnings=1 status=passed"))
            assertTrue(text.contains("App resource evidence warnings:"))
        } finally {
            System.setOut(originalOut)
            directory.toFile().deleteRecursively()
        }
    }

    @Test
    fun legacySummarySchemaFailsClosedBeforePayloadParsing() {
        val file = Files.createTempFile("ocdeck-resource-schema", ".json")
        try {
            Files.write(file, "{\"schemaVersion\":9}".toByteArray())

            assertThrows(IllegalArgumentException::class.java) {
                loadAppResourcePolicyInput(file.toFile(), "gomobile")
            }
        } finally {
            Files.deleteIfExists(file)
        }
    }

    @Test
    fun validSchemaTenEvidenceLoadsAndRecomputesCohorts() {
        withEvidenceFile(evidenceJson()) { file ->
            val input = loadAppResourcePolicyInput(file, "gomobile")

            assertEquals(10, input.workloadResult.completedIterations)
            assertEquals((1..5).toList(), input.samples.map { it.sampleOrdinal })
            assertEquals(AppE2eThreadLifecycleCohorts(100, 0, 0), input.threadCohorts)
        }
    }

    @Test
    fun malformedThreadLifecycleAndCheckpointMismatchFailClosed() {
        listOf(
            evidenceJson(histogram = List(15) { 0 }),
            evidenceJson(histogram = MutableList(16) { 0 }.also { it[0] = 1; it[15] = 100 }),
            evidenceJson(histogram = MutableList(16) { 0 }.also {
                it[15] = APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT + 1
            }),
            evidenceJson(histogram = MutableList(16) { 0 }.also { it[1] = 9_000; it[2] = 9_000 }),
            evidenceJson(startThreads = 99),
        ).forEach { evidence ->
            withEvidenceFile(evidence) { file ->
                assertThrows(IllegalArgumentException::class.java) {
                    loadAppResourcePolicyInput(file, "gomobile")
                }
            }
        }
    }

    @Test
    fun hostResourceBoundsAlsoFailClosedOffline() {
        listOf(
            evidenceJson(endCpu = 10_000_000, resourceCpu = 10_000_000),
            evidenceJson(openFdCount = APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT + 1),
        ).forEach { evidence ->
            withEvidenceFile(evidence) { file ->
                assertThrows(IllegalArgumentException::class.java) {
                    loadAppResourcePolicyInput(file, "gomobile")
                }
            }
        }
    }

    @Test
    fun incompleteWorkloadReconnectMismatchAndFalseOrdinalsFailClosed() {
        listOf(
            evidenceJson(completedIterations = 9, reconnectCount = 4),
            evidenceJson(reconnectCount = 4),
            evidenceJson(sampleOrdinals = listOf(1, 2, 3, 4, 4)),
        ).forEach { evidence ->
            withEvidenceFile(evidence) { file ->
                assertThrows(IllegalArgumentException::class.java) {
                    loadAppResourcePolicyInput(file, "gomobile")
                }
            }
        }
    }

    @Test
    fun comparisonThresholdUsesEitherBackendDuration() {
        val evaluation = evaluateAppResourcePolicy(
            input("gomobile", cpu = 40_000, pss = 80_000, threads = 100, wall = 59_000),
            input("kotlin", cpu = 40_000, pss = 60_000, threads = 104, wall = 118_000),
        )

        assertTrue(evaluation.failures.any { it.contains("wall duration") })
    }

    @Test
    fun comparativeLimitsStartAtSixtySecondsAndSkipBaseline() {
        val belowThreshold = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, wall = 59_999),
            input("kotlin", cpu = 250_000, pss = 120_000, threads = 104, wall = 59_999),
        )
        val atThreshold = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, wall = 60_000),
            input("kotlin", cpu = 200_001, pss = 60_000, threads = 104, wall = 60_000),
        )
        val baseline = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, profile = "baseline"),
            input("kotlin", cpu = 250_000, pss = 120_000, threads = 104, profile = "baseline"),
        )

        assertTrue(belowThreshold.failures.isEmpty())
        assertTrue(atThreshold.failures.any { it.contains("process CPU") })
        assertTrue(baseline.failures.isEmpty())
    }

    @Test
    fun ordinaryPssHardLimitStillFails() {
        val evaluation = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100),
            input("kotlin", cpu = 170_000, pss = 116_385, threads = 104),
        )

        assertTrue(evaluation.failures.any { it.contains("PSS exceeded") })
    }

    @Test
    fun ordinaryWallAndPssHardLimitBoundariesPass() {
        val evaluation = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, wall = 120_000),
            input("kotlin", cpu = 170_000, pss = 116_384, threads = 104, wall = 150_000),
        )

        assertTrue(evaluation.failures.toString(), evaluation.failures.isEmpty())
    }

    @Test
    fun ordinaryWallHardLimitStillFails() {
        val evaluation = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, wall = 120_000),
            input("kotlin", cpu = 170_000, pss = 60_000, threads = 104, wall = 150_001),
        )

        assertTrue(evaluation.failures.any { it.contains("1.25x GoMobile limit") })
    }

    @Test
    fun goMobileLeakWindowThreadGrowthStillFailsClosed() {
        val evaluation = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, threadGrowth = 9),
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100),
        )

        assertTrue(evaluation.failures.any { it.contains("gomobile thread count increased") })
    }

    @Test
    fun threadTrendBoundaryIsHardOnlyForLeakAndSoak() {
        val leakAtBoundary = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, threadGrowth = 8),
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100),
        )
        val soakAboveBoundary = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, profile = "soak"),
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100, threadGrowth = 9, profile = "soak"),
        )
        val stressAboveBoundary = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, profile = "stress"),
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100, threadGrowth = 9, profile = "stress"),
        )

        assertTrue(leakAtBoundary.failures.isEmpty())
        assertTrue(soakAboveBoundary.failures.any { it.contains("thread count increased") })
        assertTrue(stressAboveBoundary.failures.isEmpty())
    }

    @Test
    fun fdTrendBoundaryRemainsHardOnlyForLeakAndSoak() {
        val leakAtBoundary = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, fdGrowth = 4),
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100),
        )
        val soakAboveBoundary = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, profile = "soak"),
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100, fdGrowth = 5, profile = "soak"),
        )
        val stressAboveBoundary = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, profile = "stress"),
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100, fdGrowth = 5, profile = "stress"),
        )

        assertTrue(leakAtBoundary.failures.isEmpty())
        assertTrue(soakAboveBoundary.failures.any { it.contains("FD count increased") })
        assertTrue(stressAboveBoundary.failures.isEmpty())
    }

    @Test
    fun stableFdCountBoundaryRemainsHardForEveryProfile() {
        val passing = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, stableFdCount = 256),
            input("kotlin", cpu = 140_000, pss = 60_000, threads = 100, stableFdCount = 256),
        )
        val failing = evaluateAppResourcePolicy(
            input("gomobile", cpu = 100_000, pss = 80_000, threads = 100, profile = "baseline"),
            input(
                "kotlin",
                cpu = 140_000,
                pss = 60_000,
                threads = 100,
                stableFdCount = 257,
                profile = "baseline",
            ),
        )

        assertTrue(passing.failures.isEmpty())
        assertTrue(failing.failures.any { it.contains("stable-window FD count exceeded 256") })
    }

    private fun input(
        backend: String,
        cpu: Long,
        pss: Long,
        threads: Long,
        threadGrowth: Long = 0,
        fdGrowth: Long = 0,
        stableFdCount: Long = 100,
        wall: Long = 120_000,
        profile: String = "leak",
        quiescenceThreadDelta: Long = 0,
        quiescenceFdDelta: Long = 0,
    ): AppResourcePolicyInput {
        val samples = (0 until 16).map { index ->
            AppResourcePolicySnapshot(
                elapsedRealtimeMillis = index * 1_000L,
                processCpuMillis = index * 100L,
                pssKiB = pss,
                javaHeapBytes = 8L * 1024 * 1024,
                nativeHeapBytes = 12L * 1024 * 1024,
                openFdCount = stableFdCount + if (index >= 12) fdGrowth else 0,
                threadCount = threads + if (index >= 12) threadGrowth else 0,
                sampleOrdinal = index + 1,
            )
        }
        val workload = AppE2eWorkload(profile, 16, 1, 0, 4, 1)
        val lifecycle = AppE2eThreadLifecycle(
            schemaVersion = APP_E2E_THREAD_LIFECYCLE_SCHEMA_VERSION,
            classifier = APP_E2E_THREAD_LIFECYCLE_CLASSIFIER,
            captureLimit = APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT,
            presenceHistogram = MutableList(16) { 0 }.also { it[15] = threads.toInt() },
        )
        return AppResourcePolicyInput(
            backend = backend,
            profile = profile,
            repository = buildJsonObject { put("candidate", "same") },
            device = buildJsonObject { put("serial", "same") },
            workload = workload,
            workloadResult = AppE2eWorkloadResult(profile, 16, 4),
            wallDurationMillis = wall,
            cpuDurationMillis = cpu,
            start = samples.first(),
            visitorReady = samples.first(),
            end = samples.last(),
            samples = samples,
            quiescence = samples.first().copy(
                elapsedRealtimeMillis = samples.last().elapsedRealtimeMillis + APP_E2E_RESOURCE_QUIESCENCE_MILLIS,
                processCpuMillis = samples.last().processCpuMillis,
                openFdCount = samples.first().openFdCount + quiescenceFdDelta,
                threadCount = samples.first().threadCount + quiescenceThreadDelta,
            ),
            threadLifecycle = lifecycle,
            threadCohorts = threadLifecycleCohorts(lifecycle),
        )
    }

    private fun evidenceJson(
        backend: String = "gomobile",
        completedIterations: Int = 10,
        reconnectCount: Int = 5,
        sampleOrdinals: List<Int> = (1..5).toList(),
        histogram: List<Int> = MutableList(16) { 0 }.also { it[15] = 100 },
        startThreads: Int = 100,
        quiescenceThreads: Int = 100,
        endCpu: Long = 10_000,
        resourceCpu: Long = 10_000,
        openFdCount: Int = 100,
        quiescenceOpenFdCount: Int = openFdCount,
    ): String = buildJsonObject {
            put("schemaVersion", 10)
            put("backend", backend)
            put("suite", "core")
            put("status", "passed")
            put("cleanupVerified", true)
            put("authorizesKotlinDefault", false)
            putJsonObject("repository") {
                put("candidate", "same")
                put("worktreeClean", true)
            }
            putJsonObject("device") { put("serial", "same") }
            putJsonObject("targetApk") { put("byteIdentical", true) }
            putJsonObject("testApk") { put("byteIdentical", true) }
            putJsonObject("workload") {
                put("profile", "stress")
                put("iterations", 10)
                put("concurrency", 1)
                put("durationMillis", 0)
                put("reconnectEveryIterations", 2)
                put("sampleEveryIterations", 2)
            }
            putJsonArray("workloads") {
                add(buildJsonObject {
                    put("profile", "stress")
                    put("completedIterations", completedIterations)
                    put("reconnectCount", reconnectCount)
                })
            }
            putJsonArray("resources") {
                add(buildJsonObject {
                    putJsonObject("start") { addSnapshot(0, 0, null, startThreads, openFdCount) }
                    putJsonObject("visitorReady") { addSnapshot(100, 10, null, openFdCount = openFdCount) }
                    putJsonObject("end") { addSnapshot(120_000, endCpu, null, openFdCount = openFdCount) }
                    put("wallDurationMillis", 120_000)
                    put("cpuDurationMillis", resourceCpu)
                    putJsonArray("samples") {
                        sampleOrdinals.forEach { ordinal ->
                            add(buildJsonObject {
                                addSnapshot(ordinal * 1_000L, ordinal * 100L, ordinal, openFdCount = openFdCount)
                            })
                        }
                    }
                    put("droppedSampleCount", 0)
                    putJsonObject("sampleRetention") {
                        put("strategy", APP_E2E_RESOURCE_SAMPLE_RETENTION_STRATEGY)
                        put("observedSampleCount", 5)
                        put("retainedHeadSampleCount", 5)
                        put("retainedTailSampleCount", 0)
                    }
                    putJsonObject("quiescence") {
                        put("requestedDelayMillis", APP_E2E_RESOURCE_QUIESCENCE_MILLIS)
                        putJsonObject("snapshot") {
                            addSnapshot(
                                210_000,
                                endCpu,
                                null,
                                threads = quiescenceThreads,
                                openFdCount = quiescenceOpenFdCount,
                            )
                        }
                    }
                    putJsonObject("threadLifecycle") {
                        put("schemaVersion", APP_E2E_THREAD_LIFECYCLE_SCHEMA_VERSION)
                        put("classifier", APP_E2E_THREAD_LIFECYCLE_CLASSIFIER)
                        put("captureLimit", APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT)
                        putJsonArray("presenceHistogram") { histogram.forEach { add(it) } }
                    }
                })
            }
        }.toString()

    private fun kotlinx.serialization.json.JsonObjectBuilder.addSnapshot(
        elapsed: Long,
        cpu: Long,
        ordinal: Int?,
        threads: Int = 100,
        openFdCount: Int = 100,
    ) {
        put("elapsedRealtimeMillis", elapsed)
        put("processCpuMillis", cpu)
        put("pssKiB", 64_000)
        put("javaHeapBytes", 8L * 1024 * 1024)
        put("nativeHeapBytes", 12L * 1024 * 1024)
        put("openFdCount", openFdCount)
        put("threadCount", threads)
        if (ordinal != null) put("sampleOrdinal", ordinal)
    }

    private fun withEvidenceFile(evidence: String, block: (java.io.File) -> Unit) {
        val file = Files.createTempFile("ocdeck-resource-policy", ".json")
        try {
            Files.write(file, evidence.toByteArray())
            block(file.toFile())
        } finally {
            Files.deleteIfExists(file)
        }
    }
}
