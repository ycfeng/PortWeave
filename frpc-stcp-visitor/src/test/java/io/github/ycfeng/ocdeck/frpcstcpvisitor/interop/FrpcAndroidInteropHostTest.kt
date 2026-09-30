package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class FrpcAndroidInteropHostTest {
    @Test
    fun compatSuiteContainsOnlyTheFrozenV1PlainProfile() {
        val profiles = androidInteropProfiles("compat")

        assertEquals(1, profiles.size)
        assertEquals("success-v1-00", profiles.single().caseId)
        assertEquals("success", profiles.single().scenario.kind)
        assertEquals("v1", profiles.single().wireProtocol)
        assertFalse(profiles.single().useEncryption)
        assertFalse(profiles.single().useCompression)
        assertTrue(profiles.single().usesConcurrentTraffic)
        assertFalse(profiles.single().isRestart)
    }

    @Test
    fun fullSuiteContainsTheFrozenProfilesInOrder() {
        val profiles = androidInteropProfiles("full")

        assertEquals(12, profiles.size)
        assertEquals(
            listOf(
                "success-v1-00|success|v1|false|false",
                "success-v1-01|success|v1|false|true",
                "success-v1-10|success|v1|true|false",
                "success-v1-11|success|v1|true|true",
                "success-v2-00|success|v2|false|false",
                "success-v2-01|success|v2|false|true",
                "success-v2-10|success|v2|true|false",
                "success-v2-11|success|v2|true|true",
                "negative-wrong-token-v2-00|wrong_token|v2|false|false",
                "negative-wrong-secret-v2-00|wrong_secret|v2|false|false",
                "negative-bind-conflict-v2-00|bind_conflict|v2|false|false",
                "restart-v2-11|restart|v2|true|true",
            ),
            profiles.map { profile ->
                "${profile.caseId}|${profile.scenario.kind}|${profile.wireProtocol}|" +
                    "${profile.useEncryption}|${profile.useCompression}"
            },
        )
        assertFalse(profiles[8].usesConcurrentTraffic)
        assertFalse(profiles[9].usesConcurrentTraffic)
        assertFalse(profiles[10].usesConcurrentTraffic)
        assertTrue(profiles.last().usesConcurrentTraffic)
        assertTrue(profiles.last().isRestart)
        assertEquals(
            listOf(
                "success", "success", "success", "success",
                "success", "success", "success", "success",
                "control_failure", "stcp_secret_rejected", "bind_conflict", "recovered",
            ),
            profiles.map { it.expectedSemanticOutcome },
        )
    }

    @Test
    fun suitesRejectMissingAliasesAndUnexpectedValues() {
        listOf("", "Compat", "compat ", "matrix").forEach { suite ->
            assertThrows(InteropFailure::class.java) { androidInteropProfiles(suite) }
        }
    }

    @Test
    fun androidRuntimePageSizeParserAcceptsGetconfOutputAndRejectsInvalidValues() {
        assertEquals(4_096, parseAndroidRuntimePageSize("4096\n"))
        assertEquals(16_384, parseAndroidRuntimePageSize(" 16384 \r\n"))
        listOf("", "0", "-4096", "16384\nunexpected", "unavailable").forEach { output ->
            assertThrows(InteropFailure::class.java) { parseAndroidRuntimePageSize(output) }
        }
    }

    @Test
    fun androidRuntimePageSizeUsesLegacyProbeOnlyWhenGetconfIsMissing() {
        var legacyProbeCalls = 0
        assertEquals(
            16_384,
            resolveAndroidRuntimePageSize(AdbCommandResult(0, "16384\n", "")) {
                legacyProbeCalls += 1
                "4096\n"
            },
        )
        assertEquals(0, legacyProbeCalls)

        assertEquals(
            4_096,
            resolveAndroidRuntimePageSize(AdbCommandResult(127, "", "getconf: not found")) {
                legacyProbeCalls += 1
                "4096\n"
            },
        )
        assertEquals(1, legacyProbeCalls)

        assertThrows(InteropFailure::class.java) {
            resolveAndroidRuntimePageSize(AdbCommandResult(1, "", "device unavailable")) {
                legacyProbeCalls += 1
                "4096\n"
            }
        }
        assertEquals(1, legacyProbeCalls)
    }

    @Test
    fun appE2eTlsCertificatePinIsRequiredOnlyForNetworkSuite() {
        val pin = "0".repeat(64)

        assertTrue(isValidAppE2eTlsCertificatePin("network", pin))
        assertFalse(isValidAppE2eTlsCertificatePin("network", null))
        assertFalse(isValidAppE2eTlsCertificatePin("network", "A".repeat(64)))
        listOf("core", "lifecycle", "process").forEach { suite ->
            assertTrue(isValidAppE2eTlsCertificatePin(suite, null))
            assertFalse(isValidAppE2eTlsCertificatePin(suite, pin))
        }
    }

    @Test
    fun appE2eRepositoryIdentityRequiresExactLowercaseIdsAndCleanStatus() {
        val commit = "1".repeat(40)
        val tree = "a".repeat(40)

        assertEquals(
            AppE2eRepositoryIdentity(commit, tree, worktreeClean = true),
            parseAppE2eRepositoryIdentity("$commit\n", "$tree\r\n", ""),
        )
        listOf(
            Triple("A".repeat(40), tree, ""),
            Triple(commit, "b".repeat(39), ""),
            Triple("$commit\n$commit\n", tree, ""),
            Triple(commit, tree, " M tracked-file\n"),
            Triple(commit, tree, "?? untracked-file\n"),
        ).forEach { (head, treeOutput, status) ->
            assertThrows(InteropFailure::class.java) {
                parseAppE2eRepositoryIdentity(head, treeOutput, status)
            }
        }
    }

    @Test
    fun appE2eInstalledApkIdentityParsersRejectAmbiguousOrNonCanonicalOutput() {
        val path = "/data/app/~~token==/io.github.ycfeng.ocdeck/base.apk"
        val digest = "c".repeat(64)

        assertEquals(path, parseInstalledAppE2eApkPath("package:$path\n"))
        assertEquals(digest, parseInstalledAppE2eSha256("$digest  $path\n"))
        listOf(
            "",
            "package:$path\npackage:/data/app/other/base.apk\n",
            "package:relative/base.apk\n",
            "package:/data/app/base.zip\n",
        ).forEach { output ->
            assertThrows(InteropFailure::class.java) { parseInstalledAppE2eApkPath(output) }
        }
        listOf(
            "",
            "${digest.uppercase()}  $path\n",
            "$digest\n",
            "$digest  $path\n$digest  $path\n",
        ).forEach { output ->
            assertThrows(InteropFailure::class.java) { parseInstalledAppE2eSha256(output) }
        }
    }

    @Test
    fun appE2eArtifactIdentityRequiresMatchingLowercaseSha256Values() {
        val digest = "d".repeat(64)

        assertEquals(
            AppE2eArtifactIdentity(digest, digest, byteIdentical = true),
            buildAppE2eArtifactIdentity(digest, digest),
        )
        listOf(
            digest to "e".repeat(64),
            digest.uppercase() to digest,
            digest to "invalid",
        ).forEach { (local, installed) ->
            assertThrows(InteropFailure::class.java) {
                buildAppE2eArtifactIdentity(local, installed)
            }
        }
    }

    @Test
    fun appE2eArtifactHasherStreamsARegularFileAndRejectsEmptyInput() {
        val directory = Files.createTempDirectory("frpc-app-e2e-evidence-test-")
        try {
            val artifact = directory.resolve("app.apk")
            Files.write(artifact, "bounded artifact".toByteArray(StandardCharsets.UTF_8))

            assertEquals(
                "f1b8a495c25bdc02a88c4daf15376b362a09b4db6525d41a255541f9e4013f9f",
                sha256AppE2eArtifact(artifact),
            )
            val empty = directory.resolve("empty.apk")
            Files.createFile(empty)
            assertThrows(InteropFailure::class.java) { sha256AppE2eArtifact(empty) }
        } finally {
            deleteTreeBestEffort(directory)
        }
    }

    @Test
    fun appE2eWorkloadParserUsesBoundedProfileDefaultsAndRejectsInvalidOverrides() {
        assertEquals(
            AppE2eWorkload("baseline", 5, 1, 0L, 0, 1),
            parseAppE2eWorkload("core", "baseline", null, null, null, null, null),
        )
        assertEquals(
            AppE2eWorkload("stress", 20, 4, 0L, 5, 2),
            parseAppE2eWorkload("core", "stress", "20", "4", "0", "5", "2"),
        )
        listOf(
            arrayOf("core", "stress", "not-a-number", null, null, null, null),
            arrayOf("core", "stress", "10", "33", null, null, null),
            arrayOf("network", "baseline", null, null, null, null, null),
            arrayOf("core", "leak", "10", "1", "0", "0", "1"),
            arrayOf("core", "soak", "100", "1", "59999", "10", "1"),
        ).forEach { values ->
            assertThrows(InteropFailure::class.java) {
                parseAppE2eWorkload(
                    values[0]!!,
                    values[1],
                    values[2],
                    values[3],
                    values[4],
                    values[5],
                    values[6],
                )
            }
        }
    }

    @Test
    fun appE2eResourceEvidenceRequiresBoundedMonotonicSamples() {
        val start = resourceSnapshot(elapsed = 100, cpu = 20)
        val sample = resourceSnapshot(elapsed = 150, cpu = 30, sampleOrdinal = 1)
        val end = resourceSnapshot(elapsed = 200, cpu = 40)
        val evidence = AppE2eResourceEvidence(
            start = start,
            visitorReady = start.copy(elapsedRealtimeMillis = 120, processCpuMillis = 25),
            end = end,
            wallDurationMillis = 100,
            cpuDurationMillis = 20,
            samples = listOf(sample),
            droppedSampleCount = 0,
            sampleRetention = resourceRetention(1),
            quiescence = resourceQuiescence(end),
            threadLifecycle = threadLifecycle(bucket = 15, count = 1),
        )
        val workload = AppE2eWorkload("baseline", 1, 1, 0, 0, 1)
        val workloadResult = AppE2eWorkloadResult("baseline", 1, 0)

        assertTrue(isValidAppE2eResourceEvidence(evidence, workload, workloadResult))
        assertFalse(isValidAppE2eResourceEvidence(evidence.copy(wallDurationMillis = 99), workload, workloadResult))
        assertFalse(isValidAppE2eResourceEvidence(evidence.copy(samples = emptyList()), workload, workloadResult))
        assertFalse(isValidAppE2eResourceEvidence(evidence.copy(droppedSampleCount = 1), workload, workloadResult))
        assertFalse(
            isValidAppE2eResourceEvidence(
                evidence.copy(sampleRetention = evidence.sampleRetention.copy(strategy = "legacy")),
                workload,
                workloadResult,
            ),
        )
        assertFalse(isValidAppE2eResourceEvidence(evidence.copy(quiescence = null), workload, workloadResult))
        assertFalse(
            isValidAppE2eResourceEvidence(
                evidence.copy(start = start.copy(threadCount = 2)),
                workload,
                workloadResult,
            ),
        )
        assertFalse(
            isValidAppE2eResourceEvidence(
                evidence,
                workload.copy(iterations = 2),
                workloadResult,
            ),
        )
        assertFalse(
            isValidAppE2eResourceEvidence(
                evidence,
                workload.copy(reconnectEveryIterations = 1),
                workloadResult,
            ),
        )
        assertFalse(
            isValidAppE2eResourceEvidence(
                evidence.copy(samples = listOf(sample, resourceSnapshot(elapsed = 140, cpu = 31))),
                workload,
                workloadResult,
            ),
        )
        assertFalse(
            isValidAppE2eResourceEvidence(
                evidence.copy(samples = List(MAXIMUM_APP_E2E_RESOURCE_SAMPLES + 1) { sample }),
                workload,
                workloadResult,
            ),
        )

        val overflowWorkload = AppE2eWorkload("stress", 1_000, 1, 0, 0, 2)
        val overflowResult = AppE2eWorkloadResult("stress", 1_000, 0)
        val overflowEvidence = evidence.copy(
            samples = expectedSampleOrdinals(500, 127, 127).map { ordinal ->
                sample.copy(sampleOrdinal = ordinal)
            },
            droppedSampleCount = 246,
            sampleRetention = resourceRetention(500),
        )
        assertTrue(isValidAppE2eResourceEvidence(overflowEvidence, overflowWorkload, overflowResult))
        assertFalse(
            isValidAppE2eResourceEvidence(
                overflowEvidence.copy(droppedSampleCount = 245),
                overflowWorkload,
                overflowResult,
            ),
        )
        assertFalse(
            isValidAppE2eResourceEvidence(
                evidence.copy(samples = emptyList()),
                AppE2eWorkload("soak", 100, 1, 60_000, 0, 25),
                AppE2eWorkloadResult("soak", 10, 0),
            ),
        )

        val functionalWorkload = AppE2eWorkload("functional", 0, 1, 0, 0, 1)
        assertTrue(
            isValidAppE2eResourceEvidence(
                evidence.copy(
                    samples = emptyList(),
                    sampleRetention = resourceRetention(0),
                    quiescence = null,
                    visitorReady = null,
                    threadLifecycle = null,
                ),
                functionalWorkload,
                null,
            ),
        )
        assertFalse(isValidAppE2eResourceEvidence(evidence, functionalWorkload, null))
    }

    @Test
    fun appE2eThreadLifecycleRequiresBoundedHistogramAndMatchingCheckpointCounts() {
        val valid = threadLifecycle(bucket = 15, count = 1)

        assertTrue(isValidThreadLifecycle(valid))
        assertFalse(isValidThreadLifecycle(valid.copy(presenceHistogram = List(15) { 0 })))
        assertFalse(isValidThreadLifecycle(valid.copy(presenceHistogram = valid.presenceHistogram.toMutableList().also { it[0] = 1 })))
        assertFalse(isValidThreadLifecycle(valid.copy(presenceHistogram = valid.presenceHistogram.toMutableList().also {
            it[15] = APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT + 1
        })))
        assertFalse(isValidThreadLifecycle(valid.copy(presenceHistogram = MutableList(16) { 0 }.also {
            it[1] = 9_000
            it[2] = 9_000
        })))

        val histogram = MutableList(16) { 0 }.also {
            it[9] = 2
            it[10] = 3
            it[8] = 5
        }
        assertEquals(
            AppE2eThreadLifecycleCohorts(2, 3, 5),
            threadLifecycleCohorts(valid.copy(presenceHistogram = histogram)),
        )
    }

    @Test
    fun appE2eInstrumentationTimeoutCoversWorkloadAndRemainsBounded() {
        assertEquals(
            900_000L,
            appE2eInstrumentationTimeoutMillis(AppE2eWorkload("baseline", 5, 1, 0L, 0, 1)),
        )
        assertEquals(
            29_550_000L,
            appE2eInstrumentationTimeoutMillis(AppE2eWorkload("soak", 100_000, 4, 28_800_000L, 25, 25)),
        )
        assertEquals(
            87_150_000L,
            appE2eInstrumentationTimeoutMillis(AppE2eWorkload("stress", 100_000, 1, 0L, 0, 1)),
        )
    }

    @Test
    fun profilesExposeTheExactScenarioChecks() {
        val expected = mapOf(
            "success" to listOf(
                "open_state",
                "health",
                "global_sse",
                "project_sse",
                "concurrent_rest_sse",
                "closed",
                "port_released",
            ),
            "wrong_token" to listOf(
                "control_rejected",
                "session_failed",
                "listener_not_bound",
                "closed",
                "port_released",
            ),
            "wrong_secret" to listOf(
                "open_state",
                "health_rejected",
                "listener_consistent",
                "closed",
                "port_released",
            ),
            "bind_conflict" to listOf(
                "bind_rejected",
                "visitor_failed",
                "listener_not_bound",
                "closed",
                "port_released",
            ),
            "restart" to listOf(
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
            ),
        )

        androidInteropProfiles("full").forEach { profile ->
            assertEquals(expected.getValue(profile.scenario.kind), profile.expectedChecks)
        }
    }

    @Test
    fun resultValidationRejectsStructuralEchoDrift() {
        val profile = androidInteropProfiles("full")[4]
        val result = passedResult(profile, "gomobile")
        val expectation = AndroidInteropResultExpectation(
            runId = RUN_ID,
            backend = "gomobile",
            profile = profile,
        )

        validateAndroidInteropResult(result, expectation)
        listOf(
            result.copy(caseId = "success-v2-01"),
            result.copy(scenario = AndroidInteropScenario("restart")),
            result.copy(wireProtocol = "v1"),
            result.copy(useEncryption = true),
            result.copy(useCompression = true),
            result.copy(backend = "kotlin"),
            result.copy(semanticOutcome = "recovered"),
        ).forEach { drifted ->
            assertThrows(InteropFailure::class.java) {
                validateAndroidInteropResult(drifted, expectation)
            }
        }
    }

    @Test
    fun resultValidationRequiresPassedStatusExactChecksAndExpectedPort() {
        val profile = androidInteropProfiles("compat").single()
        val result = passedResult(profile, "kotlin")
        val expectation = AndroidInteropResultExpectation(
            runId = RUN_ID,
            backend = "kotlin",
            profile = profile,
            expectedBindPort = BIND_PORT,
        )

        validateAndroidInteropResult(result, expectation)
        listOf(
            result.copy(status = "failed", failedStage = "tunnel_health", failureCode = "TUNNEL_PROBE"),
            result.copy(checks = result.checks.dropLast(1)),
            result.copy(checks = result.checks.reversed()),
            result.copy(bindPort = BIND_PORT + 1),
            result.copy(semanticOutcome = null),
        ).forEach { invalid ->
            assertThrows(InteropFailure::class.java) {
                validateAndroidInteropResult(invalid, expectation)
            }
        }
    }

    @Test
    fun backendEquivalenceRequiresGoMobileThenKotlinWithTheSameSemanticsAndPort() {
        val profile = androidInteropProfiles("full").last()
        val goMobile = passedResult(profile, "gomobile")
        val kotlin = passedResult(profile, "kotlin")

        requireEquivalentAndroidInteropResults(goMobile, kotlin)
        assertThrows(InteropFailure::class.java) {
            requireEquivalentAndroidInteropResults(goMobile, kotlin.copy(bindPort = BIND_PORT + 1))
        }
        assertThrows(InteropFailure::class.java) {
            requireEquivalentAndroidInteropResults(goMobile, kotlin.copy(checks = kotlin.checks.dropLast(1)))
        }
        assertThrows(InteropFailure::class.java) {
            requireEquivalentAndroidInteropResults(goMobile, kotlin.copy(semanticOutcome = "success"))
        }
        assertThrows(InteropFailure::class.java) {
            requireEquivalentAndroidInteropResults(goMobile.copy(backend = "kotlin"), kotlin)
        }
    }

    @Test
    fun suiteSummaryContainsTheExactCompletedProfilesWithoutRuntimeIdentifiers() {
        val profiles = androidInteropProfiles("compat")
        val summary = buildAndroidInteropSuiteSummary(
            suite = "compat",
            apiLevel = 26,
            abi = "x86_64",
            pageSize = 4_096,
            completedProfiles = profiles.map { profile ->
                AndroidInteropCompletedProfile(
                    profile = profile,
                    goMobile = passedResult(profile, "gomobile"),
                    kotlin = passedResult(profile, "kotlin"),
                )
            },
        )

        assertEquals(1, summary.schemaVersion)
        assertEquals("compat", summary.suite)
        assertEquals(AndroidInteropSummaryDevice(26, "x86_64", 4_096), summary.device)
        assertEquals(listOf("gomobile", "kotlin"), summary.backendOrder)
        assertFalse(summary.authorizesKotlinDefault)
        assertEquals(listOf("success-v1-00"), summary.profiles.map { it.caseId })
        assertEquals(
            listOf("gomobile", "kotlin"),
            summary.profiles.single().backends.map { it.backend },
        )
        assertEquals(
            listOf("success", "success"),
            summary.profiles.single().backends.map { it.semanticOutcome },
        )
        assertTrue(summary.profiles.single().equivalent)

        assertThrows(InteropFailure::class.java) {
            buildAndroidInteropSuiteSummary("full", 36, "x86_64", 4_096, emptyList())
        }
    }

    @Test
    fun suiteSummaryWriterPublishesOnceWithoutFollowingAnExistingDestination() {
        val directory = Files.createTempDirectory("frpc-android-summary-test-")
        try {
            val profile = androidInteropProfiles("compat").single()
            val summary = buildAndroidInteropSuiteSummary(
                suite = "compat",
                apiLevel = 26,
                abi = "x86_64",
                pageSize = 4_096,
                completedProfiles = listOf(
                    AndroidInteropCompletedProfile(
                        profile,
                        passedResult(profile, "gomobile"),
                        passedResult(profile, "kotlin"),
                    ),
                ),
            )
            val output = directory.resolve("summary.json")

            writeAndroidInteropSuiteSummary(output, summary)

            assertEquals(
                summary,
                kotlinx.serialization.json.Json.decodeFromString<AndroidInteropSuiteSummary>(
                    String(Files.readAllBytes(output), StandardCharsets.UTF_8),
                ),
            )
            assertThrows(InteropFailure::class.java) {
                writeAndroidInteropSuiteSummary(output, summary)
            }
        } finally {
            deleteTreeBestEffort(directory)
        }
    }

    @Test
    fun restartMarkerPollingIgnoresTransientOutputBeforeAcceptingTheMatchingMarker() {
        val expected = restartMarker()
        val reads = listOf(
            AdbCommandResult(0, "marker is not published yet", ""),
            AdbCommandResult(0, Json.encodeToString(expected), ""),
        )
        var readIndex = 0
        var completionObserved = false

        awaitAndroidInteropRestartMarker(
            expected = expected,
            readMarker = { reads[readIndex++] },
            instrumentationIsDone = { false },
            observeInstrumentation = { completionObserved = true },
            timeoutMillis = 1_000L,
            pollMillis = 1L,
            sleep = {},
        )

        assertEquals(2, readIndex)
        assertFalse(completionObserved)
    }

    @Test
    fun restartMarkerPollingRejectsMatchingSchemaFromAnotherInvocation() {
        val expected = restartMarker()
        val actual = expected.copy(step = "device_disconnected")

        assertThrows(InteropFailure::class.java) {
            awaitAndroidInteropRestartMarker(
                expected = expected,
                readMarker = { AdbCommandResult(0, Json.encodeToString(actual), "") },
                instrumentationIsDone = { false },
                observeInstrumentation = {},
                timeoutMillis = 1_000L,
                pollMillis = 1L,
                sleep = {},
            )
        }
    }

    @Test
    fun restartMarkerPollingFailsClosedWhenInstrumentationCompletesFirst() {
        val expected = restartMarker()
        var completionObserved = false

        assertThrows(InteropFailure::class.java) {
            awaitAndroidInteropRestartMarker(
                expected = expected,
                readMarker = { AdbCommandResult(0, "", "marker unavailable") },
                instrumentationIsDone = { true },
                observeInstrumentation = { completionObserved = true },
                timeoutMillis = 1_000L,
                pollMillis = 1L,
                sleep = {},
            )
        }
        assertTrue(completionObserved)
    }

    @Test
    fun reverseRecoveryReturnsOnlyTheMappingAddedByThisRun() {
        val output = """
            device-serial tcp:41001 tcp:7000
            device-serial tcp:41002 tcp:7000
            device-serial tcp:41003 tcp:8000
        """.trimIndent()

        assertEquals(41_002, recoverOwnedReversePort(output, 7_000, setOf(41_001)))
        assertNull(recoverOwnedReversePort(output, 8_000, setOf(41_003)))
    }

    @Test
    fun reverseRecoveryRejectsAmbiguousNewMappings() {
        val output = """
            device-serial tcp:41001 tcp:7000
            device-serial tcp:41002 tcp:7000
        """.trimIndent()

        assertThrows(InteropFailure::class.java) {
            recoverOwnedReversePort(output, 7_000, emptySet())
        }
    }

    @Test
    fun maximumResourceResultFitsTheHostBound() {
        val snapshot = AppE2eResourceSnapshot(
            elapsedRealtimeMillis = Long.MAX_VALUE,
            processCpuMillis = Long.MAX_VALUE,
            pssKiB = Long.MAX_VALUE,
            javaHeapBytes = Long.MAX_VALUE,
            nativeHeapBytes = Long.MAX_VALUE,
            openFdCount = 16_384,
            threadCount = 16_384,
        )
        val result = AppE2eResult(
            schemaVersion = 9,
            runId = RUN_ID,
            status = "passed",
            checks = List(19) { "bounded_check_$it" },
            backend = "kotlin",
            initialGeneration = Long.MAX_VALUE,
            initialControlEpoch = Long.MAX_VALUE,
            recoveredGeneration = Long.MAX_VALUE,
            recoveredControlEpoch = Long.MAX_VALUE,
            suite = "core",
            topology = "adb_reverse",
            networkSwitchProven = false,
            phase = "single",
            resources = AppE2eResourceEvidence(
                start = snapshot,
                visitorReady = snapshot,
                end = snapshot,
                wallDurationMillis = 0,
                cpuDurationMillis = 0,
                samples = expectedSampleOrdinals(4_000, 127, 127).map { ordinal ->
                    snapshot.copy(sampleOrdinal = ordinal)
                },
                droppedSampleCount = Int.MAX_VALUE,
                sampleRetention = AppE2eResourceSampleRetention(
                    strategy = APP_E2E_RESOURCE_SAMPLE_RETENTION_STRATEGY,
                    observedSampleCount = Int.MAX_VALUE,
                    retainedHeadSampleCount = APP_E2E_RESOURCE_HEAD_SAMPLES,
                    retainedTailSampleCount = APP_E2E_RESOURCE_TAIL_SAMPLES,
                ),
                quiescence = AppE2eResourceQuiescence(
                    requestedDelayMillis = APP_E2E_RESOURCE_QUIESCENCE_MILLIS,
                    snapshot = snapshot,
                ),
                threadLifecycle = threadLifecycle(bucket = 15, count = 16_384),
            ),
            workload = AppE2eWorkloadResult(
                profile = "soak",
                completedIterations = 100_000,
                reconnectCount = 100_000,
            ),
        )

        val byteCount = Json.encodeToString(result).toByteArray(StandardCharsets.UTF_8).size

        assertTrue("maximum App E2E result was $byteCount bytes", byteCount <= 64 * 1024)
    }

    @Test
    fun runtimeDiagnosticsAcceptOnlyAllowlistedAggregateCountKeys() {
        val goRuntimeCounts = listOf(
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
        ).associateWith { 1L }
        val valid = AppE2eRuntimeInitialization(
            requestedDelayMillis = 1,
            coldThreadCount = 1,
            initializedThreadCount = 1,
            initializedRuntimeCounts = goRuntimeCounts + ("app.fd.category.pipe" to 4L),
            postSessionQuiescenceRuntimeCounts = mapOf("app.fd.category.total" to 102),
        )

        assertTrue(
            isValidAppE2eRuntimeInitialization(
                valid.copy(initializedRuntimeCounts = emptyMap(), postSessionQuiescenceRuntimeCounts = emptyMap()),
            ),
        )
        assertTrue(isValidAppE2eRuntimeInitialization(valid))
        assertFalse(
            isValidAppE2eRuntimeInitialization(
                valid.copy(initializedRuntimeCounts = mapOf("app.fd.raw.target" to 1)),
            ),
        )
        assertFalse(
            isValidAppE2eRuntimeInitialization(
                valid.copy(postSessionQuiescenceRuntimeCounts = mapOf("app.fd.category.pipe" to -1)),
            ),
        )
    }

    @Test
    fun recoveryIdentityKeepsKotlinGenerationAndAllowsGoMobileReplacement() {
        assertTrue(isValidAppE2eRecoveryIdentity("kotlin", 2, 3, 2, 4))
        assertFalse(isValidAppE2eRecoveryIdentity("kotlin", 2, 3, 3, 1))
        assertTrue(isValidAppE2eRecoveryIdentity("gomobile", 2, 3, 2, 4))
        assertTrue(isValidAppE2eRecoveryIdentity("gomobile", 2, 3, 3, 1))
        assertFalse(isValidAppE2eRecoveryIdentity("gomobile", 2, 3, 2, 3))
        assertFalse(isValidAppE2eRecoveryIdentity("unknown", 2, 3, 3, 1))
    }

    private fun passedResult(
        profile: AndroidInteropProfile,
        backend: String,
    ): AndroidInteropResult = AndroidInteropResult(
        schemaVersion = 2,
        runId = RUN_ID,
        backend = backend,
        caseId = profile.caseId,
        scenario = profile.scenario,
        wireProtocol = profile.wireProtocol,
        useEncryption = profile.useEncryption,
        useCompression = profile.useCompression,
        status = "passed",
        semanticOutcome = profile.expectedSemanticOutcome,
        bindPort = BIND_PORT,
        checks = profile.expectedChecks,
    )

    private fun restartMarker(): AndroidInteropRestartMarker = AndroidInteropRestartMarker(
        schemaVersion = 1,
        runId = RUN_ID,
        caseId = "restart-v2-11",
        backend = "gomobile",
        step = "device_ready",
    )

    private fun resourceSnapshot(
        elapsed: Long,
        cpu: Long,
        sampleOrdinal: Int? = null,
    ): AppE2eResourceSnapshot = AppE2eResourceSnapshot(
        elapsedRealtimeMillis = elapsed,
        processCpuMillis = cpu,
        pssKiB = 1,
        javaHeapBytes = 1,
        nativeHeapBytes = 1,
        openFdCount = 1,
        threadCount = 1,
        sampleOrdinal = sampleOrdinal,
    )

    private fun resourceRetention(observedCount: Int): AppE2eResourceSampleRetention {
        val headCount = minOf(observedCount, APP_E2E_RESOURCE_HEAD_SAMPLES)
        return AppE2eResourceSampleRetention(
            strategy = APP_E2E_RESOURCE_SAMPLE_RETENTION_STRATEGY,
            observedSampleCount = observedCount,
            retainedHeadSampleCount = headCount,
            retainedTailSampleCount = minOf(
                maxOf(observedCount - headCount, 0),
                APP_E2E_RESOURCE_TAIL_SAMPLES,
            ),
        )
    }

    private fun resourceQuiescence(end: AppE2eResourceSnapshot): AppE2eResourceQuiescence =
        AppE2eResourceQuiescence(
            requestedDelayMillis = APP_E2E_RESOURCE_QUIESCENCE_MILLIS,
            snapshot = end.copy(elapsedRealtimeMillis = end.elapsedRealtimeMillis + APP_E2E_RESOURCE_QUIESCENCE_MILLIS),
        )

    private fun threadLifecycle(bucket: Int, count: Int): AppE2eThreadLifecycle = AppE2eThreadLifecycle(
        schemaVersion = APP_E2E_THREAD_LIFECYCLE_SCHEMA_VERSION,
        classifier = APP_E2E_THREAD_LIFECYCLE_CLASSIFIER,
        captureLimit = APP_E2E_THREAD_LIFECYCLE_CAPTURE_LIMIT,
        presenceHistogram = MutableList(APP_E2E_THREAD_LIFECYCLE_BUCKET_COUNT) { 0 }.also { it[bucket] = count },
    )

    private companion object {
        const val RUN_ID = "00000000-0000-0000-0000-000000000001"
        const val BIND_PORT = 41_234
    }
}
