package io.github.ycfeng.ocdeck.frpcstcpvisitor

import java.net.JarURLConnection
import java.net.URL
import java.nio.file.Files
import java.nio.file.Paths
import java.security.MessageDigest
import java.util.Collections
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class FrpcStcpVisitorFixtureContractTest {
    // This versioned manifest is a closed contract, not a forward-compatible server DTO.
    // Unknown fields must fail so generator schema drift requires an explicit Kotlin update.
    private val strictJson = Json {
        ignoreUnknownKeys = false
        isLenient = false
        coerceInputValues = false
    }
    private val classLoader = FrpcStcpVisitorFixtureContractTest::class.java.classLoader
        ?: throw AssertionError("Test classloader is unavailable")

    @Test
    fun manifestMetadataMatchesThePinnedOfflineContract() {
        val manifest = loadManifest()

        assertEquals(1, manifest.schemaVersion)
        assertEquals("k0-go-oracle-v5", manifest.generatorVersion)
        assertEquals(
            Pins(
                bridge = "0.3.19-frp0.69.1-p6",
                frp = "github.com/fatedier/frp@v0.69.1 (frp-v0.69.1-p6)",
                golib = "github.com/fatedier/golib@v0.7.0",
                snappy = "github.com/golang/snappy@v0.0.4",
                yamux = "github.com/fatedier/yamux@v0.0.0-20250825093530-d0154be01cd6",
            ),
            manifest.pins,
        )
        assertEquals(
            Provenance(
                synthetic = true,
                generator = "frpc-stcp-visitor-go/cmd/contractfixture",
                source =
                    "Deterministic Go oracle over the bridge DTO contract, pinned frp/golib/snappy " +
                        "APIs, and traffic recorded " +
                        "from paired pinned yamux Client/Server sessions over in-memory " +
                        "connections; no captured external traffic.",
                license = "MIT",
                containsCapturedTraffic = false,
                containsRawSecrets = false,
            ),
            manifest.provenance,
        )
        assertEquals(
            Limits(
                wireV1JsonLength = 10_240,
                wireV2FramePayload = 65_536,
                yamuxHeader = 12,
                yamuxInitialWindow = 262_144,
                yamuxFrpStreamWindow = 6_291_456,
            ),
            manifest.limits,
        )
    }

    @Test
    fun entriesExactlyMatchSafeClasspathResourcesAndHashes() {
        val manifestBytes = readResource(MANIFEST_RESOURCE)
        val manifest = decodeManifest(manifestBytes)
        val paths = manifest.entries.map(Entry::path)
        val ids = manifest.entries.map(Entry::id)

        assertTrue("Manifest entries must not be empty", paths.isNotEmpty())
        assertEquals("Manifest entries must be sorted by path", paths.sorted(), paths)
        assertEquals("Manifest entry paths must be unique", paths.size, paths.toSet().size)
        assertEquals("Manifest entry IDs must be unique", ids.size, ids.toSet().size)
        assertEquals("Manifest entry inventory changed", EXPECTED_ENTRY_METADATA.keys, ids.toSet())
        assertEquals(
            "Manifest entry metadata changed",
            EXPECTED_ENTRY_METADATA,
            manifest.entries.associate { entry ->
                entry.id to EntryMetadata(entry.path, entry.layer, entry.expected)
            },
        )

        val contents = mutableMapOf(MANIFEST_FILE to manifestBytes)
        manifest.entries.forEach { entry ->
            assertTrue("Entry ID must not be blank", entry.id.isNotBlank())
            assertTrue("Entry layer must not be blank", entry.layer.isNotBlank())
            assertTrue("Entry expectation must not be blank", entry.expected.isNotBlank())
            assertSafeRelativePath(entry.path)
            assertFalse("Manifest must not hash itself", entry.path == MANIFEST_FILE)
            assertTrue(
                "Entry length must be positive and bounded for ${entry.path}",
                entry.length in 1L..MAX_RESOURCE_BYTES.toLong(),
            )
            assertTrue(
                "Entry SHA-256 must be lowercase hexadecimal for ${entry.path}",
                LOWERCASE_SHA256.matches(entry.sha256),
            )

            val bytes = readResource("$CONTRACT_ROOT/${entry.path}")
            assertEquals("Length mismatch for ${entry.path}", entry.length, bytes.size.toLong())
            assertEquals("SHA-256 mismatch for ${entry.path}", entry.sha256, sha256(bytes))
            contents[entry.path] = bytes
        }

        assertEquals(
            "Classpath contract resources must exactly match the manifest",
            paths.toSet() + MANIFEST_FILE,
            listContractResources(manifestUrl()),
        )
        contents.forEach { (path, bytes) -> assertContainsNoRawCredentials(path, bytes) }
    }

    @Test
    fun chunkPlansAndMutationsReferenceDeclaredFixtures() {
        val manifest = loadManifest()
        val entriesByPath = manifest.entries.associateBy(Entry::path)

        assertSortedUniqueIds("chunk plan", manifest.chunkPlans.map(ChunkPlan::id))
        assertEquals("Manifest chunk-plan metadata changed", EXPECTED_CHUNK_PLANS, manifest.chunkPlans)
        manifest.chunkPlans.forEach { plan ->
            assertTrue("Chunk plan source must not be blank", plan.source.isNotBlank())
            assertTrue("Chunk plan expectation must not be blank", plan.expected.isNotBlank())
            assertTrue("Chunk plan sizes must not be empty", plan.sizes.isNotEmpty())
            val source = entriesByPath[plan.source]
                ?: throw AssertionError("Chunk plan ${plan.id} references an undeclared fixture")

            plan.sizes.forEach { size ->
                assertTrue("Chunk sizes must be positive for ${plan.id}", size > 0)
                assertTrue(
                    "A chunk size must not exceed its source for ${plan.id}",
                    size.toLong() <= source.length,
                )
            }
            val cycleSize = plan.sizes.sumOf { size -> size.toLong() }
            assertTrue(
                "One chunk-size cycle must fit within its source for ${plan.id}",
                cycleSize in 1L..source.length,
            )
        }

        assertSortedUniqueIds("mutation", manifest.mutations.map(Mutation::id))
        assertEquals("Manifest mutation metadata changed", EXPECTED_MUTATIONS, manifest.mutations)
        manifest.mutations.forEach { mutation ->
            assertTrue("Mutation source must not be blank", mutation.source.isNotBlank())
            assertTrue("Mutation operation must not be blank", mutation.operation.isNotBlank())
            assertTrue("Mutation expectation must not be blank", mutation.expected.isNotBlank())
            assertTrue(
                "Mutation ${mutation.id} uses an unsupported recipe operation",
                mutation.operation in MUTATION_OPERATIONS,
            )
            val source = entriesByPath[mutation.source]
                ?: throw AssertionError("Mutation ${mutation.id} references an undeclared fixture")

            assertMutationParameters(mutation, source)
        }
    }

    private fun loadManifest(): Manifest = decodeManifest(readResource(MANIFEST_RESOURCE))

    private fun decodeManifest(bytes: ByteArray): Manifest =
        strictJson.decodeFromString(bytes.decodeToString(throwOnInvalidSequence = true))

    private fun readResource(path: String): ByteArray {
        val stream = classLoader.getResourceAsStream(path)
            ?: throw AssertionError("Classpath resource is missing: $path")
        return stream.use {
            val bytes = it.readNBytes(MAX_RESOURCE_BYTES + 1)
            assertTrue("Classpath resource exceeds the offline fixture limit: $path", bytes.size <= MAX_RESOURCE_BYTES)
            bytes
        }
    }

    private fun manifestUrl(): URL {
        val urls = Collections.list(classLoader.getResources(MANIFEST_RESOURCE))
        assertEquals("Classpath must contain exactly one contract manifest", 1, urls.size)
        return urls.single()
    }

    private fun listContractResources(url: URL): Set<String> = when (url.protocol) {
        "file" -> {
            val root = Paths.get(url.toURI()).parent
                ?: throw AssertionError("Classpath manifest has no resource root")
            buildSet {
                Files.walk(root).use { paths ->
                    paths.filter { path -> Files.isRegularFile(path) }
                        .forEach { path ->
                            add(root.relativize(path).toString().replace('\\', '/'))
                        }
                }
            }
        }

        "jar" -> {
            val connection = url.openConnection() as? JarURLConnection
                ?: throw AssertionError("Classpath manifest does not use a readable jar connection")
            connection.useCaches = false
            connection.jarFile.use { jar ->
                buildSet {
                    val prefix = "$CONTRACT_ROOT/"
                    val entries = jar.entries()
                    while (entries.hasMoreElements()) {
                        val entry = entries.nextElement()
                        if (!entry.isDirectory && entry.name.startsWith(prefix)) {
                            add(entry.name.removePrefix(prefix))
                        }
                    }
                }
            }
        }

        else -> throw AssertionError("Unsupported classpath resource protocol: ${url.protocol}")
    }

    private fun assertSafeRelativePath(path: String) {
        assertTrue("Fixture path must not be blank", path.isNotBlank())
        assertFalse("Fixture path must not contain NUL", path.contains('\u0000'))
        assertFalse("Fixture path must not contain backslashes: $path", path.contains('\\'))
        assertFalse("Fixture path must not be POSIX-absolute: $path", path.startsWith('/'))
        assertFalse("Fixture path must not be Windows-absolute: $path", WINDOWS_DRIVE_PATH.containsMatchIn(path))
        assertTrue(
            "Fixture path must be normalized and traversal-free: $path",
            path.split('/').all { segment -> segment.isNotEmpty() && segment != "." && segment != ".." },
        )
    }

    private fun assertSortedUniqueIds(label: String, ids: List<String>) {
        assertTrue("$label IDs must not be empty", ids.isNotEmpty())
        assertTrue("$label IDs must not be blank", ids.all(String::isNotBlank))
        assertEquals("$label IDs must be sorted", ids.sorted(), ids)
        assertEquals("$label IDs must be unique", ids.size, ids.toSet().size)
    }

    private fun assertMutationParameters(mutation: Mutation, source: Entry) {
        fun requireOffset(width: Int): Int {
            val offset = mutation.offset
                ?: throw AssertionError("Mutation ${mutation.id} requires an offset")
            assertTrue(
                "Mutation range must fit within its source for ${mutation.id}",
                offset >= 0 && offset.toLong() + width <= source.length,
            )
            return offset
        }

        fun assertNoParameters() {
            assertEquals("Unexpected offset for ${mutation.id}", null, mutation.offset)
            assertEquals("Unexpected byte value for ${mutation.id}", null, mutation.byteValue)
            assertEquals("Unexpected integer value for ${mutation.id}", null, mutation.intValue)
            assertEquals("Unexpected count for ${mutation.id}", null, mutation.count)
            assertEquals("Unexpected mask for ${mutation.id}", null, mutation.mask)
        }

        when (mutation.operation) {
            "xor-byte" -> {
                requireOffset(1)
                val mask = mutation.mask
                    ?: throw AssertionError("Mutation ${mutation.id} requires a mask")
                assertTrue("Mutation mask must be non-zero for ${mutation.id}", mask in 1..0xff)
                assertEquals(null, mutation.byteValue)
                assertEquals(null, mutation.intValue)
                assertEquals(null, mutation.count)
            }

            "set-byte" -> {
                requireOffset(1)
                val value = mutation.byteValue
                    ?: throw AssertionError("Mutation ${mutation.id} requires a byte value")
                assertTrue("Mutation byte value is invalid for ${mutation.id}", value in 0..0xff)
                assertEquals(null, mutation.intValue)
                assertEquals(null, mutation.count)
                assertEquals(null, mutation.mask)
            }

            "set-int64-be" -> {
                requireOffset(Long.SIZE_BYTES)
                mutation.intValue
                    ?: throw AssertionError("Mutation ${mutation.id} requires an integer value")
                assertEquals(null, mutation.byteValue)
                assertEquals(null, mutation.count)
                assertEquals(null, mutation.mask)
            }

            "set-uint16-be" -> {
                requireOffset(Short.SIZE_BYTES)
                val value = mutation.intValue
                    ?: throw AssertionError("Mutation ${mutation.id} requires an integer value")
                assertTrue("Mutation uint16 value is invalid for ${mutation.id}", value in 0..0xffff)
                assertEquals(null, mutation.byteValue)
                assertEquals(null, mutation.count)
                assertEquals(null, mutation.mask)
            }

            "set-uint32-be" -> {
                requireOffset(Int.SIZE_BYTES)
                val value = mutation.intValue
                    ?: throw AssertionError("Mutation ${mutation.id} requires an integer value")
                assertTrue("Mutation uint32 value is invalid for ${mutation.id}", value in 0..0xffff_ffffL)
                assertEquals(null, mutation.byteValue)
                assertEquals(null, mutation.count)
                assertEquals(null, mutation.mask)
            }

            "truncate-tail" -> {
                val count = mutation.count
                    ?: throw AssertionError("Mutation ${mutation.id} requires a count")
                assertTrue(
                    "Mutation count must be positive and bounded for ${mutation.id}",
                    count > 0 && count.toLong() < source.length,
                )
                assertEquals(null, mutation.offset)
                assertEquals(null, mutation.byteValue)
                assertEquals(null, mutation.intValue)
                assertEquals(null, mutation.mask)
            }

            "swap-aead-records", "wrong-aead-role" -> assertNoParameters()
        }
    }

    private fun assertContainsNoRawCredentials(path: String, bytes: ByteArray) {
        val searchable = bytes.toString(Charsets.ISO_8859_1)
        RAW_SYNTHETIC_CREDENTIALS.forEach { credential ->
            assertFalse("Resource contains a raw synthetic credential: $path", searchable.contains(credential))
        }
        val withoutAllowedRedactions = REDACTED_SENSITIVE_KEY_VALUE.replace(searchable, "")
        assertFalse(
            "Resource contains a common sensitive key-value pair: $path",
            SENSITIVE_KEY_VALUE.containsMatchIn(withoutAllowedRedactions),
        )
        assertFalse(
            "Resource contains private-key material: $path",
            PRIVATE_KEY_HEADER.containsMatchIn(searchable),
        )
    }

    private fun sha256(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256")
            .digest(bytes)
            .joinToString("") { byte -> "%02x".format(byte) }

    @Serializable
    private data class Manifest(
        val schemaVersion: Int,
        val generatorVersion: String,
        val pins: Pins,
        val provenance: Provenance,
        val limits: Limits,
        val entries: List<Entry>,
        val chunkPlans: List<ChunkPlan>,
        val mutations: List<Mutation>,
    )

    @Serializable
    private data class Pins(
        val bridge: String,
        val frp: String,
        val golib: String,
        val snappy: String,
        val yamux: String,
    )

    @Serializable
    private data class Provenance(
        val synthetic: Boolean,
        val generator: String,
        val source: String,
        val license: String,
        val containsCapturedTraffic: Boolean,
        val containsRawSecrets: Boolean,
    )

    @Serializable
    private data class Limits(
        val wireV1JsonLength: Long,
        val wireV2FramePayload: Long,
        val yamuxHeader: Int,
        val yamuxInitialWindow: Long,
        val yamuxFrpStreamWindow: Long,
    )

    @Serializable
    private data class Entry(
        val id: String,
        val path: String,
        val layer: String,
        val length: Long,
        val sha256: String,
        val expected: String,
    )

    private data class EntryMetadata(
        val path: String,
        val layer: String,
        val expected: String,
    )

    @Serializable
    private data class ChunkPlan(
        val id: String,
        val source: String,
        val sizes: List<Int>,
        val expected: String,
    )

    @Serializable
    private data class Mutation(
        val id: String,
        val source: String,
        val operation: String,
        val offset: Int? = null,
        val byteValue: Int? = null,
        val intValue: Long? = null,
        val count: Int? = null,
        val mask: Int? = null,
        val expected: String,
    )

    private companion object {
        const val CONTRACT_ROOT = "io/github/ycfeng/ocdeck/frpcstcpvisitor/contract/v1"
        const val MANIFEST_FILE = "manifest.json"
        const val MANIFEST_RESOURCE = "$CONTRACT_ROOT/$MANIFEST_FILE"
        const val MAX_RESOURCE_BYTES = 1 shl 20

        val LOWERCASE_SHA256 = Regex("[0-9a-f]{64}")
        val WINDOWS_DRIVE_PATH = Regex("^[A-Za-z]:")
        val RAW_SYNTHETIC_CREDENTIALS = listOf(
            "ocdeck-k0-obviously-synthetic-token",
            "ocdeck-k0-obviously-synthetic-stcp-secret",
        )
        val MUTATION_OPERATIONS = setOf(
            "xor-byte",
            "set-byte",
            "set-int64-be",
            "set-uint16-be",
            "set-uint32-be",
            "truncate-tail",
            "swap-aead-records",
            "wrong-aead-role",
        )
        val EXPECTED_ENTRY_METADATA = mapOf(
            "bridge-dto-contract" to EntryMetadata(
                "bridge/dto-contract.json",
                "bridge-json",
                "Go/Kotlin DTO decode and encode semantics match",
            ),
            "snappy-cfb-framed-compressed" to EntryMetadata(
                "compression/snappy/cfb/compressed.bin",
                "snappy-cfb-framed",
                "wire=CFB(Snappy framed); decode-order=CFB-then-Snappy",
            ),
            "snappy-framed-boundary-65537" to EntryMetadata(
                "compression/snappy/framed/boundary-65537.bin",
                "snappy-framed",
                "chunks=compressed,uncompressed; decode=repeat-byte(0x61,65537)",
            ),
            "snappy-framed-compressed" to EntryMetadata(
                "compression/snappy/framed/compressed.bin",
                "snappy-framed",
                "chunks=compressed; decode=repeat-byte(0x61,20)",
            ),
            "snappy-framed-uncompressed" to EntryMetadata(
                "compression/snappy/framed/uncompressed.bin",
                "snappy-framed",
                "chunks=uncompressed; decode=byte-sequence-mod-256(256)",
            ),
            "snappy-raw-compressed" to EntryMetadata(
                "compression/snappy/raw/compressed.bin",
                "snappy-raw",
                "decode=repeat-byte(0x61,20)",
            ),
            "control-v1-client-cfb" to EntryMetadata(
                "control/v1/cfb/client-to-server.bin",
                "control-v1-cfb",
                "decrypt=client-to-server plaintext",
            ),
            "control-v1-server-cfb" to EntryMetadata(
                "control/v1/cfb/server-to-client.bin",
                "control-v1-cfb",
                "decrypt=server-to-client plaintext",
            ),
            "control-v1-client-plain" to EntryMetadata(
                "control/v1/plain/client-to-server.bin",
                "control-v1-plaintext",
                "messages=Ping,NewWorkConn",
            ),
            "control-v1-server-plain" to EntryMetadata(
                "control/v1/plain/server-to-client.bin",
                "control-v1-plaintext",
                "messages=Pong,ReqWorkConn",
            ),
            "control-v2-aes-256-gcm-client" to EntryMetadata(
                "control/v2/aes-256-gcm/client-to-server.bin",
                "control-v2-aead",
                "decrypt=server-role; integrity=authenticated",
            ),
            "control-v2-aes-256-gcm-server" to EntryMetadata(
                "control/v2/aes-256-gcm/server-to-client.bin",
                "control-v2-aead",
                "decrypt=client-role; integrity=authenticated",
            ),
            "control-v2-client-plain" to EntryMetadata(
                "control/v2/plain/client-to-server.bin",
                "control-v2-plaintext",
                "messages=Ping,NewWorkConn",
            ),
            "control-v2-server-plain" to EntryMetadata(
                "control/v2/plain/server-to-client.bin",
                "control-v2-plaintext",
                "messages=Pong,ReqWorkConn",
            ),
            "control-v2-xchacha20-poly1305-client" to EntryMetadata(
                "control/v2/xchacha20-poly1305/client-to-server.bin",
                "control-v2-aead",
                "decrypt=server-role; integrity=authenticated",
            ),
            "control-v2-xchacha20-poly1305-server" to EntryMetadata(
                "control/v2/xchacha20-poly1305/server-to-client.bin",
                "control-v2-aead",
                "decrypt=client-role; integrity=authenticated",
            ),
            "wire-v1-login-response" to EntryMetadata(
                "wire/v1/login-response.bin",
                "wire-v1",
                "decode-login-response; run-id=present; error=empty",
            ),
            "wire-v1-login-token" to EntryMetadata(
                "wire/v1/login-token.bin",
                "wire-v1",
                "decode-login; token-auth=valid",
            ),
            "wire-v1-new-visitor-conn-response" to EntryMetadata(
                "wire/v1/new-visitor-conn-response.bin",
                "wire-v1",
                "decode-new-visitor-conn-response; proxy=present; error=empty",
            ),
            "wire-v1-new-visitor-stcp" to EntryMetadata(
                "wire/v1/new-visitor-stcp.bin",
                "wire-v1",
                "decode-new-visitor; stcp-auth=valid",
            ),
            "wire-v1-start-work-conn" to EntryMetadata(
                "wire/v1/start-work-conn.bin",
                "wire-v1",
                "decode-start-work-conn; proxy-addresses-ports=present; error=empty",
            ),
            "wire-v2-aes-256-gcm-client-bootstrap" to EntryMetadata(
                "wire/v2/aes-256-gcm/client-bootstrap.bin",
                "wire-v2-bootstrap",
                "magic; client-hello; login; token-auth=valid",
            ),
            "wire-v2-aes-256-gcm-server-bootstrap" to EntryMetadata(
                "wire/v2/aes-256-gcm/server-bootstrap.bin",
                "wire-v2-bootstrap",
                "server-hello; login-response",
            ),
            "wire-v2-new-visitor-conn-response" to EntryMetadata(
                "wire/v2/new-visitor-conn-response.bin",
                "wire-v2",
                "decode-new-visitor-conn-response; proxy=present; error=empty",
            ),
            "wire-v2-new-visitor-stcp" to EntryMetadata(
                "wire/v2/new-visitor-stcp.bin",
                "wire-v2",
                "magic; decode-new-visitor; stcp-auth=valid",
            ),
            "wire-v2-start-work-conn" to EntryMetadata(
                "wire/v2/start-work-conn.bin",
                "wire-v2",
                "decode-start-work-conn; proxy-addresses-ports=present; error=empty",
            ),
            "wire-v2-xchacha20-poly1305-client-bootstrap" to EntryMetadata(
                "wire/v2/xchacha20-poly1305/client-bootstrap.bin",
                "wire-v2-bootstrap",
                "magic; client-hello; login; token-auth=valid",
            ),
            "wire-v2-xchacha20-poly1305-server-bootstrap" to EntryMetadata(
                "wire/v2/xchacha20-poly1305/server-bootstrap.bin",
                "wire-v2-bootstrap",
                "server-hello; login-response",
            ),
            "yamux-client-to-server-trace" to EntryMetadata(
                "yamux/client-to-server.trace.bin",
                "yamux",
                "recorded pinned Client/Server lifecycle; frames=9; odd client SYN; DATA; FIN; Ping API; GOAWAY",
            ),
            "yamux-flow-control-client-to-server-trace" to EntryMetadata(
                "yamux/flow-control/client-to-server.trace.bin",
                "yamux",
                "recorded pinned Client/Server flow control; frames=3; payload consumes half the receive window",
            ),
            "yamux-flow-control-server-to-client-trace" to EntryMetadata(
                "yamux/flow-control/server-to-client.trace.bin",
                "yamux",
                "recorded pinned Client/Server flow control; frames=3; ACK; WINDOW_UPDATE; FIN",
            ),
            "yamux-reset-client-to-server-trace" to EntryMetadata(
                "yamux/reset/client-to-server.trace.bin",
                "yamux",
                "recorded pinned Client/Server backlog reset; frames=3; two SYN attempts; surviving stream FIN",
            ),
            "yamux-reset-server-to-client-trace" to EntryMetadata(
                "yamux/reset/server-to-client.trace.bin",
                "yamux",
                "recorded pinned Client/Server backlog reset; frames=3; RST error; surviving stream ACK and FIN",
            ),
            "yamux-server-to-client-trace" to EntryMetadata(
                "yamux/server-to-client.trace.bin",
                "yamux",
                "recorded pinned Client/Server lifecycle; frames=9; even server SYN; DATA; FIN; Ping API; GOAWAY",
            ),
        )
        val EXPECTED_CHUNK_PLANS = listOf(
            ChunkPlan(
                "control-v2-aes-record-splits",
                "control/v2/aes-256-gcm/client-to-server.bin",
                listOf(1, 3, 2, 7),
                "decrypt-success",
            ),
            ChunkPlan(
                "snappy-cfb-framed-splits",
                "compression/snappy/cfb/compressed.bin",
                listOf(1, 2, 3, 5, 8),
                "decrypt-then-decompress-success",
            ),
            ChunkPlan("wire-v1-bytewise", "wire/v1/login-token.bin", listOf(1), "decode-success"),
            ChunkPlan(
                "wire-v2-frame-splits",
                "wire/v2/aes-256-gcm/client-bootstrap.bin",
                listOf(1, 2, 3, 5, 8),
                "decode-success",
            ),
            ChunkPlan(
                "yamux-flow-control-splits",
                "yamux/flow-control/client-to-server.trace.bin",
                listOf(1, 11, 4096, 3),
                "parse-success",
            ),
            ChunkPlan(
                "yamux-lifecycle-splits",
                "yamux/client-to-server.trace.bin",
                listOf(1, 11, 2, 5),
                "parse-success",
            ),
            ChunkPlan(
                "yamux-reset-splits",
                "yamux/reset/server-to-client.trace.bin",
                listOf(2, 1, 9, 4),
                "parse-success",
            ),
        )
        val EXPECTED_MUTATIONS = listOf(
            Mutation(
                "control-v1-cfb-bit-flip",
                "control/v1/cfb/client-to-server.bin",
                "xor-byte",
                offset = 209,
                mask = 1,
                expected = "accept-altered",
            ),
            Mutation(
                "control-v2-aes-bit-flip",
                "control/v2/aes-256-gcm/client-to-server.bin",
                "xor-byte",
                offset = 248,
                mask = 1,
                expected = "integrity-failure",
            ),
            Mutation(
                "control-v2-aes-record-reorder",
                "control/v2/aes-256-gcm/client-to-server.bin",
                "swap-aead-records",
                expected = "integrity-failure",
            ),
            Mutation(
                "control-v2-aes-truncated",
                "control/v2/aes-256-gcm/client-to-server.bin",
                "truncate-tail",
                count = 1,
                expected = "reject-truncated",
            ),
            Mutation(
                "control-v2-aes-wrong-role",
                "control/v2/aes-256-gcm/server-to-client.bin",
                "wrong-aead-role",
                expected = "integrity-failure",
            ),
            Mutation(
                "control-v2-xchacha-bit-flip",
                "control/v2/xchacha20-poly1305/client-to-server.bin",
                "xor-byte",
                offset = 260,
                mask = 1,
                expected = "integrity-failure",
            ),
            Mutation(
                "control-v2-xchacha-record-reorder",
                "control/v2/xchacha20-poly1305/client-to-server.bin",
                "swap-aead-records",
                expected = "integrity-failure",
            ),
            Mutation(
                "control-v2-xchacha-wrong-role",
                "control/v2/xchacha20-poly1305/server-to-client.bin",
                "wrong-aead-role",
                expected = "integrity-failure",
            ),
            Mutation(
                "wire-v1-length-max-plus-one",
                "wire/v1/login-token.bin",
                "set-int64-be",
                offset = 1,
                intValue = 10_241,
                expected = "reject-over-limit",
            ),
            Mutation(
                "wire-v1-negative-length",
                "wire/v1/login-token.bin",
                "set-int64-be",
                offset = 1,
                intValue = -1,
                expected = "reject-negative-length",
            ),
            Mutation(
                "wire-v1-truncated",
                "wire/v1/login-token.bin",
                "truncate-tail",
                count = 1,
                expected = "reject-truncated",
            ),
            Mutation(
                "wire-v1-unknown-type",
                "wire/v1/login-token.bin",
                "set-byte",
                offset = 0,
                byteValue = 255,
                expected = "reject-unknown-type",
            ),
            Mutation(
                "wire-v2-error-flags",
                "wire/v2/aes-256-gcm/client-bootstrap.bin",
                "set-uint16-be",
                offset = 9,
                intValue = 1,
                expected = "reject-flags",
            ),
            Mutation(
                "wire-v2-payload-max-plus-one",
                "wire/v2/aes-256-gcm/client-bootstrap.bin",
                "set-uint32-be",
                offset = 11,
                intValue = 65_537,
                expected = "reject-over-limit",
            ),
            Mutation(
                "wire-v2-truncated",
                "wire/v2/aes-256-gcm/client-bootstrap.bin",
                "truncate-tail",
                count = 1,
                expected = "reject-truncated",
            ),
            Mutation(
                "wire-v2-unknown-message-type",
                "wire/v2/new-visitor-stcp.bin",
                "set-uint16-be",
                offset = 15,
                intValue = 65_535,
                expected = "reject-unknown-type",
            ),
            Mutation(
                "yamux-data-bit-flip",
                "yamux/client-to-server.trace.bin",
                "xor-byte",
                offset = 24,
                mask = 1,
                expected = "accept-altered",
            ),
            Mutation(
                "yamux-unknown-type",
                "yamux/client-to-server.trace.bin",
                "set-byte",
                offset = 1,
                byteValue = 255,
                expected = "reject-unknown-type",
            ),
        )
        private const val SENSITIVE_KEY =
            "token|secret|password|passwd|passphrase|authorization|cookie|set-cookie|credential|" +
                "auth[_-]?token|access[_-]?token|refresh[_-]?token|api[_-]?key|client[_-]?secret|" +
                "secret[_-]?key|stcp[_-]?secret|private[_-]?key"
        val REDACTED_SENSITIVE_KEY_VALUE = Regex(
            """(?i)(?:^|[\s?&;,\{\[])"?(?:$SENSITIVE_KEY)"?\s*[:=]\s*"?<redacted>"?(?=[\s,;}\]]|$)""",
        )
        val SENSITIVE_KEY_VALUE = Regex(
            """(?i)(?:^|[\s?&;,\{\[])"?(?:$SENSITIVE_KEY)"?\s*[:=]\s*(?!"?<redacted>"?(?:[\s,;}\]]|$))(?:"[^"\r\n]+"|[^\s,;&}\]]+)""",
        )
        val PRIVATE_KEY_HEADER = Regex(
            """-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----""",
            RegexOption.IGNORE_CASE,
        )
    }
}
