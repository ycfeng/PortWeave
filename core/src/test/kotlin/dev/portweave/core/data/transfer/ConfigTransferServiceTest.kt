package dev.portweave.core.data.transfer

import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.SshCredentialRefs
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.core.security.CredentialRef
import dev.portweave.core.security.CredentialKind
import dev.portweave.core.security.CredentialStore
import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.UUID
import java.util.zip.GZIPOutputStream
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ConfigTransferServiceTest {
    @Test
    fun exportOmitsEveryCredentialReferenceAndDeclaresNoSecrets() {
        val refs = List(4) { CredentialRef(UUID.randomUUID()) }
        val config = config("secure").copy(
            spec = (config("secure").spec as TunnelSpec.Ssh).copy(
                credentials = SshCredentialRefs(refs[0], refs[1], refs[2], refs[3]),
            ),
        )
        val json = ConfigTransferService(FakeRepository()).export(listOf(config))

        assertTrue(json.contains("\"containsSecrets\": false"))
        refs.forEach { assertFalse(json.contains(it.value.toString())) }
        assertFalse(json.contains("passwordRef"))
        assertFalse(json.contains("privateKeyRef"))
        val privateJson = ConfigTransferService(FakeRepository()).export(listOf(config), privacyMode = true)
        assertFalse(privateJson.contains("example.invalid"))
        assertFalse(privateJson.contains("target.invalid"))
        assertFalse(privateJson.contains("\"user\""))
    }

    @Test
    fun createCopyRewritesInternalDependenciesAndCommitsOneBatch() = runTest {
        val parent = config("parent")
        val child = config("child").copy(
            spec = (config("child").spec as TunnelSpec.Ssh).copy(endpoint = EndpointSource.Dependency(parent.id)),
        )
        val repository = FakeRepository(parent)
        val transfer = ConfigTransferService(repository, nowEpochMillis = { 42L })
        val result = transfer.import(transfer.export(listOf(parent, child)), ImportConflictStrategy.CREATE_COPY)

        assertEquals(2, result.imported)
        assertEquals(1, repository.batchCalls)
        val importedParent = repository.lastBatch.single { it.name == "parent" }
        val importedChild = repository.lastBatch.single { it.name == "child" }
        assertNotEquals(parent.id, importedParent.id)
        assertEquals(importedParent.id, (importedChild.spec as TunnelSpec.Ssh).endpoint.let { (it as EndpointSource.Dependency).configId })
    }

    @Test
    fun cyclicPackageIsRejectedBeforeRepositoryMutation() = runTest {
        val first = config("first")
        val second = config("second")
        val cyclicFirst = first.copy(spec = (first.spec as TunnelSpec.Ssh).copy(endpoint = EndpointSource.Dependency(second.id)))
        val cyclicSecond = second.copy(spec = (second.spec as TunnelSpec.Ssh).copy(endpoint = EndpointSource.Dependency(first.id)))
        val repository = FakeRepository()
        val transfer = ConfigTransferService(repository)

        val failure = runCatching {
            transfer.import(transfer.export(listOf(cyclicFirst, cyclicSecond)), ImportConflictStrategy.CREATE_COPY)
        }.exceptionOrNull()

        assertTrue(failure != null)
        assertEquals(0, repository.batchCalls)
    }

    @Test
    fun replaceByNameKeepsLocalId() = runTest {
        val local = config("same")
        val incoming = config("same").copy(description = "new")
        val repository = FakeRepository(local)
        val transfer = ConfigTransferService(repository)

        transfer.import(transfer.export(listOf(incoming)), ImportConflictStrategy.REPLACE_LOCAL)

        assertEquals(setOf(local.id), repository.lastReplaceIds)
        assertEquals(local.id, repository.lastBatch.single().id)
    }

    @Test
    fun compactExportRemainsImportableForQrCodes() = runTest {
        val source = config("qr-share")
        val repository = FakeRepository()
        val transfer = ConfigTransferService(repository)

        val compact = transfer.export(listOf(source), prettyPrint = false)
        val result = transfer.import(compact, ImportConflictStrategy.CREATE_COPY)

        assertFalse(compact.contains('\n'))
        assertEquals(1, result.imported)
        assertEquals(source.name, repository.lastBatch.single().name)
    }

    @Test
    fun qrPayloadRoundTripsGzipBase45() {
        val json = """{"value":"${"repeat".repeat(200)}"}"""

        val encoded = QrConfigPayloadCodec.encode(json)

        assertTrue(encoded.startsWith("PORTWEAVE:2:"))
        assertTrue(encoded.all { it in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ $%*+-./:" })
        assertFalse(encoded.contains(json))
        assertEquals(json, QrConfigPayloadCodec.decodeIfEncoded(encoded))
    }

    @Test
    fun qrPayloadStillDecodesLegacyV1() {
        val json = """{"legacy":true}"""

        assertEquals(json, QrConfigPayloadCodec.decodeIfEncoded(legacyQrPayload(json)))
    }

    @Test
    fun compactV2NeedsFewerQrDataBitsThanLegacyV1() {
        val json = ConfigTransferService(FakeRepository(), nowEpochMillis = { 1_700_000_000_000L })
            .export(listOf(config("qr-size")), prettyPrint = false)
        val legacy = legacyQrPayload(json)
        val compact = QrConfigPayloadCodec.encode(json)

        assertTrue(compact.length * 5.5 < legacy.length * 8.0)
    }

    @Test
    fun compactV2PreservesAndRewritesInternalDependencies() = runTest {
        val parent = config("qr-parent")
        val child = config("qr-child").copy(
            spec = (config("qr-child").spec as TunnelSpec.Ssh).copy(
                endpoint = EndpointSource.Dependency(parent.id),
            ),
        )
        val target = FakeRepository()
        val transfer = ConfigTransferService(target)

        val result = transfer.import(
            QrConfigPayloadCodec.encode(transfer.export(listOf(parent, child), prettyPrint = false)),
            ImportConflictStrategy.CREATE_COPY,
        )

        val importedParent = target.lastBatch.single { it.name == parent.name }
        val importedChild = target.lastBatch.single { it.name == child.name }
        assertEquals(2, result.imported)
        assertEquals(
            importedParent.id,
            ((importedChild.spec as TunnelSpec.Ssh).endpoint as EndpointSource.Dependency).configId,
        )
    }

    @Test
    fun compactV2RoundTripsStcpConfiguration() = runTest {
        val source = TunnelConfig.empty(TunnelKind.STCP, "stcp-qr", 30142).copy(
            spec = TunnelSpec.Stcp(
                endpoint = EndpointSource.Remote("frps.example", 7000),
                identity = dev.portweave.core.model.FrpsServerIdentity("frps.example", 7000),
                user = "client-user",
                serverUser = "server-user",
                serverName = "service-name",
                wireProtocol = dev.portweave.core.model.StcpWireProtocol.V2,
                useEncryption = true,
                useCompression = true,
                tlsLeafPinSha256 = "SHA256:pin",
                connectTimeoutSeconds = 20,
            ),
        )
        val target = FakeRepository()
        val transfer = ConfigTransferService(target)

        val result = transfer.import(
            QrConfigPayloadCodec.encode(transfer.export(listOf(source), prettyPrint = false)),
            ImportConflictStrategy.CREATE_COPY,
        )

        assertEquals(1, result.imported)
        assertEquals(source.spec, target.lastBatch.single().spec)
    }

    @Test
    fun base45MatchesRfcExample() {
        assertEquals("BB8", Base45.encode("AB".toByteArray()))
        assertEquals("AB", Base45.decode("BB8").decodeToString())
    }

    @Test
    fun sensitiveQrExportRestoresCredentialsIntoTargetStore() = runTest {
        val passwordRef = CredentialRef(UUID.randomUUID())
        val fingerprintRef = CredentialRef(UUID.randomUUID())
        val source = config("sensitive").let { base ->
            base.copy(spec = (base.spec as TunnelSpec.Ssh).copy(
                credentials = SshCredentialRefs(password = passwordRef, hostFingerprint = fingerprintRef),
            ))
        }
        val sourceStore = FakeCredentialStore(mapOf(
            passwordRef to "secret-password".toByteArray(),
            fingerprintRef to "SHA256:fingerprint".toByteArray(),
        ))
        val json = ConfigTransferService(FakeRepository(), sourceStore)
            .exportIncludingSecrets(listOf(source), prettyPrint = false)
        val targetRepository = FakeRepository()
        val targetStore = FakeCredentialStore()

        val result = ConfigTransferService(targetRepository, targetStore).import(
            QrConfigPayloadCodec.encode(json),
            ImportConflictStrategy.CREATE_COPY,
        )

        val refs = (targetRepository.lastBatch.single().spec as TunnelSpec.Ssh).credentials
        assertTrue(json.contains("\"containsSecrets\":true"))
        assertEquals("secret-password", targetStore.read(requireNotNull(refs.password)).decodeToString())
        assertEquals("SHA256:fingerprint", targetStore.read(requireNotNull(refs.hostFingerprint)).decodeToString())
        assertTrue(result.credentialsRequired.isEmpty())
    }
}

private fun legacyQrPayload(json: String): String {
    val compressed = ByteArrayOutputStream().use { output ->
        GZIPOutputStream(output).use { it.write(json.toByteArray()) }
        output.toByteArray()
    }
    return "PORTWEAVE:1:GZIP:" + Base64.getEncoder().encodeToString(compressed)
}

private class FakeCredentialStore(initial: Map<CredentialRef, ByteArray> = emptyMap()) : CredentialStore {
    private val values = initial.toMutableMap()
    override suspend fun put(kind: CredentialKind, value: ByteArray): CredentialRef =
        CredentialRef(UUID.randomUUID()).also { values[it] = value.copyOf() }
    override suspend fun read(ref: CredentialRef): ByteArray = values.getValue(ref).copyOf()
    override suspend fun delete(ref: CredentialRef) { values.remove(ref) }
    override suspend fun rotate(ref: CredentialRef, kind: CredentialKind, value: ByteArray): CredentialRef {
        delete(ref)
        return put(kind, value)
    }
}

private class FakeRepository(vararg configs: TunnelConfig) : ConfigRepository {
    private val values = MutableStateFlow(configs.toList())
    override val configs: Flow<List<TunnelConfig>> = values
    var batchCalls = 0
    var lastBatch = emptyList<TunnelConfig>()
    var lastReplaceIds = emptySet<UUID>()

    override suspend fun find(id: UUID) = values.value.firstOrNull { it.id == id }
    override suspend fun snapshot() = values.value
    override suspend fun save(config: TunnelConfig) = error("unexpected non-atomic save")
    override suspend fun delete(id: UUID) = error("unexpected non-atomic delete")
    override suspend fun replaceBatch(configs: List<TunnelConfig>, replaceIds: Set<UUID>) {
        batchCalls++
        lastBatch = configs
        lastReplaceIds = replaceIds
        values.value = values.value.filterNot { it.id in replaceIds } + configs
    }
}

private fun config(name: String): TunnelConfig = TunnelConfig.empty(
    kind = TunnelKind.SSH,
    name = name,
    localPort = 10_000 + name.hashCode().and(1_000),
).copy(
    spec = TunnelSpec.Ssh(
        endpoint = EndpointSource.Remote("example.invalid", 22),
        identity = dev.portweave.core.model.SshServerIdentity("example.invalid", 22),
        username = "user",
        remoteTargetHost = "target.invalid",
        remoteTargetPort = 443,
    ),
)
