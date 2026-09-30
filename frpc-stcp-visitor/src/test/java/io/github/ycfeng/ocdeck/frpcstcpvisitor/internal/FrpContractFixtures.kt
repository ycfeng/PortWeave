package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json

internal object FrpContractFixtures {
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = false
        coerceInputValues = false
    }
    private val classLoader = FrpContractFixtures::class.java.classLoader
        ?: throw AssertionError("Test classloader is unavailable")

    val manifest: ContractManifest by lazy {
        json.decodeFromString(readResource("manifest.json").decodeToString(throwOnInvalidSequence = true))
    }

    val syntheticToken: String = "ocdeck-k0-" + "obviously-synthetic-token"
    val syntheticStcpSecret: String = "ocdeck-k0-" + "obviously-synthetic-stcp-secret"

    fun bytes(entryId: String): ByteArray {
        val entry = manifest.entries.singleOrNull { it.id == entryId }
            ?: throw AssertionError("Unknown contract entry: $entryId")
        return readResource(entry.path).also { bytes ->
            if (bytes.size.toLong() != entry.length) {
                throw AssertionError("Contract entry length changed: $entryId")
            }
        }
    }

    fun chunkPlan(id: String): ContractChunkPlan =
        manifest.chunkPlans.singleOrNull { it.id == id }
            ?: throw AssertionError("Unknown contract chunk plan: $id")

    fun mutation(id: String): ContractMutation =
        manifest.mutations.singleOrNull { it.id == id }
            ?: throw AssertionError("Unknown contract mutation: $id")

    fun mutated(id: String): ByteArray {
        val mutation = mutation(id)
        val sourceEntry = manifest.entries.singleOrNull { it.path == mutation.source }
            ?: throw AssertionError("Mutation source is not declared: ${mutation.source}")
        val source = bytes(sourceEntry.id)
        if (mutation.operation == "wrong-aead-role") {
            return source.copyOf()
        }
        if (mutation.operation == "swap-aead-records") {
            val nonceSize = if (mutation.source.contains("xchacha20-poly1305")) 24 else 12
            return swapFirstAeadRecords(source, nonceSize)
        }

        val result = source.copyOf()
        when (mutation.operation) {
            "xor-byte" -> {
                val offset = mutation.offset ?: throw AssertionError("Missing mutation offset: $id")
                val mask = mutation.mask ?: throw AssertionError("Missing mutation mask: $id")
                result[offset] = (result[offset].toInt() xor mask).toByte()
            }
            "set-byte" -> {
                val offset = mutation.offset ?: throw AssertionError("Missing mutation offset: $id")
                result[offset] = (mutation.byteValue ?: throw AssertionError("Missing byte value: $id")).toByte()
            }
            "set-int64-be" -> {
                val offset = mutation.offset ?: throw AssertionError("Missing mutation offset: $id")
                ByteBuffer.wrap(result, offset, Long.SIZE_BYTES)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putLong(mutation.intValue ?: throw AssertionError("Missing integer value: $id"))
            }
            "set-uint16-be" -> {
                val offset = mutation.offset ?: throw AssertionError("Missing mutation offset: $id")
                ByteBuffer.wrap(result, offset, Short.SIZE_BYTES)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putShort((mutation.intValue ?: throw AssertionError("Missing integer value: $id")).toShort())
            }
            "set-uint32-be" -> {
                val offset = mutation.offset ?: throw AssertionError("Missing mutation offset: $id")
                ByteBuffer.wrap(result, offset, Int.SIZE_BYTES)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putInt((mutation.intValue ?: throw AssertionError("Missing integer value: $id")).toInt())
            }
            "truncate-tail" -> {
                val count = mutation.count ?: throw AssertionError("Missing truncation count: $id")
                return result.copyOf(result.size - count)
            }
            else -> throw AssertionError("Unsupported mutation operation: ${mutation.operation}")
        }
        return result
    }

    private fun readResource(path: String): ByteArray {
        val resource = "$CONTRACT_ROOT/$path"
        val stream = classLoader.getResourceAsStream(resource)
            ?: throw AssertionError("Missing contract resource: $resource")
        return stream.use {
            val bytes = it.readNBytes(MAX_RESOURCE_BYTES + 1)
            if (bytes.size > MAX_RESOURCE_BYTES) {
                throw AssertionError("Contract resource exceeds test bound: $resource")
            }
            bytes
        }
    }

    private fun swapFirstAeadRecords(source: ByteArray, nonceSize: Int): ByteArray {
        var position = nonceSize
        val records = mutableListOf<ByteArray>()
        while (position < source.size) {
            if (position > source.size - Int.SIZE_BYTES) {
                throw AssertionError("AEAD fixture has a truncated record header")
            }
            val length = ByteBuffer.wrap(source, position, Int.SIZE_BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .int
            val end = position + Int.SIZE_BYTES + length
            if (length < 0 || end > source.size) {
                throw AssertionError("AEAD fixture has a truncated record")
            }
            records += source.copyOfRange(position, end)
            position = end
        }
        if (records.size < 2) {
            throw AssertionError("AEAD fixture has fewer than two records")
        }
        return buildList {
            add(source.copyOfRange(0, nonceSize))
            add(records[1])
            add(records[0])
            addAll(records.drop(2))
        }.fold(ByteArray(0)) { output, part -> output + part }
    }

    private const val CONTRACT_ROOT = "io/github/ycfeng/ocdeck/frpcstcpvisitor/contract/v1"
    private const val MAX_RESOURCE_BYTES = 1 shl 20
}

internal class PlannedChunkInputStream(
    bytes: ByteArray,
    private val sizes: List<Int>,
) : InputStream() {
    private val delegate = ByteArrayInputStream(bytes)
    private var index = 0

    init {
        require(sizes.isNotEmpty() && sizes.all { it > 0 })
    }

    override fun read(): Int = delegate.read()

    override fun read(bytes: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) {
            return 0
        }
        val maximum = sizes[index++ % sizes.size]
        return delegate.read(bytes, offset, minOf(length, maximum))
    }
}

@Serializable
internal data class ContractManifest(
    val limits: ContractLimits,
    val entries: List<ContractEntry>,
    val chunkPlans: List<ContractChunkPlan>,
    val mutations: List<ContractMutation>,
)

@Serializable
internal data class ContractLimits(
    val wireV1JsonLength: Int,
    val wireV2FramePayload: Int,
)

@Serializable
internal data class ContractEntry(
    val id: String,
    val path: String,
    val length: Long,
)

@Serializable
internal data class ContractChunkPlan(
    val id: String,
    val source: String,
    val sizes: List<Int>,
)

@Serializable
internal data class ContractMutation(
    val id: String,
    val source: String,
    val operation: String,
    val offset: Int? = null,
    val byteValue: Int? = null,
    val intValue: Long? = null,
    val count: Int? = null,
    val mask: Int? = null,
)
