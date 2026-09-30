package dev.portweave.core.data.repository

import android.content.Context
import dev.portweave.core.data.db.PortWeaveDatabase
import dev.portweave.core.data.db.TunnelConfigDao
import dev.portweave.core.data.db.toDomain
import dev.portweave.core.data.db.toEntity
import dev.portweave.core.data.db.toSshDetailEntity
import dev.portweave.core.data.db.toStcpDetailEntity
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.DependencyGraph
import dev.portweave.core.model.validationError
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.first

interface ConfigRepository {
    val configs: Flow<List<TunnelConfig>>

    suspend fun find(id: UUID): TunnelConfig?
    suspend fun snapshot(): List<TunnelConfig> = configs.first()
    suspend fun save(config: TunnelConfig)
    suspend fun replaceBatch(configs: List<TunnelConfig>, replaceIds: Set<UUID>) {
        replaceIds.forEach { delete(it) }
        configs.forEach { save(it) }
    }
    suspend fun delete(id: UUID)
}

fun createRoomConfigRepository(context: Context): ConfigRepository = RoomConfigRepository(
    PortWeaveDatabase.create(context).tunnelConfigDao(),
)

class RoomConfigRepository(
    private val dao: TunnelConfigDao,
    private val nowEpochMillis: () -> Long = System::currentTimeMillis,
) : ConfigRepository {
    override val configs: Flow<List<TunnelConfig>> = dao.observeAll().map { entities ->
        entities.map { it.toDomain() }
    }

    override suspend fun find(id: UUID): TunnelConfig? = dao.findById(id.toString())?.toDomain()

    override suspend fun snapshot(): List<TunnelConfig> = dao.findAll().map { it.toDomain() }

    override suspend fun save(config: TunnelConfig) {
        require(config.validationError() == null) { config.validationError().orEmpty() }
        val candidate = snapshot().filterNot { it.id == config.id } + config
        DependencyGraph(candidate).validate()
        val current = dao.findById(config.id.toString())?.config
        val persisted = config.copy(
            name = config.name.trim(),
            description = config.description.trim(),
            revision = if (current == null) 1 else current.revision + 1,
            createdAtEpochMillis = current?.createdAt ?: config.createdAtEpochMillis,
            updatedAtEpochMillis = nowEpochMillis(),
        )
        dao.upsertWithDetails(
            persisted.toEntity(),
            persisted.toSshDetailEntity(),
            persisted.toStcpDetailEntity(),
        )
    }

    override suspend fun replaceBatch(configs: List<TunnelConfig>, replaceIds: Set<UUID>) {
        require(configs.map(TunnelConfig::id).distinct().size == configs.size) { "导入配置 ID 重复" }
        configs.forEach { require(it.validationError() == null) { it.validationError().orEmpty() } }
        val current = snapshot()
        val candidate = current.filterNot { it.id in replaceIds } + configs
        DependencyGraph(candidate).validate()
        val now = nowEpochMillis()
        val currentById = current.associateBy(TunnelConfig::id)
        val persisted = configs.map { config ->
            val existing = currentById[config.id]
            config.copy(
                name = config.name.trim(),
                description = config.description.trim(),
                revision = (existing?.revision ?: 0L) + 1L,
                createdAtEpochMillis = existing?.createdAtEpochMillis ?: config.createdAtEpochMillis,
                updatedAtEpochMillis = now,
            )
        }
        dao.replaceBatch(
            deleteIds = replaceIds.map(UUID::toString),
            configs = persisted.map(TunnelConfig::toEntity),
            sshDetails = persisted.mapNotNull(TunnelConfig::toSshDetailEntity),
            stcpDetails = persisted.mapNotNull(TunnelConfig::toStcpDetailEntity),
        )
    }

    override suspend fun delete(id: UUID) {
        require(DependencyGraph(snapshot()).transitiveDependents(id).isEmpty()) {
            "配置仍被其他映射依赖"
        }
        dao.deleteById(id.toString())
    }
}
