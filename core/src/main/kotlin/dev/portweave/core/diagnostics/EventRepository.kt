package dev.portweave.core.diagnostics

import dev.portweave.core.data.db.TunnelEventDao
import dev.portweave.core.data.db.toDomain
import dev.portweave.core.data.db.toEntity
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

interface EventRepository {
    fun observeRecent(limit: Int = 1_000): Flow<List<TunnelEvent>>
    fun observeForConfig(configId: UUID, limit: Int = 1_000): Flow<List<TunnelEvent>>
    suspend fun recent(limit: Int = 5_000): List<TunnelEvent>
    suspend fun record(event: TunnelEvent)
    suspend fun clear(configId: UUID? = null)
    suspend fun prune(retentionDays: Int = 7)
}

class RoomEventRepository(
    private val dao: TunnelEventDao,
    private val nowEpochMillis: () -> Long = System::currentTimeMillis,
) : EventRepository {
    override fun observeRecent(limit: Int) = dao.observeRecent(limit.coerceIn(1, 5_000))
        .map { events -> events.map { it.toDomain() } }

    override fun observeForConfig(configId: UUID, limit: Int) =
        dao.observeForConfig(configId.toString(), limit.coerceIn(1, 1_000))
            .map { events -> events.map { it.toDomain() } }

    override suspend fun recent(limit: Int) = dao.recent(limit.coerceIn(1, 5_000)).map { it.toDomain() }

    override suspend fun record(event: TunnelEvent) {
        dao.insert(event.toEntity())
        dao.trimConfig(event.configId.toString(), 1_000)
    }

    override suspend fun clear(configId: UUID?) {
        if (configId == null) dao.clearAll() else dao.clearForConfig(configId.toString())
    }

    override suspend fun prune(retentionDays: Int) {
        val days = retentionDays.coerceIn(1, 90)
        dao.deleteOlderThan(nowEpochMillis() - days * 86_400_000L)
    }
}
