package dev.portweave.core.diagnostics

import dev.portweave.core.data.db.TunnelEventDao
import dev.portweave.core.data.db.TunnelEventEntity
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Test

class RoomEventRepositoryTest {
    @Test
    fun recordTrimsPerConfigAndPruneUsesRetentionCutoff() = runTest {
        val dao = RecordingEventDao()
        val repository = RoomEventRepository(dao, nowEpochMillis = { 10L * Day })
        val id = UUID.randomUUID()

        repository.record(
            TunnelEvent(
                configId = id,
                generation = 3,
                level = EventLevel.INFO,
                code = TunnelEventCode.TUNNEL_READY,
                messageKey = "ready",
                elapsedRealtimeMillis = 10,
                occurredAtEpochMillis = 20,
            ),
        )
        repository.prune(7)

        assertEquals(1_000, dao.trimKeep)
        assertEquals(id.toString(), dao.trimConfigId)
        assertEquals(3L * Day, dao.cutoff)
    }

    private companion object { const val Day = 86_400_000L }
}

private class RecordingEventDao : TunnelEventDao {
    var trimKeep = 0
    var trimConfigId: String? = null
    var cutoff = 0L
    override suspend fun insert(event: TunnelEventEntity) = 1L
    override fun observeRecent(limit: Int): Flow<List<TunnelEventEntity>> = flowOf(emptyList())
    override fun observeForConfig(configId: String, limit: Int): Flow<List<TunnelEventEntity>> = flowOf(emptyList())
    override suspend fun recent(limit: Int) = emptyList<TunnelEventEntity>()
    override suspend fun clearForConfig(configId: String) = Unit
    override suspend fun clearAll() = Unit
    override suspend fun deleteOlderThan(cutoffEpochMillis: Long): Int { cutoff = cutoffEpochMillis; return 0 }
    override suspend fun trimConfig(configId: String, keep: Int): Int {
        trimConfigId = configId
        trimKeep = keep
        return 0
    }
}
