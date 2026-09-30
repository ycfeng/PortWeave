package dev.portweave.core.data.db

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.Query
import kotlinx.coroutines.flow.Flow

@Dao
interface TunnelEventDao {
    @Insert
    suspend fun insert(event: TunnelEventEntity): Long

    @Query("SELECT * FROM tunnel_event ORDER BY occurred_at DESC LIMIT :limit")
    fun observeRecent(limit: Int): Flow<List<TunnelEventEntity>>

    @Query("SELECT * FROM tunnel_event WHERE config_id = :configId ORDER BY occurred_at DESC LIMIT :limit")
    fun observeForConfig(configId: String, limit: Int): Flow<List<TunnelEventEntity>>

    @Query("SELECT * FROM tunnel_event ORDER BY occurred_at DESC LIMIT :limit")
    suspend fun recent(limit: Int): List<TunnelEventEntity>

    @Query("DELETE FROM tunnel_event WHERE config_id = :configId")
    suspend fun clearForConfig(configId: String)

    @Query("DELETE FROM tunnel_event")
    suspend fun clearAll()

    @Query("DELETE FROM tunnel_event WHERE occurred_at < :cutoffEpochMillis")
    suspend fun deleteOlderThan(cutoffEpochMillis: Long): Int

    @Query(
        "DELETE FROM tunnel_event WHERE config_id = :configId AND id NOT IN " +
            "(SELECT id FROM tunnel_event WHERE config_id = :configId ORDER BY occurred_at DESC LIMIT :keep)",
    )
    suspend fun trimConfig(configId: String, keep: Int): Int
}
