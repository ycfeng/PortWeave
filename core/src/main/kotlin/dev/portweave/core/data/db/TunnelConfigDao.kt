package dev.portweave.core.data.db

import androidx.room.Dao
import androidx.room.Query
import androidx.room.Transaction
import androidx.room.Upsert
import kotlinx.coroutines.flow.Flow

@Dao
interface TunnelConfigDao {
    @Query("SELECT * FROM tunnel_config ORDER BY sort_order ASC, updated_at DESC")
    @Transaction
    suspend fun findAll(): List<TunnelConfigRecord>

    @Query("SELECT * FROM tunnel_config ORDER BY sort_order ASC, updated_at DESC")
    @Transaction
    fun observeAll(): Flow<List<TunnelConfigRecord>>

    @Query("SELECT * FROM tunnel_config WHERE id = :id LIMIT 1")
    @Transaction
    suspend fun findById(id: String): TunnelConfigRecord?

    @Upsert
    suspend fun upsert(config: TunnelConfigEntity)

    @Upsert
    suspend fun upsertSsh(detail: SshDetailEntity)

    @Upsert
    suspend fun upsertStcp(detail: StcpDetailEntity)

    @Query("DELETE FROM ssh_detail WHERE config_id = :configId")
    suspend fun deleteSsh(configId: String)

    @Query("DELETE FROM stcp_detail WHERE config_id = :configId")
    suspend fun deleteStcp(configId: String)

    @Transaction
    suspend fun upsertWithDetails(
        config: TunnelConfigEntity,
        ssh: SshDetailEntity?,
        stcp: StcpDetailEntity?,
    ) {
        upsert(config)
        if (ssh == null) deleteSsh(config.id) else upsertSsh(ssh)
        if (stcp == null) deleteStcp(config.id) else upsertStcp(stcp)
    }

    @Query("DELETE FROM tunnel_config WHERE id = :id")
    suspend fun deleteById(id: String)

    @Transaction
    suspend fun replaceBatch(
        deleteIds: List<String>,
        configs: List<TunnelConfigEntity>,
        sshDetails: List<SshDetailEntity>,
        stcpDetails: List<StcpDetailEntity>,
    ) {
        deleteIds.forEach { deleteById(it) }
        configs.forEach { upsert(it) }
        sshDetails.forEach { upsertSsh(it) }
        stcpDetails.forEach { upsertStcp(it) }
    }
}
