package dev.portweave.core.data.db

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey
import dev.portweave.core.diagnostics.EventLevel
import dev.portweave.core.diagnostics.TunnelEvent
import java.util.UUID

@Entity(
    tableName = "tunnel_event",
    indices = [Index(value = ["config_id", "occurred_at"]), Index(value = ["occurred_at"])],
)
data class TunnelEventEntity(
    @PrimaryKey(autoGenerate = true) val id: Long = 0,
    @ColumnInfo(name = "config_id") val configId: String,
    val generation: Long,
    val level: String,
    @ColumnInfo(name = "event_code") val eventCode: String,
    @ColumnInfo(name = "message_key") val messageKey: String,
    @ColumnInfo(name = "sanitized_args_json") val sanitizedArgsJson: String,
    @ColumnInfo(name = "elapsed_realtime_millis") val elapsedRealtimeMillis: Long,
    @ColumnInfo(name = "occurred_at") val occurredAt: Long,
    @ColumnInfo(name = "trace_id") val traceId: String,
)

internal fun TunnelEventEntity.toDomain() = TunnelEvent(
    id = id,
    configId = UUID.fromString(configId),
    generation = generation,
    level = EventLevel.valueOf(level),
    code = eventCode,
    messageKey = messageKey,
    sanitizedArgsJson = sanitizedArgsJson,
    elapsedRealtimeMillis = elapsedRealtimeMillis,
    occurredAtEpochMillis = occurredAt,
    traceId = traceId,
)

internal fun TunnelEvent.toEntity() = TunnelEventEntity(
    id = id,
    configId = configId.toString(),
    generation = generation,
    level = level.name,
    eventCode = code,
    messageKey = messageKey,
    sanitizedArgsJson = sanitizedArgsJson,
    elapsedRealtimeMillis = elapsedRealtimeMillis,
    occurredAt = occurredAtEpochMillis,
    traceId = traceId,
)
