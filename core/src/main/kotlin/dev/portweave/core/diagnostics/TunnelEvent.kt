package dev.portweave.core.diagnostics

import java.util.UUID

enum class EventLevel { DEBUG, INFO, WARNING, ERROR }

object TunnelEventCode {
    const val START_REQUESTED = "START_REQUESTED"
    const val DEPENDENCY_RESOLVED = "DEPENDENCY_RESOLVED"
    const val LISTENER_BOUND = "LISTENER_BOUND"
    const val TUNNEL_READY = "TUNNEL_READY"
    const val NETWORK_CHANGED = "NETWORK_CHANGED"
    const val RETRY_SCHEDULED = "RETRY_SCHEDULED"
    const val STOP_REQUESTED = "STOP_REQUESTED"
    const val STOPPED = "STOPPED"
    const val FAILED = "FAILED"
    const val PROCESS_RESTORED = "PROCESS_RESTORED"
}

data class TunnelEvent(
    val id: Long = 0,
    val configId: UUID,
    val generation: Long,
    val level: EventLevel,
    val code: String,
    val messageKey: String,
    /** A JSON object produced only from an allow-list of already sanitized values. */
    val sanitizedArgsJson: String = "{}",
    val elapsedRealtimeMillis: Long,
    val occurredAtEpochMillis: Long,
    val traceId: String = UUID.randomUUID().toString(),
) {
    override fun toString(): String =
        "TunnelEvent(configId=<redacted>, generation=$generation, level=$level, code=$code, " +
            "messageKey=$messageKey, occurredAtEpochMillis=$occurredAtEpochMillis, traceId=$traceId)"
}
