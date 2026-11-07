package dev.portweave.tunnel.engine.ssh

import dev.portweave.tunnel.engine.HeartbeatCoordinator
import dev.portweave.tunnel.engine.RuntimeWorkCounters

/** What [SshHeartbeat] needs from a live SSH session. */
internal interface SshKeepAliveTransport {
    val isConnected: Boolean
    /** True when no forwarded client connection is open, so a keepalive is the only traffic. */
    val idle: Boolean
    /** Monotonic count of reads from the server; any read proves the connection is alive. */
    fun readMark(): Long
    suspend fun awaitReadAfter(mark: Long, timeoutMillis: Long): Boolean
    fun sendKeepAlive()
    /** JSch's own keepalive stays armed as a backstop in case this driver stops; it only fires after silence. */
    fun setBackstopIntervalMillis(intervalMillis: Int)
    fun declareDead()
}

/**
 * Drives SSH keepalives from [HeartbeatCoordinator] instead of JSch's per-session timer, so beats are aligned across
 * tunnels and stretched in the background. A keepalive that gets no reply within [replyTimeoutMillis] ends the
 * session the same way JSch's own keepalive exhaustion would.
 */
internal class SshHeartbeat(
    private val transport: SshKeepAliveTransport,
    private val coordinator: HeartbeatCoordinator,
    private val configuredMillis: Long,
    private val counters: RuntimeWorkCounters,
) {
    private val replyTimeoutMillis = configuredMillis.coerceIn(MinimumReplyTimeoutMillis, MaximumReplyTimeoutMillis)

    suspend fun run() {
        var backstop = -1
        while (transport.isConnected) {
            val interval = coordinator.intervalMillis(configuredMillis)
            val nextBackstop = (interval + replyTimeoutMillis + BackstopSlackMillis).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
            if (nextBackstop != backstop) {
                transport.setBackstopIntervalMillis(nextBackstop)
                backstop = nextBackstop
            }
            coordinator.awaitBeat(interval)
            if (!transport.isConnected) return
            val idle = transport.idle
            val mark = transport.readMark()
            try {
                transport.sendKeepAlive()
            } catch (_: Exception) {
                return // The session is going away; its own loss signal reports that.
            }
            counters.increment("sshKeepAlivesSent")
            if (transport.awaitReadAfter(mark, replyTimeoutMillis)) {
                if (idle) coordinator.onKeepAliveResult(answered = true, intervalMillis = interval)
            } else {
                if (!transport.isConnected) return
                counters.increment("sshKeepAliveTimeouts")
                if (idle) coordinator.onKeepAliveResult(answered = false, intervalMillis = interval)
                transport.declareDead()
                return
            }
        }
    }

    private companion object {
        const val MinimumReplyTimeoutMillis = 5_000L
        const val MaximumReplyTimeoutMillis = 30_000L
        const val BackstopSlackMillis = 5_000L
    }
}
