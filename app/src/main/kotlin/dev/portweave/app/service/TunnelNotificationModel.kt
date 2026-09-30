package dev.portweave.app.service

import dev.portweave.tunnel.engine.BlockReason
import dev.portweave.tunnel.engine.TunnelState

internal data class TunnelNotificationModel(val ready: Int, val active: Int, val recoverable: Boolean) {
    companion object {
        fun from(states: Collection<TunnelState>) = TunnelNotificationModel(
            ready = states.count { it is TunnelState.Ready },
            active = states.count { it !is TunnelState.Stopped && it !is TunnelState.Failed },
            recoverable = states.isNotEmpty() && states.all {
                it is TunnelState.Reconnecting ||
                    it is TunnelState.Blocked && it.reason == BlockReason.NETWORK_UNAVAILABLE ||
                    it is TunnelState.Failed && it.error.retryable
            },
        )
    }
}
