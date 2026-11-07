package dev.portweave.app.service

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/**
 * [available] requires a validated default network. [networkHandle] identifies that network, which tells a real
 * switch (new handle) apart from validation flapping on the network the tunnels already use (same handle).
 */
data class NetworkStatus(val available: Boolean, val generation: Long, val networkHandle: Long? = null)

class NetworkMonitor(context: Context) : AutoCloseable {
    private val manager = context.getSystemService(ConnectivityManager::class.java)
    private val generation = AtomicLong()
    @Volatile private var lastNetwork: Network? = manager.activeNetwork
    private val mutableStatus = MutableStateFlow(NetworkStatus(currentAvailable(), 0, lastNetwork?.networkHandle))
    val status: StateFlow<NetworkStatus> = mutableStatus.asStateFlow()

    private val callback = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) = publish(network, currentAvailable())
        override fun onLost(network: Network) = publish(manager.activeNetwork, currentAvailable())
        override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) =
            publish(network, currentAvailable())
    }

    init {
        manager.registerDefaultNetworkCallback(callback)
    }

    private fun currentAvailable(): Boolean = manager.activeNetwork?.let(manager::getNetworkCapabilities)
        ?.let { capabilities ->
            capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET) &&
                capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED)
        } == true

    @Synchronized
    private fun publish(network: Network?, available: Boolean) {
        val previous = mutableStatus.value
        if (previous.available == available && lastNetwork == network) return
        lastNetwork = network
        mutableStatus.value = NetworkStatus(available, generation.incrementAndGet(), network?.networkHandle)
    }

    override fun close() {
        runCatching { manager.unregisterNetworkCallback(callback) }
    }
}
