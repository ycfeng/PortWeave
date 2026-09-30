package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport

import android.net.ConnectivityManager
import java.net.InetAddress
import java.net.Socket

internal class AndroidDefaultNetworkTcpDialRouteProvider(
    private val connectivityManager: ConnectivityManager,
) : TcpDialRouteProvider {
    override fun current(): TcpDialRoute {
        val network = connectivityManager.activeNetwork
            ?: throw FrpTransportException(FrpTransportFailure.CONNECT_FAILED)
        return object : TcpDialRoute {
            override fun resolve(host: String): List<InetAddress> = network.getAllByName(host).asList()

            override fun createSocket(): Socket = network.socketFactory.createSocket()
        }
    }

    override fun toString(): String = "AndroidDefaultNetworkTcpDialRouteProvider(configured=true)"
}
