package dev.portweave.app.feature.home

import android.content.Intent
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.junit4.v2.createAndroidComposeRule
import androidx.compose.ui.test.onAllNodesWithContentDescription
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.performClick
import androidx.test.runner.intent.IntentMonitorRegistry
import dev.portweave.app.MainActivity
import dev.portweave.app.PortWeaveApplication
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.tunnel.engine.TunnelState
import java.net.InetAddress
import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Rule
import org.junit.Test

class ProtocolBrowserActionDeviceTest {
    @get:Rule
    val composeRule = createAndroidComposeRule<MainActivity>()

    private lateinit var server: ServerSocket
    private lateinit var config: TunnelConfig

    @Before
    fun seedReadyHttpConfiguration() {
        server = ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))
        thread(name = "browser-action-http-server") {
            runCatching {
                server.accept().use { socket ->
                    val input = socket.getInputStream()
                    var matched = 0
                    val terminator = "\r\n\r\n".toByteArray()
                    while (matched < terminator.size) {
                        val next = input.read()
                        if (next == -1) break
                        matched = if (next.toByte() == terminator[matched]) matched + 1 else 0
                    }
                    socket.getOutputStream().apply {
                        write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".toByteArray())
                        flush()
                    }
                }
            }
        }

        val container = (composeRule.activity.application as PortWeaveApplication).container
        config = TunnelConfig.empty(TunnelKind.SSH, "HTTP 探测真机测试", server.localPort)
        runBlocking { container.configRepository.save(config) }
        container.runtimeStateStore.update(config.id, TunnelState.Ready(System.currentTimeMillis()))
        composeRule.waitUntil(5_000) {
            composeRule.onAllNodesWithContentDescription("在浏览器中打开")
                .fetchSemanticsNodes().isNotEmpty()
        }
    }

    @After
    fun cleanUp() {
        val container = (composeRule.activity.application as PortWeaveApplication).container
        container.runtimeStateStore.remove(config.id)
        runBlocking { container.configRepository.delete(config.id) }
        server.close()
    }

    @Test
    fun detectedHttpPortShowsBrowserActionAndOpensItsUrl() {
        val observedIntent = AtomicReference<Intent?>()
        val callback = androidx.test.runner.intent.IntentCallback { intent ->
            if (intent.action == Intent.ACTION_VIEW) observedIntent.set(intent)
        }
        IntentMonitorRegistry.getInstance().addIntentCallback(callback)
        try {
            composeRule.onNodeWithContentDescription("在浏览器中打开")
                .assertIsDisplayed()
                .performClick()
            composeRule.waitUntil(5_000) { observedIntent.get() != null }
            assertEquals(
                "http://127.0.0.1:${server.localPort}/",
                observedIntent.get()?.dataString,
            )
        } finally {
            IntentMonitorRegistry.getInstance().removeIntentCallback(callback)
        }
    }
}
