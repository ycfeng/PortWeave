package dev.portweave.app

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.runtime.getValue
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import dev.portweave.app.feature.background.BackgroundRestrictionPrompt
import dev.portweave.app.navigation.PortWeaveNavGraph
import dev.portweave.app.ui.theme.PortWeaveTheme

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        val container = (application as PortWeaveApplication).container
        setContent {
            val settings by container.settingsStore.settings.collectAsStateWithLifecycle(
                initialValue = dev.portweave.core.data.settings.AppSettings(),
            )
            PortWeaveTheme(settings = settings) {
                PortWeaveNavGraph(container = container)
                BackgroundRestrictionPrompt(container)
            }
        }
    }

    override fun onStart() {
        super.onStart()
        (application as PortWeaveApplication).container.workPolicy.visible.value = true
        (application as PortWeaveApplication).container.tunnelServiceClient.bind()
    }

    override fun onStop() {
        (application as PortWeaveApplication).container.workPolicy.visible.value = false
        (application as PortWeaveApplication).container.tunnelServiceClient.unbind()
        super.onStop()
    }
}
