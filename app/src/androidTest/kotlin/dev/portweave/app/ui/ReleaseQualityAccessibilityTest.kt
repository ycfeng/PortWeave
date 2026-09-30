package dev.portweave.app.ui

import androidx.compose.foundation.layout.Column
import androidx.compose.material3.Button
import androidx.compose.material3.Text
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.test.assertHasClickAction
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.junit4.v2.createComposeRule
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.unit.Density
import dev.portweave.app.ui.component.AdaptiveScreen
import dev.portweave.app.ui.component.TunnelStatusPill
import dev.portweave.app.ui.theme.PortWeaveTheme
import dev.portweave.core.data.settings.AppSettings
import org.junit.Rule
import org.junit.Test

class ReleaseQualityAccessibilityTest {
    @get:Rule
    val composeRule = createComposeRule()

    @Test
    fun statusAndPrimaryActionRemainReachableAtTwoHundredPercentFontScale() {
        composeRule.setContent {
            val density = LocalDensity.current
            androidx.compose.runtime.CompositionLocalProvider(
                LocalDensity provides Density(density.density, fontScale = 2f),
            ) {
                PortWeaveTheme(AppSettings()) {
                    AdaptiveScreen {
                        Column {
                            TunnelStatusPill("已就绪")
                            Button(onClick = {}) { Text("停止映射") }
                        }
                    }
                }
            }
        }

        composeRule.onNodeWithText("已就绪").assertIsDisplayed()
        composeRule.onNodeWithText("停止映射").assertIsDisplayed().assertHasClickAction()
    }
}
