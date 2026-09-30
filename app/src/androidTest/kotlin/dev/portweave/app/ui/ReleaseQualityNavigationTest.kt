package dev.portweave.app.ui

import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.assert
import androidx.compose.ui.test.junit4.v2.createAndroidComposeRule
import androidx.compose.ui.semantics.SemanticsActions
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.onAllNodesWithText
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import dev.portweave.app.MainActivity
import dev.portweave.app.PortWeaveApplication
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.Assert.assertTrue

class ReleaseQualityNavigationTest {
    @get:Rule
    val composeRule = createAndroidComposeRule<MainActivity>()

    private lateinit var configId: UUID

    @Before
    fun seedConfiguration() {
        val repository = (composeRule.activity.application as PortWeaveApplication).container.configRepository
        val config = TunnelConfig.empty(TunnelKind.SSH, ConfigName, LocalPort)
        configId = config.id
        runBlocking { repository.save(config) }
        composeRule.waitUntil(5_000) {
            composeRule.onAllNodesWithText(ConfigName).fetchSemanticsNodes().isNotEmpty()
        }
    }

    @After
    fun removeConfiguration() {
        val repository = (composeRule.activity.application as PortWeaveApplication).container.configRepository
        runBlocking { repository.delete(configId) }
    }

    @Test
    fun primaryScreensRemainNavigableAndScrollable() {
        val card = composeRule.onNodeWithContentDescription(
            "$ConfigName，SSH 本地转发，未运行，点击编辑",
        )
        composeRule.onNodeWithText(ConfigName).assertIsDisplayed()
        card.assert(
            SemanticsMatcher.expectValue(SemanticsProperties.StateDescription, "未运行"),
        )
        val actionLabels = card.fetchSemanticsNode().config[SemanticsActions.CustomActions].map { it.label }
        assertTrue(actionLabels.containsAll(listOf("查看详情", "启动映射")))
        composeRule.onNodeWithText(ConfigName).performClick()
        composeRule.onNodeWithText("127.0.0.1:$LocalPort").assertIsDisplayed()
        composeRule.onNodeWithContentDescription("查看日志").performClick()
        composeRule.onNodeWithText("结构化日志").assertIsDisplayed()
        composeRule.onNodeWithContentDescription("返回").performClick()
        composeRule.onNodeWithContentDescription("返回").performClick()

        composeRule.onNodeWithContentDescription("设置").performClick()
        composeRule.onNodeWithText("设置与数据").assertIsDisplayed()
        composeRule.onNodeWithText("查看结构化日志").performScrollTo().assertIsDisplayed().performClick()
        composeRule.onNodeWithText("结构化日志").assertIsDisplayed()
        composeRule.onNodeWithContentDescription("返回").performClick()
        composeRule.onNodeWithContentDescription("返回").performClick()

        composeRule.onNodeWithContentDescription("新建配置").performClick()
        composeRule.onNodeWithText("新建映射").assertIsDisplayed()
        composeRule.onNodeWithText("保存配置").performScrollTo().assertIsDisplayed()
    }

    private companion object {
        const val ConfigName = "发布质量测试"
        const val LocalPort = 15432
    }
}
