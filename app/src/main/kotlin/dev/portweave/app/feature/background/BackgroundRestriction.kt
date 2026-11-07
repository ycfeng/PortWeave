package dev.portweave.app.feature.background

import android.app.ActivityManager
import android.app.ApplicationExitInfo
import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.provider.Settings
import android.widget.Toast
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import dev.portweave.app.AppContainer
import dev.portweave.app.ui.theme.PortWeaveThemeTokens
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/**
 * Finds the previous process death when the system, not the user, ended it. Vendor ROMs report their background
 * cleanups inconsistently, so this looks only at reasons a user action never produces.
 */
object BackgroundRestrictionDetector {
    fun unexpectedExitAfter(context: Context, acknowledgedMillis: Long): Long? {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return null
        val manager = context.getSystemService(ActivityManager::class.java) ?: return null
        val previous = runCatching {
            manager.getHistoricalProcessExitReasons(context.packageName, 0, 1).firstOrNull()
        }.getOrNull() ?: return null
        return previous.timestamp.takeIf {
            it > acknowledgedMillis && previous.processName == context.packageName && previous.reason in SystemReasons
        }
    }

    private val SystemReasons = setOf(
        ApplicationExitInfo.REASON_OTHER,
        ApplicationExitInfo.REASON_SIGNALED,
        ApplicationExitInfo.REASON_EXCESSIVE_RESOURCE_USAGE,
        ApplicationExitInfo.REASON_LOW_MEMORY,
    )
}

/** Shown once per unexpected exit, and only if tunnels were supposed to be running when it happened. */
@Composable
fun BackgroundRestrictionPrompt(container: AppContainer) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    var exitAt by remember { mutableStateOf<Long?>(null) }
    LaunchedEffect(Unit) {
        if (container.desiredRuntimeStore.snapshot().isEmpty()) return@LaunchedEffect
        val acknowledged = container.settingsStore.settings.first().acknowledgedExitTimestamp
        exitAt = withContext(Dispatchers.IO) {
            BackgroundRestrictionDetector.unexpectedExitAfter(context, acknowledged)
        }
    }
    val timestamp = exitAt ?: return
    fun acknowledge() {
        exitAt = null
        scope.launch { container.settingsStore.acknowledgeExit(timestamp) }
    }
    AlertDialog(
        onDismissRequest = ::acknowledge,
        title = { Text("系统可能限制了后台运行") },
        text = {
            Column(Modifier.verticalScroll(rememberScrollState()), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text("PortWeave 上次在后台运行映射时被系统结束。若希望映射在后台持续可用，可在系统设置中放宽对 PortWeave 的后台限制。")
                BackgroundRunGuideText()
            }
        },
        confirmButton = {
            Button(onClick = {
                openAppDetails(context)
                acknowledge()
            }) { Text("打开应用设置") }
        },
        dismissButton = { OutlinedButton(onClick = ::acknowledge) { Text("知道了") } },
    )
}

/** Vendor settings differ by system version; these are hints, never deep links that may break. */
@Composable
fun BackgroundRunGuideText() {
    Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
        VendorTips.forEach { (system, path) ->
            Text(system, style = MaterialTheme.typography.titleSmall)
            Text(path, color = PortWeaveThemeTokens.status.mutedText)
        }
        Text("具体名称和位置以手机实际系统版本为准。", color = PortWeaveThemeTokens.status.mutedText)
    }
}

private val VendorTips = listOf(
    "ColorOS / realme UI / OxygenOS" to "电池 → 应用耗电管理 → 允许后台行为、允许自启动",
    "OriginOS / Funtouch OS" to "电池 → 后台耗电管理 → 允许后台高耗电",
    "HyperOS / MIUI" to "应用设置 → 省电策略 → 无限制，并开启自启动",
    "MagicOS / HarmonyOS / EMUI" to "应用启动管理 → 关闭自动管理 → 允许后台活动",
    "通用" to "在多任务界面锁定 PortWeave 的应用卡片",
)

/** The generic, stable entry point; vendor-specific activities change between versions and are not hard-coded. */
fun openAppDetails(context: Context) {
    val intent = Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:${context.packageName}"))
        .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
    try {
        context.startActivity(intent)
    } catch (_: ActivityNotFoundException) {
        Toast.makeText(context, "无法打开应用设置", Toast.LENGTH_SHORT).show()
    } catch (_: SecurityException) {
        Toast.makeText(context, "无法打开应用设置", Toast.LENGTH_SHORT).show()
    }
}
