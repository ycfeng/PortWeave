package dev.portweave.app.feature.settings

import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.PowerManager
import android.provider.Settings
import android.widget.Toast
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.ArrowBack
import androidx.compose.material3.Button
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.RadioButton
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.stateDescription
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.compose.LocalLifecycleOwner
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import dev.portweave.app.diagnostics.DiagnosticBundleExporter
import dev.portweave.app.service.RuntimeStateStore
import dev.portweave.app.ui.theme.PortWeaveThemeTokens
import dev.portweave.app.ui.component.AdaptiveScreen
import dev.portweave.app.ui.component.PortWeaveSwitch
import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.data.settings.AppSettings
import dev.portweave.core.data.settings.SettingsStore
import dev.portweave.core.data.settings.ThemeMode
import dev.portweave.core.data.transfer.ConfigTransferService
import dev.portweave.core.data.transfer.ImportConflictStrategy
import dev.portweave.core.data.transfer.ImportPreview
import dev.portweave.core.diagnostics.EventRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SettingsScreen(
    settingsStore: SettingsStore,
    repository: ConfigRepository,
    transfer: ConfigTransferService,
    diagnostics: DiagnosticBundleExporter,
    events: EventRepository,
    runtimeStore: RuntimeStateStore,
    onLogs: () -> Unit,
    onBack: () -> Unit,
) {
    val settings by settingsStore.settings.collectAsStateWithLifecycle(AppSettings())
    val scope = rememberCoroutineScope()
    val context = LocalContext.current
    var message by remember { mutableStateOf<String?>(null) }
    var strategy by remember { mutableStateOf(ImportConflictStrategy.CREATE_COPY) }
    var pendingImport by remember { mutableStateOf<PendingImport?>(null) }
    var legalText by remember { mutableStateOf<String?>(null) }
    var ignoringBatteryOptimizations by remember { mutableStateOf(isIgnoringBatteryOptimizations(context)) }
    val lifecycleOwner = LocalLifecycleOwner.current

    DisposableEffect(lifecycleOwner, context) {
        val observer = LifecycleEventObserver { _, event ->
            if (event == Lifecycle.Event.ON_RESUME) {
                ignoringBatteryOptimizations = isIgnoringBatteryOptimizations(context)
            }
        }
        lifecycleOwner.lifecycle.addObserver(observer)
        onDispose { lifecycleOwner.lifecycle.removeObserver(observer) }
    }
    val versionName = remember(context) {
        @Suppress("DEPRECATION")
        context.packageManager.getPackageInfo(context.packageName, 0).versionName ?: "未知"
    }
    val exportConfigs = rememberLauncherForActivityResult(ActivityResultContracts.CreateDocument("application/json")) { uri ->
        uri ?: return@rememberLauncherForActivityResult
        scope.launch {
            message = runCatching {
                val json = transfer.export(repository.snapshot())
                withContext(Dispatchers.IO) { context.contentResolver.openOutputStream(uri)?.writer()?.use { it.write(json) } }
                "配置已导出（不含秘密）"
            }.getOrElse { "导出失败：${it.message}" }
        }
    }
    val importConfigs = rememberLauncherForActivityResult(ActivityResultContracts.OpenDocument()) { uri ->
        uri ?: return@rememberLauncherForActivityResult
        scope.launch {
            message = runCatching {
                val json = withContext(Dispatchers.IO) {
                    context.contentResolver.openInputStream(uri)?.bufferedReader()?.use { it.readText() }
                        ?: error("无法读取文件")
                }
                val preview = transfer.preview(json, strategy)
                pendingImport = PendingImport(json, preview, strategy)
                "导入预览已就绪"
            }.getOrElse { "导入校验失败，未写入任何配置：${it.message}" }
        }
    }

    pendingImport?.let { pending ->
        AlertDialog(
            onDismissRequest = { pendingImport = null },
            title = { Text("确认导入") },
            text = {
                Text(
                    "将导入 ${pending.preview.configs.size} 项，跳过 ${pending.preview.skipped} 项；" +
                        "${pending.preview.credentialsRequired.size} 项需要补充凭据。整批写入将作为一个事务执行。",
                )
            },
            confirmButton = {
                Button(onClick = {
                    pendingImport = null
                    scope.launch {
                        message = runCatching {
                            val result = transfer.import(pending.json, pending.strategy)
                            "已导入 ${result.imported} 项，跳过 ${result.skipped} 项"
                        }.getOrElse { "导入失败，事务已回滚：${it.message}" }
                    }
                }) { Text("导入") }
            },
            dismissButton = { OutlinedButton(onClick = { pendingImport = null }) { Text("取消") } },
        )
    }
    legalText?.let { text ->
        AlertDialog(
            onDismissRequest = { legalText = null },
            title = { Text("开源许可证与 NOTICE") },
            text = {
                Text(
                    text,
                    modifier = Modifier.verticalScroll(rememberScrollState()),
                    style = MaterialTheme.typography.bodyMedium,
                )
            },
            confirmButton = { Button(onClick = { legalText = null }) { Text("关闭") } },
        )
    }
    val exportDiagnostics = rememberLauncherForActivityResult(ActivityResultContracts.CreateDocument("application/zip")) { uri ->
        uri ?: return@rememberLauncherForActivityResult
        scope.launch {
            message = runCatching {
                withContext(Dispatchers.IO) {
                    context.contentResolver.openOutputStream(uri)?.use { diagnostics.write(it, runtimeStore.runtime.value) }
                        ?: error("无法创建诊断包")
                }
                "脱敏诊断包已导出"
            }.getOrElse { "诊断包导出失败：${it.message}" }
        }
    }

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("设置与数据") },
                navigationIcon = {
                    IconButton(onClick = onBack) {
                        Icon(Icons.AutoMirrored.Outlined.ArrowBack, contentDescription = "返回")
                    }
                },
            )
        },
    ) { padding ->
        AdaptiveScreen(modifier = Modifier.padding(padding)) {
            Column(
                modifier = Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(16.dp),
                verticalArrangement = Arrangement.spacedBy(10.dp),
            ) {
            Text("外观", style = MaterialTheme.typography.titleLarge)
            Text("主题模式", style = MaterialTheme.typography.titleMedium)
            ThemeMode.entries.forEach { mode ->
                Row(
                    modifier = Modifier.fillMaxWidth().selectable(
                        selected = settings.themeMode == mode,
                        role = Role.RadioButton,
                        onClick = { scope.launch { settingsStore.setThemeMode(mode) } },
                    ).padding(vertical = 4.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    RadioButton(selected = settings.themeMode == mode, onClick = null)
                    Text(mode.displayName(), modifier = Modifier.padding(start = 8.dp))
                }
            }
            Row(
                modifier = Modifier.fillMaxWidth().padding(top = 8.dp).semantics(mergeDescendants = true) {
                    stateDescription = if (settings.dynamicColor) "已开启" else "已关闭"
                },
                verticalAlignment = Alignment.CenterVertically,
            ) {
                Column(modifier = Modifier.weight(1f)) {
                    Text("动态取色", style = MaterialTheme.typography.titleMedium)
                    Text(
                        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) "使用系统壁纸色彩。" else "需要 Android 12 或更高版本。",
                        color = PortWeaveThemeTokens.status.mutedText,
                    )
                }
                PortWeaveSwitch(
                    checked = settings.dynamicColor,
                    enabled = Build.VERSION.SDK_INT >= Build.VERSION_CODES.S,
                    onCheckedChange = { scope.launch { settingsStore.setDynamicColor(it) } },
                )
            }

            Text("后台运行", style = MaterialTheme.typography.titleLarge, modifier = Modifier.padding(top = 20.dp))
            Text("忽略电池优化", style = MaterialTheme.typography.titleMedium)
            Text(
                if (ignoringBatteryOptimizations) {
                    "已忽略电池优化，系统更不容易暂停后台端口转发。"
                } else {
                    "省电策略可能暂停后台端口转发。可向系统申请忽略 PortWeave 的电池优化。"
                },
                color = if (ignoringBatteryOptimizations) {
                    MaterialTheme.colorScheme.primary
                } else {
                    PortWeaveThemeTokens.status.mutedText
                },
            )
            OutlinedButton(
                onClick = { requestIgnoreBatteryOptimizations(context) },
                enabled = !ignoringBatteryOptimizations,
                modifier = Modifier.fillMaxWidth(),
            ) {
                Text(if (ignoringBatteryOptimizations) "已完成" else "申请忽略电池优化")
            }

            Text("数据", style = MaterialTheme.typography.titleLarge, modifier = Modifier.padding(top = 20.dp))
            Text("日志保留周期", style = MaterialTheme.typography.titleMedium)
            listOf(1, 7, 30).forEach { days ->
                Row(
                    Modifier.fillMaxWidth().selectable(
                        selected = settings.logRetentionDays == days,
                        role = Role.RadioButton,
                        onClick = { scope.launch { settingsStore.setLogRetentionDays(days) } },
                    ).padding(vertical = 4.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    RadioButton(
                        selected = settings.logRetentionDays == days,
                        onClick = null,
                    )
                    Text("$days 天")
                }
            }
            Text("导入冲突处理", style = MaterialTheme.typography.titleMedium)
            ImportConflictStrategy.entries.forEach { item ->
                Row(
                    Modifier.fillMaxWidth().selectable(
                        selected = strategy == item,
                        role = Role.RadioButton,
                        onClick = { strategy = item },
                    ).padding(vertical = 4.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    RadioButton(selected = strategy == item, onClick = null)
                    Text(item.displayName())
                }
            }
            Button(onClick = { exportConfigs.launch("portweave-configs.json") }, modifier = Modifier.fillMaxWidth()) {
                Text("导出配置（不含秘密）")
            }
            OutlinedButton(onClick = { importConfigs.launch(arrayOf("application/json", "text/json", "text/plain")) }, modifier = Modifier.fillMaxWidth()) {
                Text("导入配置")
            }
            OutlinedButton(onClick = { exportDiagnostics.launch("portweave-diagnostics.zip") }, modifier = Modifier.fillMaxWidth()) {
                Text("导出脱敏诊断包")
            }
            OutlinedButton(onClick = onLogs, modifier = Modifier.fillMaxWidth()) { Text("查看结构化日志") }
            OutlinedButton(
                onClick = { scope.launch { events.clear(); message = "日志已清空" } },
                modifier = Modifier.fillMaxWidth(),
            ) { Text("清空全部日志") }
            message?.let { Text(it, color = MaterialTheme.colorScheme.primary) }
            Text("关于", style = MaterialTheme.typography.titleLarge, modifier = Modifier.padding(top = 20.dp))
            Text("PortWeave $versionName")
            Text(
                "无分析或广告 SDK；仅连接用户配置的服务。秘密使用 Android Keystore 支持的加密存储。",
                color = PortWeaveThemeTokens.status.mutedText,
            )
            OutlinedButton(
                onClick = {
                    legalText = runCatching {
                        context.assets.open("licenses.txt").bufferedReader().use { it.readText() }
                    }.getOrElse { "无法读取离线许可证：${it.message}" }
                },
                modifier = Modifier.fillMaxWidth(),
            ) { Text("查看开源许可证与 NOTICE") }
            }
        }
    }
}

private fun ThemeMode.displayName(): String = when (this) {
    ThemeMode.SYSTEM -> "跟随系统"
    ThemeMode.LIGHT -> "浅色"
    ThemeMode.DARK -> "深色"
}

private fun ImportConflictStrategy.displayName(): String = when (this) {
    ImportConflictStrategy.CREATE_COPY -> "创建副本并重写内部依赖"
    ImportConflictStrategy.REPLACE_LOCAL -> "替换本地冲突项"
    ImportConflictStrategy.SKIP -> "跳过冲突项及依赖它的导入项"
}

private fun isIgnoringBatteryOptimizations(context: Context): Boolean {
    val powerManager = context.getSystemService(PowerManager::class.java) ?: return false
    return powerManager.isIgnoringBatteryOptimizations(context.packageName)
}

private fun requestIgnoreBatteryOptimizations(context: Context) {
    if (isIgnoringBatteryOptimizations(context)) {
        Toast.makeText(context, "已忽略电池优化", Toast.LENGTH_SHORT).show()
        return
    }

    val intent = Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS).apply {
        data = Uri.parse("package:${context.packageName}")
    }
    if (intent.resolveActivity(context.packageManager) == null) {
        Toast.makeText(context, "当前系统不支持直接申请忽略电池优化", Toast.LENGTH_SHORT).show()
        return
    }

    try {
        context.startActivity(intent)
    } catch (_: ActivityNotFoundException) {
        Toast.makeText(context, "当前系统不支持直接申请忽略电池优化", Toast.LENGTH_SHORT).show()
    } catch (_: SecurityException) {
        Toast.makeText(context, "无法打开电池优化确认弹窗", Toast.LENGTH_SHORT).show()
    }
}

private data class PendingImport(
    val json: String,
    val preview: ImportPreview,
    val strategy: ImportConflictStrategy,
)
