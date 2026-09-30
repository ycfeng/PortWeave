package dev.portweave.app.feature.home

import android.Manifest
import android.content.ActivityNotFoundException
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Add
import androidx.compose.material.icons.outlined.Cable
import androidx.compose.material.icons.outlined.PlayArrow
import androidx.compose.material.icons.outlined.PowerSettingsNew
import androidx.compose.material.icons.outlined.Public
import androidx.compose.material.icons.outlined.Settings
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.ExtendedFloatingActionButton
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.CustomAccessibilityAction
import androidx.compose.ui.semantics.customActions
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.stateDescription
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.LifecycleStartEffect
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import dev.portweave.app.ui.component.TunnelStatusPill
import dev.portweave.app.ui.component.adaptiveColumnCount
import dev.portweave.app.ui.format.formatByteCount
import dev.portweave.app.ui.component.displayName
import dev.portweave.app.ui.theme.PortWeaveThemeTokens
import dev.portweave.app.service.RuntimeStateStore
import dev.portweave.app.service.RuntimeSnapshot
import dev.portweave.app.service.TunnelForegroundService
import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.model.TunnelConfig
import dev.portweave.tunnel.engine.TunnelState

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun HomeScreen(
    repository: ConfigRepository,
    runtimeStateStore: RuntimeStateStore,
    onAdd: () -> Unit,
    onEdit: (String) -> Unit,
    onSettings: () -> Unit,
) {
    val viewModel: HomeViewModel = viewModel(factory = HomeViewModel.factory(repository, runtimeStateStore))
    LifecycleStartEffect(viewModel) {
        viewModel.setVisible(true)
        onStopOrDispose { viewModel.setVisible(false) }
    }
    val configs by viewModel.configs.collectAsStateWithLifecycle()
    val protocols by viewModel.protocols.collectAsStateWithLifecycle()
    val runtimeStates by runtimeStateStore.states.collectAsStateWithLifecycle()
    val runtime by runtimeStateStore.runtime.collectAsStateWithLifecycle()
    val context = LocalContext.current
    var pendingStart by remember { mutableStateOf<TunnelConfig?>(null) }
    val notificationPermission = rememberLauncherForActivityResult(
        ActivityResultContracts.RequestPermission(),
    ) {
        pendingStart?.let { TunnelForegroundService.start(context, it.id) }
        pendingStart = null
    }
    fun start(config: TunnelConfig) {
        if (
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
            context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            pendingStart = config
            notificationPermission.launch(Manifest.permission.POST_NOTIFICATIONS)
        } else {
            TunnelForegroundService.start(context, config.id)
        }
    }

    Scaffold(
        topBar = {
            TopAppBar(
                title = {
                    Column {
                        Text("PortWeave", style = MaterialTheme.typography.titleLarge)
                        Text(
                            "本地端口映射",
                            color = PortWeaveThemeTokens.status.mutedText,
                            style = MaterialTheme.typography.labelMedium,
                        )
                    }
                },
                actions = {
                    IconButton(onClick = onSettings) {
                        Icon(Icons.Outlined.Settings, contentDescription = "设置")
                    }
                },
            )
        },
        floatingActionButton = {
            ExtendedFloatingActionButton(
                onClick = onAdd,
                icon = { Icon(Icons.Outlined.Add, contentDescription = "新建配置") },
                text = { Text("新建映射") },
                containerColor = MaterialTheme.colorScheme.primary,
                contentColor = MaterialTheme.colorScheme.onPrimary,
            )
        },
    ) { padding ->
        if (configs.isEmpty()) {
            EmptyHome(modifier = Modifier.padding(padding), onAdd = onAdd)
        } else {
            BoxWithConstraints(modifier = Modifier.fillMaxSize().padding(padding)) {
                val columns = adaptiveColumnCount(maxWidth.value.toInt())
                LazyVerticalGrid(
                    columns = GridCells.Fixed(columns),
                    modifier = Modifier.fillMaxSize(),
                    contentPadding = PaddingValues(horizontal = if (columns > 1) 24.dp else 16.dp, vertical = 12.dp),
                    horizontalArrangement = Arrangement.spacedBy(12.dp),
                    verticalArrangement = Arrangement.spacedBy(12.dp),
                ) {
                    items(configs, key = { it.id }) { config ->
                        ConfigCard(
                            config = config,
                            state = runtimeStates[config.id] ?: TunnelState.Stopped,
                            runtime = runtime[config.id] ?: RuntimeSnapshot(),
                            protocol = protocols[config.id],
                            onClick = { onEdit(config.id.toString()) },
                            onStart = { start(config) },
                            onStop = { TunnelForegroundService.stop(context, config.id) },
                            onOpenBrowser = { protocol ->
                                val uri = Uri.parse("${protocol.urlScheme}://127.0.0.1:${config.localPort}/")
                                try {
                                    context.startActivity(Intent(Intent.ACTION_VIEW, uri))
                                } catch (_: ActivityNotFoundException) {
                                    // The browser action is hidden on devices without a matching app after the failed attempt.
                                }
                            },
                        )
                    }
                }
            }
        }
    }
}

@Composable
private fun EmptyHome(modifier: Modifier, onAdd: () -> Unit) {
    Box(modifier = modifier.fillMaxSize().padding(32.dp), contentAlignment = Alignment.Center) {
        Column(horizontalAlignment = Alignment.CenterHorizontally) {
            Icon(
                Icons.Outlined.Cable,
                contentDescription = null,
                modifier = Modifier.size(52.dp),
                tint = MaterialTheme.colorScheme.primary,
            )
            Text("编织第一条链路", style = MaterialTheme.typography.titleLarge)
            Text(
                "创建 SSH 或 STCP 配置，本地端口将在这里集中管理。",
                modifier = Modifier.padding(top = 8.dp),
                color = PortWeaveThemeTokens.status.mutedText,
            )
            Text(
                "新建配置",
                modifier = Modifier.padding(top = 20.dp).clickable(onClick = onAdd).padding(12.dp),
                color = MaterialTheme.colorScheme.primary,
                style = MaterialTheme.typography.labelLarge,
            )
        }
    }
}

@Composable
private fun ConfigCard(
    config: TunnelConfig,
    state: TunnelState,
    runtime: RuntimeSnapshot,
    protocol: PortProtocol?,
    onClick: () -> Unit,
    onStart: () -> Unit,
    onStop: () -> Unit,
    onOpenBrowser: (PortProtocol) -> Unit,
) {
    val running = state is TunnelState.ResolvingDependencies || state is TunnelState.WaitingDependency ||
        state is TunnelState.Starting || state is TunnelState.Ready ||
        state is TunnelState.Reconnecting || state is TunnelState.Blocked || state is TunnelState.Stopping
    val (statusText, statusColor) = when (state) {
        TunnelState.Stopped -> "未运行" to PortWeaveThemeTokens.status.mutedText
        TunnelState.ResolvingDependencies -> "正在解析依赖" to PortWeaveThemeTokens.status.warning
        is TunnelState.WaitingDependency -> "等待依赖就绪" to PortWeaveThemeTokens.status.warning
        is TunnelState.Starting -> "正在连接" to PortWeaveThemeTokens.status.warning
        is TunnelState.Ready -> "已就绪" to PortWeaveThemeTokens.status.success
        is TunnelState.Reconnecting -> "正在重连 · ${state.attempt}" to PortWeaveThemeTokens.status.warning
        is TunnelState.Blocked -> "等待处理" to PortWeaveThemeTokens.status.warning
        TunnelState.Stopping -> "正在停止" to PortWeaveThemeTokens.status.warning
        is TunnelState.Failed -> "失败 · ${state.error.code.name}" to MaterialTheme.colorScheme.error
    }
    Card(
        modifier = Modifier
            .fillMaxWidth()
            .semantics {
                contentDescription = "${config.name}，${config.kind.displayName()}，$statusText，点击编辑"
                stateDescription = statusText
                customActions = listOfNotNull(
                    CustomAccessibilityAction("查看详情") { onClick(); true },
                    CustomAccessibilityAction(if (running) "停止映射" else "启动映射") {
                        if (running) onStop() else onStart()
                        true
                    },
                    protocol?.let {
                        CustomAccessibilityAction("在浏览器中打开") { onOpenBrowser(it); true }
                    },
                )
            }
            .clickable(onClickLabel = "查看详情", onClick = onClick),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
        border = BorderStroke(1.dp, PortWeaveThemeTokens.status.border),
    ) {
        Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Column(modifier = Modifier.weight(1f)) {
                    Text(config.name, style = MaterialTheme.typography.titleMedium)
                    Text(
                        config.kind.displayName(),
                        color = PortWeaveThemeTokens.status.mutedText,
                        style = MaterialTheme.typography.bodyMedium,
                    )
                }
                Spacer(Modifier.width(12.dp))
                TunnelStatusPill(statusText, color = statusColor)
            }
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(
                    "127.0.0.1 : ${config.localPort}",
                    modifier = Modifier.weight(1f),
                    color = MaterialTheme.colorScheme.secondary,
                    fontFamily = PortWeaveThemeTokens.mono,
                    style = MaterialTheme.typography.bodyMedium,
                )
                protocol?.let {
                    IconButton(onClick = { onOpenBrowser(it) }) {
                        Icon(Icons.Outlined.Public, contentDescription = "在浏览器中打开")
                    }
                }
            }
            if (config.description.isNotBlank()) {
                Text(config.description, color = PortWeaveThemeTokens.status.mutedText)
            }
            if (config.requiresCredentials) {
                Text("需要补充凭据", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelLarge)
            }
            if (runtime.reconnectCount > 0 || runtime.capabilities.byteCounters) {
                val traffic = if (runtime.capabilities.byteCounters) {
                    " · ↑${formatByteCount(runtime.metrics.bytesUploaded ?: 0)} " +
                        "↓${formatByteCount(runtime.metrics.bytesDownloaded ?: 0)}"
                } else ""
                Text(
                    "重连 ${runtime.reconnectCount} 次$traffic",
                    color = PortWeaveThemeTokens.status.mutedText,
                    style = MaterialTheme.typography.labelMedium,
                )
            }
            Button(
                onClick = if (running) onStop else onStart,
                enabled = state !is TunnelState.Stopping,
                modifier = Modifier.fillMaxWidth(),
            ) {
                Box(
                    modifier = Modifier.size(24.dp),
                    contentAlignment = Alignment.Center,
                ) {
                    Icon(
                        if (running) Icons.Outlined.PowerSettingsNew else Icons.Outlined.PlayArrow,
                        contentDescription = null,
                        modifier = Modifier.size(if (running) 20.dp else 24.dp),
                    )
                }
                Text(if (running) "停止" else if (state is TunnelState.Failed) "重试" else "启动")
            }
        }
    }
}
