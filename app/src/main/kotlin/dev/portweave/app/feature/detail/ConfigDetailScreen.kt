package dev.portweave.app.feature.detail

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.ArrowBack
import androidx.compose.material.icons.automirrored.outlined.List
import androidx.compose.material.icons.outlined.Edit
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material.icons.outlined.Close
import androidx.compose.material.icons.outlined.Lock
import androidx.compose.material.icons.outlined.MoreVert
import androidx.compose.material.icons.outlined.QrCode2
import androidx.compose.material.icons.outlined.WarningAmber
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.Card
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.Alignment
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.platform.LocalConfiguration
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import androidx.compose.ui.window.DialogProperties
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import dev.portweave.app.service.RuntimeSnapshot
import dev.portweave.app.service.RuntimeStateStore
import dev.portweave.app.service.TunnelServiceClient
import dev.portweave.app.ui.component.AdaptiveScreen
import dev.portweave.app.ui.component.PortWeaveSwitch
import dev.portweave.app.ui.component.TunnelStatusPill
import dev.portweave.app.ui.component.displayName
import dev.portweave.app.ui.format.formatByteCount
import dev.portweave.app.ui.format.eventDescription
import dev.portweave.app.ui.format.eventLevelColor
import dev.portweave.app.ui.format.eventTitle
import dev.portweave.app.ui.format.tunnelErrorPresentation
import dev.portweave.app.ui.qrcode.createQrCode
import dev.portweave.app.ui.theme.PortWeaveThemeTokens
import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.data.transfer.ConfigTransferService
import dev.portweave.core.data.transfer.QrConfigPayloadCodec
import dev.portweave.core.diagnostics.EventRepository
import dev.portweave.core.diagnostics.TunnelEvent
import dev.portweave.core.model.DependencyGraph
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelSpec
import dev.portweave.core.security.CredentialRef
import dev.portweave.core.security.CredentialStore
import dev.portweave.tunnel.engine.TunnelState
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ConfigDetailScreen(
    configId: UUID,
    repository: ConfigRepository,
    transfer: ConfigTransferService,
    events: EventRepository,
    runtimeStore: RuntimeStateStore,
    credentialStore: CredentialStore,
    tunnelServiceClient: TunnelServiceClient,
    onBack: () -> Unit,
    onDeleted: () -> Unit,
    onEdit: () -> Unit,
    onLogs: () -> Unit,
) {
    val configs by repository.configs.collectAsStateWithLifecycle(emptyList())
    val runtime by runtimeStore.runtime.collectAsStateWithLifecycle()
    val recent by events.observeForConfig(configId, 10).collectAsStateWithLifecycle(emptyList())
    val config = configs.firstOrNull { it.id == configId }
    val scope = rememberCoroutineScope()
    var showShareCode by remember { mutableStateOf(false) }
    var showDeleteDialog by remember { mutableStateOf(false) }
    var isDeleting by remember { mutableStateOf(false) }
    var deleteError by remember { mutableStateOf<String?>(null) }
    var showMoreActions by remember { mutableStateOf(false) }
    if (showShareCode && config != null) {
        val sharedConfigs = remember(configs, configId) { DependencyGraph(configs).resolve(configId) }
        QrShareDialog(
            configName = config.name,
            configs = sharedConfigs,
            transfer = transfer,
            onDismiss = { showShareCode = false },
        )
    }
    if (showDeleteDialog && config != null) {
        val dependents = remember(configs, configId) { DependencyGraph(configs).transitiveDependents(configId) }
        DeleteConfigDialog(
            configName = config.name,
            dependentNames = dependents.map(TunnelConfig::name),
            isDeleting = isDeleting,
            error = deleteError,
            onDismiss = {
                if (!isDeleting) {
                    showDeleteDialog = false
                    deleteError = null
                }
            },
            onConfirm = {
                isDeleting = true
                deleteError = null
                scope.launch {
                    runCatching {
                        tunnelServiceClient.stopAndAwait(configId)
                        repository.delete(configId)
                        deleteUnusedCredentials(config, repository, credentialStore)
                        runtimeStore.remove(configId)
                    }.onSuccess {
                        showDeleteDialog = false
                        onDeleted()
                    }.onFailure {
                        deleteError = it.message ?: "删除配置失败"
                    }
                    isDeleting = false
                }
            },
        )
    }
    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("配置详情") },
                navigationIcon = {
                    IconButton(onClick = onBack) {
                        Icon(Icons.AutoMirrored.Outlined.ArrowBack, contentDescription = "返回")
                    }
                },
                actions = {
                    IconButton(onClick = onLogs) { Icon(Icons.AutoMirrored.Outlined.List, contentDescription = "查看日志") }
                    IconButton(onClick = onEdit) { Icon(Icons.Outlined.Edit, contentDescription = "编辑") }
                    Box {
                        IconButton(onClick = { showMoreActions = true }, enabled = config != null) {
                            Icon(Icons.Outlined.MoreVert, contentDescription = "更多操作")
                        }
                        DropdownMenu(
                            expanded = showMoreActions,
                            onDismissRequest = { showMoreActions = false },
                            modifier = Modifier.widthIn(min = 240.dp),
                            shape = MaterialTheme.shapes.large,
                            containerColor = MaterialTheme.colorScheme.surface,
                            tonalElevation = 2.dp,
                            shadowElevation = 6.dp,
                        ) {
                            DropdownMenuItem(
                                text = {
                                    Column {
                                        Text("二维码分享")
                                        Text(
                                            "生成可导入的配置二维码",
                                            color = PortWeaveThemeTokens.status.mutedText,
                                            style = MaterialTheme.typography.bodySmall,
                                        )
                                    }
                                },
                                leadingIcon = { Icon(Icons.Outlined.QrCode2, contentDescription = null) },
                                onClick = {
                                    showMoreActions = false
                                    showShareCode = true
                                },
                            )
                            HorizontalDivider(
                                modifier = Modifier.padding(horizontal = 12.dp),
                                color = PortWeaveThemeTokens.status.border,
                            )
                            DropdownMenuItem(
                                text = {
                                    Column {
                                        Text("删除配置", color = MaterialTheme.colorScheme.error)
                                        Text(
                                            "永久删除此映射",
                                            color = PortWeaveThemeTokens.status.mutedText,
                                            style = MaterialTheme.typography.bodySmall,
                                        )
                                    }
                                },
                                leadingIcon = {
                                    Icon(
                                        Icons.Outlined.Delete,
                                        contentDescription = null,
                                        tint = MaterialTheme.colorScheme.error,
                                    )
                                },
                                onClick = {
                                    showMoreActions = false
                                    showDeleteDialog = true
                                },
                            )
                        }
                    }
                },
            )
        },
    ) { padding ->
        if (config == null) {
            Text("配置不存在", modifier = Modifier.padding(padding).padding(24.dp))
            return@Scaffold
        }
        val snapshot = runtime[configId] ?: RuntimeSnapshot()
        AdaptiveScreen(modifier = Modifier.padding(padding)) {
            LazyColumn(
                modifier = Modifier.fillMaxSize(),
                contentPadding = PaddingValues(top = 12.dp, bottom = 24.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp),
            ) {
                item { HeroCard(config, snapshot) }
                item { RuntimeCard(snapshot) }
                item {
                    SectionHeader(
                        title = "最近事件",
                        action = "全部日志",
                        onAction = onLogs,
                    )
                }
                item { RecentEventsCard(recent) }
            }
        }
    }
}

@Composable
private fun DeleteConfigDialog(
    configName: String,
    dependentNames: List<String>,
    isDeleting: Boolean,
    error: String?,
    onDismiss: () -> Unit,
    onConfirm: () -> Unit,
) {
    val blocked = dependentNames.isNotEmpty()
    AlertDialog(
        onDismissRequest = onDismiss,
        icon = { Icon(Icons.Outlined.Delete, contentDescription = null) },
        title = { Text(if (blocked) "无法删除配置" else "删除“$configName”？") },
        text = {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                if (blocked) {
                    Text("此配置仍被 ${dependentNames.size} 个映射依赖，请先在这些配置中解除依赖：")
                    Text(dependentNames.joinToString("、"), color = MaterialTheme.colorScheme.error)
                } else {
                    Text("删除后无法恢复。正在运行的映射会先停止，保存的且不再使用的凭据也会一并删除。")
                }
                error?.let { Text(it, color = MaterialTheme.colorScheme.error) }
            }
        },
        confirmButton = {
            if (blocked) {
                TextButton(onClick = onDismiss) { Text("知道了") }
            } else {
                TextButton(onClick = onConfirm, enabled = !isDeleting) {
                    if (isDeleting) {
                        CircularProgressIndicator(modifier = Modifier.padding(4.dp))
                    } else {
                        Text("删除", color = MaterialTheme.colorScheme.error)
                    }
                }
            }
        },
        dismissButton = if (blocked) null else {
            { TextButton(onClick = onDismiss, enabled = !isDeleting) { Text("取消") } }
        },
    )
}

private suspend fun deleteUnusedCredentials(
    deletedConfig: TunnelConfig,
    repository: ConfigRepository,
    credentialStore: CredentialStore,
) {
    val stillReferenced = repository.snapshot().flatMapTo(mutableSetOf()) { it.credentialRefs() }
    deletedConfig.credentialRefs()
        .filterNot { it in stillReferenced }
        .forEach { runCatching { credentialStore.delete(it) } }
}

private fun TunnelConfig.credentialRefs(): Set<CredentialRef> = when (val value = spec) {
    is TunnelSpec.Ssh -> setOfNotNull(
        value.credentials.password,
        value.credentials.privateKey,
        value.credentials.passphrase,
        value.credentials.hostFingerprint,
    )
    is TunnelSpec.Stcp -> setOfNotNull(value.tokenRef, value.secretKeyRef)
}

@Composable
private fun QrShareDialog(
    configName: String,
    configs: List<TunnelConfig>,
    transfer: ConfigTransferService,
    onDismiss: () -> Unit,
) {
    var includeSecrets by remember { mutableStateOf(false) }
    var qrCode by remember { mutableStateOf<android.graphics.Bitmap?>(null) }
    var error by remember { mutableStateOf<String?>(null) }
    var isLoading by remember { mutableStateOf(true) }
    LaunchedEffect(configs, includeSecrets) {
        qrCode = null
        error = null
        isLoading = true
        runCatching {
            val json = if (includeSecrets) {
                transfer.exportIncludingSecrets(configs, prettyPrint = false)
            } else {
                transfer.export(configs, prettyPrint = false)
            }
            val payload = withContext(Dispatchers.Default) { QrConfigPayloadCodec.encode(json) }
            withContext(Dispatchers.Default) { createQrCode(payload) }
        }.onSuccess { qrCode = it }
            .onFailure { error = it.message ?: "无法生成二维码" }
        isLoading = false
    }
    val maxDialogHeight = LocalConfiguration.current.screenHeightDp.dp * 0.92f
    Dialog(
        onDismissRequest = onDismiss,
        properties = DialogProperties(usePlatformDefaultWidth = false),
    ) {
        Card(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 20.dp)
                .widthIn(max = 400.dp)
                .heightIn(max = maxDialogHeight),
            colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
            shape = RoundedCornerShape(24.dp),
        ) {
            Column(
                modifier = Modifier
                    .verticalScroll(rememberScrollState())
                    .padding(20.dp),
                verticalArrangement = Arrangement.spacedBy(16.dp),
            ) {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(12.dp),
                ) {
                    Box(
                        modifier = Modifier
                            .size(44.dp)
                            .background(MaterialTheme.colorScheme.primary.copy(alpha = 0.10f), CircleShape),
                        contentAlignment = Alignment.Center,
                    ) {
                        Icon(
                            Icons.Outlined.QrCode2,
                            contentDescription = null,
                            tint = MaterialTheme.colorScheme.primary,
                        )
                    }
                    Column(modifier = Modifier.weight(1f)) {
                        Text("分享配置", style = MaterialTheme.typography.titleLarge)
                        Text(
                            configName,
                            color = PortWeaveThemeTokens.status.mutedText,
                            style = MaterialTheme.typography.bodyMedium,
                        )
                    }
                    IconButton(onClick = onDismiss) {
                        Icon(Icons.Outlined.Close, contentDescription = "关闭")
                    }
                }

                Box(
                    modifier = Modifier
                        .fillMaxWidth()
                        .aspectRatio(1f)
                        .background(Color.White, RoundedCornerShape(16.dp))
                        .padding(12.dp),
                    contentAlignment = Alignment.Center,
                ) {
                    when {
                        isLoading -> CircularProgressIndicator()
                        qrCode != null -> Image(
                            bitmap = requireNotNull(qrCode).asImageBitmap(),
                            contentDescription = "$configName 的配置二维码",
                            modifier = Modifier.fillMaxSize(),
                        )
                        else -> Text(
                            error ?: "配置内容过多，无法生成二维码。",
                            color = MaterialTheme.colorScheme.error,
                        )
                    }
                }

                Text(
                    if (configs.size > 1) {
                        "使用 PortWeave 扫描导入 · 含 ${configs.size - 1} 个依赖配置"
                    } else {
                        "使用 PortWeave 扫描即可导入"
                    },
                    modifier = Modifier.align(Alignment.CenterHorizontally),
                    color = PortWeaveThemeTokens.status.mutedText,
                    style = MaterialTheme.typography.bodySmall,
                )

                Card(
                    colors = CardDefaults.cardColors(
                        containerColor = MaterialTheme.colorScheme.primary.copy(alpha = 0.06f),
                    ),
                    border = androidx.compose.foundation.BorderStroke(
                        1.dp,
                        MaterialTheme.colorScheme.primary.copy(alpha = 0.14f),
                    ),
                ) {
                    Row(
                        modifier = Modifier.fillMaxWidth().padding(horizontal = 14.dp, vertical = 12.dp),
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.spacedBy(12.dp),
                    ) {
                        Icon(
                            Icons.Outlined.Lock,
                            contentDescription = null,
                            tint = MaterialTheme.colorScheme.primary,
                        )
                        Column(Modifier.weight(1f)) {
                            Text("包含敏感参数", fontWeight = FontWeight.SemiBold)
                            Text(
                                "密码、私钥、Token 与密钥",
                                color = PortWeaveThemeTokens.status.mutedText,
                                style = MaterialTheme.typography.bodySmall,
                            )
                        }
                        PortWeaveSwitch(
                            checked = includeSecrets,
                            onCheckedChange = { includeSecrets = it },
                        )
                    }
                }

                val noticeColor = if (includeSecrets) {
                    MaterialTheme.colorScheme.error
                } else {
                    PortWeaveThemeTokens.status.success
                }
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .background(noticeColor.copy(alpha = 0.08f), RoundedCornerShape(12.dp))
                        .padding(12.dp),
                    verticalAlignment = Alignment.Top,
                    horizontalArrangement = Arrangement.spacedBy(10.dp),
                ) {
                    Icon(
                        if (includeSecrets) Icons.Outlined.WarningAmber else Icons.Outlined.Lock,
                        contentDescription = null,
                        modifier = Modifier.size(20.dp),
                        tint = noticeColor,
                    )
                    Text(
                        if (includeSecrets) {
                            "二维码包含完整连接凭据，请仅向可信设备展示。"
                        } else {
                            "未包含连接凭据，导入后需要重新填写。"
                        },
                        color = noticeColor,
                        style = MaterialTheme.typography.bodySmall,
                    )
                }

                Button(onClick = onDismiss, modifier = Modifier.fillMaxWidth()) {
                    Text("完成")
                }
            }
        }
    }
}

@Composable
private fun HeroCard(config: TunnelConfig, snapshot: RuntimeSnapshot) {
    val (statusText, statusColor) = tunnelStatus(snapshot.state)
    Card(
        modifier = Modifier.fillMaxWidth().padding(horizontal = 16.dp),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
        border = androidx.compose.foundation.BorderStroke(1.dp, PortWeaveThemeTokens.status.border),
    ) {
        Column(Modifier.padding(18.dp), verticalArrangement = Arrangement.spacedBy(14.dp)) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.Top,
                horizontalArrangement = Arrangement.spacedBy(12.dp),
            ) {
                Column(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(2.dp)) {
                    Text(config.name, style = MaterialTheme.typography.titleLarge)
                    Text(
                        config.kind.displayName(),
                        color = PortWeaveThemeTokens.status.mutedText,
                        style = MaterialTheme.typography.bodyMedium,
                    )
                }
                TunnelStatusPill(statusText, color = statusColor)
            }
            Column(verticalArrangement = Arrangement.spacedBy(3.dp)) {
                Text(
                    "本地访问地址",
                    color = PortWeaveThemeTokens.status.mutedText,
                    style = MaterialTheme.typography.labelMedium,
                )
                Text(
                    "127.0.0.1:${config.localPort}",
                    color = MaterialTheme.colorScheme.secondary,
                    fontFamily = PortWeaveThemeTokens.mono,
                    style = MaterialTheme.typography.titleMedium,
                )
            }
            (snapshot.state as? TunnelState.Failed)?.error?.let { error ->
                Text(
                    tunnelErrorPresentation(error).suggestion,
                    color = MaterialTheme.colorScheme.error,
                    style = MaterialTheme.typography.bodyMedium,
                )
            }
            if (config.description.isNotBlank()) {
                Text(config.description, color = PortWeaveThemeTokens.status.mutedText)
            }
            if (config.requiresCredentials) {
                Text(
                    "需要补充凭据后才能启动",
                    color = MaterialTheme.colorScheme.error,
                    style = MaterialTheme.typography.labelLarge,
                )
            }
        }
    }
}

@Composable
private fun RuntimeCard(snapshot: RuntimeSnapshot) {
    Card(
        modifier = Modifier.fillMaxWidth().padding(horizontal = 16.dp),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
        border = androidx.compose.foundation.BorderStroke(1.dp, PortWeaveThemeTokens.status.border),
    ) {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(14.dp)) {
            Text("运行数据", style = MaterialTheme.typography.titleMedium)
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(8.dp),
            ) {
                MetricTile(
                    "运行时长",
                    snapshot.startedAtEpochMillis?.let { formatDuration(System.currentTimeMillis() - it) } ?: "—",
                    Modifier.weight(1f),
                )
                MetricTile("重连", snapshot.reconnectCount.toString(), Modifier.weight(1f))
                MetricTile("网络变化", snapshot.networkChanges.toString(), Modifier.weight(1f))
            }
            snapshot.lastReadyAtEpochMillis?.let {
                SupportingMetric("最近就绪", java.text.DateFormat.getDateTimeInstance().format(it))
            }
            if (snapshot.capabilities.byteCounters) {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                ) {
                    MetricTile("上行", formatByteCount(snapshot.metrics.bytesUploaded ?: 0), Modifier.weight(1f))
                    MetricTile("下行", formatByteCount(snapshot.metrics.bytesDownloaded ?: 0), Modifier.weight(1f))
                }
            }
            if (snapshot.capabilities.activeConnectionCount) {
                SupportingMetric(
                    "活动连接",
                    "${snapshot.metrics.activeConnections ?: 0}（峰值 ${snapshot.metrics.peakConnections ?: 0}）",
                )
            }
            if (snapshot.capabilities.handshakeLatency) {
                SupportingMetric("握手耗时", snapshot.metrics.handshakeLatencyMillis?.let { "$it ms" } ?: "—")
            }
            if (!snapshot.capabilities.byteCounters && !snapshot.capabilities.activeConnectionCount) {
                Text(
                    "此映射模式不提供流量与连接计数",
                    color = PortWeaveThemeTokens.status.mutedText,
                    style = MaterialTheme.typography.bodyMedium,
                )
            }
        }
    }
}

@Composable
private fun MetricTile(label: String, value: String, modifier: Modifier = Modifier) {
    androidx.compose.material3.Surface(
        modifier = modifier,
        shape = MaterialTheme.shapes.medium,
        color = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.55f),
    ) {
        Column(
            modifier = Modifier.padding(horizontal = 10.dp, vertical = 12.dp),
            verticalArrangement = Arrangement.spacedBy(3.dp),
        ) {
            Text(
                label,
                color = PortWeaveThemeTokens.status.mutedText,
                style = MaterialTheme.typography.labelMedium,
                maxLines = 1,
            )
            Text(value, style = MaterialTheme.typography.titleMedium, maxLines = 1)
        }
    }
}

@Composable
private fun SupportingMetric(label: String, value: String) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Text(label, color = PortWeaveThemeTokens.status.mutedText)
        Text(value, fontWeight = FontWeight.Medium)
    }
}

@Composable
private fun SectionHeader(title: String, action: String, onAction: () -> Unit) {
    Row(
        modifier = Modifier.fillMaxWidth().padding(start = 16.dp, end = 8.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Text(title, modifier = Modifier.weight(1f), style = MaterialTheme.typography.titleMedium)
        TextButton(onClick = onAction) { Text(action) }
    }
}

@Composable
private fun RecentEventsCard(events: List<TunnelEvent>) {
    Card(
        modifier = Modifier.fillMaxWidth().padding(horizontal = 16.dp),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
        border = androidx.compose.foundation.BorderStroke(1.dp, PortWeaveThemeTokens.status.border),
    ) {
        if (events.isEmpty()) {
            Text(
                "暂无运行事件",
                modifier = Modifier.padding(16.dp),
                color = PortWeaveThemeTokens.status.mutedText,
            )
        } else {
            Column {
                events.forEachIndexed { index, event ->
                    EventRow(event)
                    if (index != events.lastIndex) {
                        HorizontalDivider(
                            modifier = Modifier.padding(start = 38.dp),
                            color = PortWeaveThemeTokens.status.border,
                        )
                    }
                }
            }
        }
    }
}

@Composable
private fun EventRow(event: TunnelEvent) {
    val eventColor = eventLevelColor(event.level)
    Row(
        modifier = Modifier.fillMaxWidth().padding(horizontal = 14.dp, vertical = 12.dp),
        verticalAlignment = Alignment.Top,
    ) {
        Box(
            modifier = Modifier
                .padding(top = 6.dp)
                .size(8.dp)
                .background(eventColor, CircleShape),
        )
        Column(modifier = Modifier.padding(start = 10.dp).weight(1f)) {
            Text(eventTitle(event.code), style = MaterialTheme.typography.bodyMedium, fontWeight = FontWeight.Medium)
            eventDescription(event.code, event.messageKey)?.let {
                Text(it, color = PortWeaveThemeTokens.status.mutedText, style = MaterialTheme.typography.bodyMedium)
            }
        }
        Text(
            java.text.DateFormat.getTimeInstance(java.text.DateFormat.SHORT).format(event.occurredAtEpochMillis),
            modifier = Modifier.width(64.dp),
            color = PortWeaveThemeTokens.status.mutedText,
            style = MaterialTheme.typography.labelMedium,
        )
    }
}

@Composable
private fun tunnelStatus(state: TunnelState): Pair<String, Color> = when (state) {
    TunnelState.Stopped -> "未运行" to PortWeaveThemeTokens.status.mutedText
    TunnelState.ResolvingDependencies -> "解析依赖" to PortWeaveThemeTokens.status.warning
    is TunnelState.WaitingDependency -> "等待依赖" to PortWeaveThemeTokens.status.warning
    is TunnelState.Starting -> "正在连接" to PortWeaveThemeTokens.status.warning
    is TunnelState.Ready -> "已就绪" to PortWeaveThemeTokens.status.success
    is TunnelState.Reconnecting -> if (state.dormant) {
        "暂停重连 · 网络变化或打开应用后继续" to PortWeaveThemeTokens.status.warning
    } else {
        "重连中 · ${state.attempt}" to PortWeaveThemeTokens.status.warning
    }
    is TunnelState.Blocked -> "等待处理" to PortWeaveThemeTokens.status.warning
    TunnelState.Stopping -> "正在停止" to PortWeaveThemeTokens.status.warning
    is TunnelState.Failed -> tunnelErrorPresentation(state.error).title to MaterialTheme.colorScheme.error
}

private fun formatDuration(millis: Long): String {
    val seconds = millis.coerceAtLeast(0) / 1_000
    return "%02d:%02d:%02d".format(seconds / 3_600, seconds / 60 % 60, seconds % 60)
}
