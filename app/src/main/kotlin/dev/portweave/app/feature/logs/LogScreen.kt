package dev.portweave.app.feature.logs

import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.ArrowBack
import androidx.compose.material.icons.automirrored.outlined.ReceiptLong
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material.icons.outlined.FileDownload
import androidx.compose.material.icons.outlined.MoreVert
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FilterChip
import androidx.compose.material3.FilterChipDefaults
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import dev.portweave.app.ui.component.AdaptiveScreen
import dev.portweave.app.ui.format.eventDescription
import dev.portweave.app.ui.format.eventLevelColor
import dev.portweave.app.ui.format.eventLevelLabel
import dev.portweave.app.ui.format.eventTitle
import dev.portweave.app.ui.theme.PortWeaveThemeTokens
import dev.portweave.core.diagnostics.EventLevel
import dev.portweave.core.diagnostics.EventRepository
import dev.portweave.core.diagnostics.TunnelEvent
import java.text.DateFormat
import java.util.UUID
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun LogScreen(events: EventRepository, configId: UUID?, onBack: () -> Unit) {
    val flow = remember(configId) { if (configId == null) events.observeRecent() else events.observeForConfig(configId) }
    val all by flow.collectAsStateWithLifecycle(emptyList())
    var level by remember { mutableStateOf<EventLevel?>(null) }
    var showActions by remember { mutableStateOf(false) }
    var confirmClear by remember { mutableStateOf(false) }
    val filtered = all.filter { level == null || it.level == level }
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val export = rememberLauncherForActivityResult(ActivityResultContracts.CreateDocument("application/x-ndjson")) { uri ->
        uri ?: return@rememberLauncherForActivityResult
        scope.launch {
            context.contentResolver.openOutputStream(uri)?.bufferedWriter()?.use { writer ->
                filtered.asReversed().forEach { event ->
                    writer.appendLine(Json.encodeToString(kotlinx.serialization.json.JsonObject.serializer(), buildJsonObject {
                        put("configId", event.configId.toString())
                        put("generation", event.generation)
                        put("level", event.level.name)
                        put("code", event.code)
                        put("messageKey", event.messageKey)
                        put("occurredAtEpochMillis", event.occurredAtEpochMillis)
                        put("traceId", event.traceId)
                    }))
                }
            }
        }
    }

    if (confirmClear) {
        AlertDialog(
            onDismissRequest = { confirmClear = false },
            icon = { Icon(Icons.Outlined.Delete, contentDescription = null) },
            title = { Text("清空日志？") },
            text = { Text(if (configId == null) "将删除当前保存的全部结构化日志。" else "将删除此映射的全部结构化日志。") },
            confirmButton = {
                TextButton(
                    onClick = {
                        confirmClear = false
                        scope.launch { events.clear(configId) }
                    },
                ) { Text("清空", color = MaterialTheme.colorScheme.error) }
            },
            dismissButton = { TextButton(onClick = { confirmClear = false }) { Text("取消") } },
        )
    }

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("结构化日志") },
                navigationIcon = {
                    IconButton(onClick = onBack) {
                        Icon(Icons.AutoMirrored.Outlined.ArrowBack, contentDescription = "返回")
                    }
                },
                actions = {
                    IconButton(onClick = { export.launch("portweave-events.jsonl") }) {
                        Icon(Icons.Outlined.FileDownload, contentDescription = "导出日志")
                    }
                    Box {
                        IconButton(onClick = { showActions = true }) {
                            Icon(Icons.Outlined.MoreVert, contentDescription = "更多日志操作")
                        }
                        DropdownMenu(
                            expanded = showActions,
                            onDismissRequest = { showActions = false },
                            modifier = Modifier.fillMaxWidth(0.58f),
                            shape = MaterialTheme.shapes.large,
                            containerColor = MaterialTheme.colorScheme.surface,
                        ) {
                            DropdownMenuItem(
                                text = {
                                    Column {
                                        Text("清空日志", color = MaterialTheme.colorScheme.error)
                                        Text(
                                            "删除当前范围内的记录",
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
                                    showActions = false
                                    confirmClear = true
                                },
                            )
                        }
                    }
                },
            )
        },
    ) { padding ->
        AdaptiveScreen(modifier = Modifier.padding(padding)) {
            Column(Modifier.fillMaxSize()) {
                LogFilters(selected = level, count = filtered.size, onSelected = { level = it })
                if (filtered.isEmpty()) {
                    EmptyLogs(modifier = Modifier.fillMaxSize())
                } else {
                    LazyColumn(
                        modifier = Modifier.fillMaxSize(),
                        contentPadding = PaddingValues(horizontal = 16.dp, vertical = 8.dp),
                        verticalArrangement = Arrangement.spacedBy(10.dp),
                    ) {
                        items(filtered, key = { it.id }) { event -> LogEventCard(event) }
                    }
                }
            }
        }
    }
}

@Composable
private fun LogFilters(selected: EventLevel?, count: Int, onSelected: (EventLevel?) -> Unit) {
    Column(
        modifier = Modifier.fillMaxWidth().padding(horizontal = 16.dp, vertical = 8.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        Row(modifier = Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
            Text("筛选", modifier = Modifier.weight(1f), style = MaterialTheme.typography.titleMedium)
            Text("$count 条记录", color = PortWeaveThemeTokens.status.mutedText, style = MaterialTheme.typography.labelMedium)
        }
        FlowRow(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalArrangement = Arrangement.spacedBy(4.dp),
        ) {
            FilterChip(
                selected = selected == null,
                onClick = { onSelected(null) },
                label = { Text("全部") },
                colors = logFilterColors(),
            )
            EventLevel.entries.forEach { item ->
                FilterChip(
                    selected = selected == item,
                    onClick = { onSelected(item) },
                    label = { Text(eventLevelLabel(item)) },
                    leadingIcon = {
                        Box(Modifier.size(7.dp).background(eventLevelColor(item), CircleShape))
                    },
                    colors = logFilterColors(),
                )
            }
        }
    }
}

@Composable
private fun logFilterColors() = FilterChipDefaults.filterChipColors(
    selectedContainerColor = MaterialTheme.colorScheme.primary.copy(alpha = 0.12f),
    selectedLabelColor = MaterialTheme.colorScheme.primary,
)

@Composable
private fun LogEventCard(event: TunnelEvent) {
    val levelColor = eventLevelColor(event.level)
    Card(
        modifier = Modifier.fillMaxWidth(),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
        border = BorderStroke(1.dp, PortWeaveThemeTokens.status.border),
    ) {
        Column(modifier = Modifier.padding(14.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Row(modifier = Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
                Surface(shape = CircleShape, color = levelColor.copy(alpha = 0.12f), contentColor = levelColor) {
                    Text(
                        eventLevelLabel(event.level),
                        modifier = Modifier.padding(horizontal = 9.dp, vertical = 4.dp),
                        style = MaterialTheme.typography.labelMedium,
                    )
                }
                Text(
                    DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT)
                        .format(event.occurredAtEpochMillis),
                    modifier = Modifier.padding(start = 10.dp).weight(1f),
                    color = PortWeaveThemeTokens.status.mutedText,
                    style = MaterialTheme.typography.labelMedium,
                )
            }
            Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
                Text(eventTitle(event.code), style = MaterialTheme.typography.titleMedium)
                eventDescription(event.code, event.messageKey)?.let { Text(it, color = PortWeaveThemeTokens.status.mutedText) }
                Text(
                    event.messageKey,
                    color = PortWeaveThemeTokens.status.mutedText,
                    fontFamily = PortWeaveThemeTokens.mono,
                    style = MaterialTheme.typography.labelMedium,
                )
            }
            Row(modifier = Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
                Text(
                    event.code,
                    modifier = Modifier.weight(1f),
                    color = PortWeaveThemeTokens.status.mutedText,
                    fontFamily = PortWeaveThemeTokens.mono,
                    style = MaterialTheme.typography.labelMedium,
                )
                Text(
                    "追踪 ${event.traceId.take(8)}",
                    color = PortWeaveThemeTokens.status.mutedText,
                    fontFamily = PortWeaveThemeTokens.mono,
                    style = MaterialTheme.typography.labelMedium,
                )
            }
        }
    }
}

@Composable
private fun EmptyLogs(modifier: Modifier = Modifier) {
    Column(
        modifier = modifier.padding(32.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center,
    ) {
        Icon(
            Icons.AutoMirrored.Outlined.ReceiptLong,
            contentDescription = null,
            modifier = Modifier.size(44.dp),
            tint = PortWeaveThemeTokens.status.mutedText,
        )
        Text("暂无日志", modifier = Modifier.padding(top = 12.dp), style = MaterialTheme.typography.titleMedium)
        Text("运行映射后，事件会显示在这里。", color = PortWeaveThemeTokens.status.mutedText)
    }
}
