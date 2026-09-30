package dev.portweave.app.feature.editor

import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.ArrowBack
import androidx.compose.material.icons.outlined.Key
import androidx.compose.material.icons.outlined.QrCodeScanner
import androidx.compose.material.icons.outlined.Save
import androidx.compose.material.icons.outlined.VerifiedUser
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FilterChip
import androidx.compose.material3.FilterChipDefaults
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.stateDescription
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import dev.portweave.app.ui.component.displayName
import dev.portweave.app.ui.component.AdaptiveScreen
import dev.portweave.app.ui.component.PortWeaveSwitch
import dev.portweave.app.ui.theme.PortWeaveThemeTokens
import dev.portweave.app.service.TunnelServiceClient
import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.data.transfer.ConfigTransferService
import dev.portweave.core.data.transfer.ImportConflictStrategy
import dev.portweave.core.model.SshAuthMethod
import dev.portweave.core.model.SshHostKeyPolicy
import dev.portweave.core.model.StcpWireProtocol
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.security.CredentialStore
import dev.portweave.tunnel.engine.ssh.SshTunnelBackend
import com.journeyapps.barcodescanner.ScanContract
import com.journeyapps.barcodescanner.ScanOptions
import android.widget.Toast
import kotlinx.coroutines.launch

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ConfigEditorScreen(
    repository: ConfigRepository,
    transfer: ConfigTransferService,
    credentialStore: CredentialStore,
    sshBackend: SshTunnelBackend,
    tunnelServiceClient: TunnelServiceClient,
    configId: String?,
    onBack: () -> Unit,
) {
    val viewModel: ConfigEditorViewModel = viewModel(
        key = "editor-${configId ?: "new"}",
        factory = ConfigEditorViewModel.factory(
            repository,
            credentialStore,
            sshBackend,
            tunnelServiceClient,
            configId,
        ),
    )
    val state by viewModel.state.collectAsStateWithLifecycle()
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val privateKeyPicker = rememberLauncherForActivityResult(ActivityResultContracts.OpenDocument()) { uri ->
        uri?.let { viewModel.importPrivateKey(it, context.contentResolver) }
    }
    val qrScanner = rememberLauncherForActivityResult(ScanContract()) { result ->
        val contents = result.contents ?: return@rememberLauncherForActivityResult
        scope.launch {
            val message = runCatching {
                val imported = transfer.import(contents, ImportConflictStrategy.CREATE_COPY)
                "已导入 ${imported.imported} 个配置，请补充所需凭据"
            }.getOrElse { "二维码导入失败：${it.message ?: "格式无效"}" }
            Toast.makeText(context, message, Toast.LENGTH_LONG).show()
            if (message.startsWith("已导入")) onBack()
        }
    }
    fun launchQrScanner() {
        qrScanner.launch(
            ScanOptions()
                .setDesiredBarcodeFormats(ScanOptions.QR_CODE)
                .setPrompt("扫描 PortWeave 配置二维码")
                .setBeepEnabled(false)
                // CaptureActivity uses fullSensor in the manifest, so its layout and prompt
                // follow the way the device is actually being held.
                .setOrientationLocked(false),
        )
    }

    LaunchedEffect(viewModel) { viewModel.saved.collect { onBack() } }

    state.discoveredHostKey?.let { discovered ->
        AlertDialog(
            onDismissRequest = viewModel::dismissDiscoveredHostKey,
            icon = { Icon(Icons.Outlined.VerifiedUser, contentDescription = null) },
            title = { Text("确认 SSH 主机身份") },
            text = {
                Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Text("请通过可信渠道核对以下信息。确认后，任何密钥变化都会阻止连接。")
                    Text("算法：${discovered.algorithm}")
                    Text(
                        discovered.sha256Fingerprint,
                        fontFamily = PortWeaveThemeTokens.mono,
                        style = MaterialTheme.typography.bodyMedium,
                    )
                }
            },
            confirmButton = { TextButton(onClick = viewModel::acceptDiscoveredHostKey) { Text("确认并使用") } },
            dismissButton = { TextButton(onClick = viewModel::dismissDiscoveredHostKey) { Text("取消") } },
        )
    }

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text(if (configId == null) "新建映射" else "编辑映射") },
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
                modifier = Modifier
                    .fillMaxSize()
                    .verticalScroll(rememberScrollState())
                    .padding(horizontal = 16.dp, vertical = 12.dp),
                verticalArrangement = Arrangement.spacedBy(16.dp),
            ) {
            if (configId == null) {
                OutlinedButton(onClick = ::launchQrScanner, modifier = Modifier.fillMaxWidth().heightIn(min = 52.dp)) {
                    Icon(Icons.Outlined.QrCodeScanner, contentDescription = "扫码导入配置")
                    Text("扫描二维码导入", modifier = Modifier.padding(start = 8.dp))
                }
            }
            EditorSection(
                title = "基础信息",
                description = "选择映射类型并设置本地监听信息。",
            ) {
                FlowRow(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    verticalArrangement = Arrangement.spacedBy(4.dp),
                ) {
                    TunnelKind.entries.forEach { kind ->
                        FilterChip(
                            selected = state.kind == kind,
                            onClick = { viewModel.setKind(kind) },
                            label = { Text(kind.displayName()) },
                            colors = editorFilterChipColors(),
                        )
                    }
                }
                OutlinedTextField(
                    state.name,
                    viewModel::setName,
                    Modifier.fillMaxWidth(),
                    label = { Text("配置名称") },
                    singleLine = true,
                )
                OutlinedTextField(
                    state.localPort,
                    viewModel::setLocalPort,
                    Modifier.fillMaxWidth(),
                    label = { Text("本地端口") },
                    prefix = { Text("127.0.0.1 : ", fontFamily = PortWeaveThemeTokens.mono) },
                    keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
                    singleLine = true,
                )
                OutlinedTextField(
                    state.description,
                    viewModel::setDescription,
                    Modifier.fillMaxWidth(),
                    label = { Text("说明（可选）") },
                    minLines = 2,
                )
            }
            EditorSection(
                title = "连接入口",
                description = "直接连接远端，或通过另一个映射建立连接。",
            ) {
                FlowRow(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    verticalArrangement = Arrangement.spacedBy(4.dp),
                ) {
                    FilterChip(
                        selected = state.dependencyConfigId == null,
                        onClick = { viewModel.setDependency(null) },
                        label = { Text("直接连接") },
                        colors = editorFilterChipColors(),
                    )
                    state.dependencyOptions.forEach { option ->
                        FilterChip(
                            selected = state.dependencyConfigId == option.id,
                            onClick = { viewModel.setDependency(option.id) },
                            enabled = !option.disabledByCycle,
                            label = {
                                Text(if (option.disabledByCycle) "${option.name} · 会形成循环" else option.name)
                            },
                            colors = editorFilterChipColors(),
                        )
                    }
                }
                if (state.dependencyChainPreview.isNotEmpty()) {
                    Text(
                        "启动链路：${state.dependencyChainPreview.joinToString(" → ")}",
                        color = MaterialTheme.colorScheme.secondary,
                        style = MaterialTheme.typography.bodyMedium,
                    )
                }
            }

            if (state.kind == TunnelKind.SSH) {
                SshFields(
                    state = state,
                    viewModel = viewModel,
                    onPickPrivateKey = { privateKeyPicker.launch(arrayOf("*/*")) },
                )
            } else {
                StcpFields(state, viewModel)
            }

            EditorSection(title = "运行策略") {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Column(modifier = Modifier.weight(1f)) {
                        Text("断线后自动恢复", style = MaterialTheme.typography.bodyLarge)
                        Text(
                            "使用有上限的指数退避重连",
                            color = PortWeaveThemeTokens.status.mutedText,
                            style = MaterialTheme.typography.bodyMedium,
                        )
                    }
                    PortWeaveSwitch(checked = state.autoRecover, onCheckedChange = viewModel::setAutoRecover)
                }
            }
            state.error?.let { Text(it, color = MaterialTheme.colorScheme.error) }
            Button(
                onClick = viewModel::save,
                enabled = !state.isLoading && !state.isSaving && !state.isDiscovering,
                modifier = Modifier.fillMaxWidth().heightIn(min = 52.dp),
            ) {
                Icon(Icons.Outlined.Save, contentDescription = null)
                Text(if (state.isSaving) "正在保存" else "保存配置", Modifier.padding(start = 8.dp))
            }
            }
        }
    }
}

@Composable
private fun SshFields(
    state: ConfigEditorState,
    viewModel: ConfigEditorViewModel,
    onPickPrivateKey: () -> Unit,
) {
    EditorSection(
        title = "SSH 连接",
        description = "设置服务器入口和用于校验身份的逻辑地址。",
    ) {
        if (state.dependencyConfigId == null) {
            HostPortRow(state.sshHost, viewModel::setSshHost, state.sshPort, viewModel::setSshPort, "SSH 主机")
        } else {
            Text(
                "将通过所选依赖映射的本地端口连接；获取主机指纹时也会临时启动该依赖。",
                color = PortWeaveThemeTokens.status.mutedText,
                style = MaterialTheme.typography.bodyMedium,
            )
        }
        HostPortRow(
            state.logicalHost,
            viewModel::setLogicalHost,
            state.logicalPort,
            viewModel::setLogicalPort,
            "逻辑主机身份",
        )
        Text(
            "逻辑身份用于主机密钥校验，通过依赖链拨号时不会变成 127.0.0.1。",
            color = PortWeaveThemeTokens.status.mutedText,
            style = MaterialTheme.typography.bodyMedium,
        )
        OutlinedTextField(
            state.username,
            viewModel::setUsername,
            Modifier.fillMaxWidth(),
            label = { Text("用户名") },
            singleLine = true,
        )
    }
    EditorSection(
        title = "认证与主机身份",
        description = "凭据仅保存在设备的安全存储中。",
    ) {
        EditorSubsectionTitle("登录认证")
        FlowRow(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalArrangement = Arrangement.spacedBy(4.dp),
        ) {
            SshAuthMethod.entries.forEach { method ->
                FilterChip(
                    selected = state.authMethod == method,
                    onClick = { viewModel.setAuthMethod(method) },
                    label = { Text(if (method == SshAuthMethod.PASSWORD) "密码" else "私钥") },
                    colors = editorFilterChipColors(),
                )
            }
        }
        if (state.authMethod == SshAuthMethod.PASSWORD) {
            OutlinedTextField(
                state.password,
                viewModel::setPassword,
                Modifier.fillMaxWidth(),
                label = { Text(if (state.hasStoredPassword) "密码（留空则保持不变）" else "密码") },
                visualTransformation = PasswordVisualTransformation(),
                singleLine = true,
            )
        } else {
            OutlinedButton(onClick = onPickPrivateKey, modifier = Modifier.fillMaxWidth()) {
                Icon(Icons.Outlined.Key, contentDescription = null)
                Text(
                    when {
                        state.hasPrivateKey -> "已选择新私钥"
                        state.hasStoredPrivateKey -> "私钥已安全保存 · 重新选择"
                        else -> "从设备导入私钥（最大 256 KiB）"
                    },
                    Modifier.padding(start = 8.dp),
                )
            }
            OutlinedTextField(
                state.passphrase,
                viewModel::setPassphrase,
                Modifier.fillMaxWidth(),
                label = { Text("私钥密码短语（可选）") },
                visualTransformation = PasswordVisualTransformation(),
                singleLine = true,
            )
        }
        HorizontalDivider(color = PortWeaveThemeTokens.status.border)
        EditorSubsectionTitle("主机身份校验")
        FlowRow(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalArrangement = Arrangement.spacedBy(4.dp),
        ) {
            SshHostKeyPolicy.entries.forEach { policy ->
                FilterChip(
                    selected = state.hostKeyPolicy == policy,
                    onClick = { viewModel.setHostKeyPolicy(policy) },
                    label = { Text(if (policy == SshHostKeyPolicy.TOFU) "首次确认" else "固定指纹") },
                    colors = editorFilterChipColors(),
                )
            }
        }
        if (state.hostKeyPolicy == SshHostKeyPolicy.FIXED) {
            OutlinedTextField(
                state.hostFingerprint,
                viewModel::setHostFingerprint,
                Modifier.fillMaxWidth(),
                label = { Text(if (state.hasStoredFingerprint) "SHA256 指纹（留空保持不变）" else "SHA256 指纹") },
                textStyle = MaterialTheme.typography.bodyMedium.copy(fontFamily = PortWeaveThemeTokens.mono),
                singleLine = true,
            )
        } else {
            Text(
                when {
                    state.hostFingerprint.isNotEmpty() -> "本次发现的主机身份已确认"
                    state.hasStoredFingerprint -> "已保存确认过的主机身份"
                    else -> "连接前必须先获取并人工确认主机身份"
                },
                color = PortWeaveThemeTokens.status.mutedText,
            )
        }
        OutlinedButton(
            onClick = viewModel::discoverHostKey,
            enabled = !state.isDiscovering,
            modifier = Modifier.fillMaxWidth(),
        ) {
            Icon(Icons.Outlined.VerifiedUser, contentDescription = null)
            Text(if (state.isDiscovering) "正在获取主机身份" else "获取并核对主机指纹", Modifier.padding(start = 8.dp))
        }
    }
    EditorSection(
        title = "远端目标",
        description = "目标地址由 SSH 服务器所在网络解析。",
    ) {
        HostPortRow(
            state.remoteTargetHost,
            viewModel::setRemoteTargetHost,
            state.remoteTargetPort,
            viewModel::setRemoteTargetPort,
            "目标主机",
        )
        ResponsiveFieldRow { first, second ->
            OutlinedTextField(
                state.connectTimeoutSeconds,
                viewModel::setConnectTimeout,
                first,
                label = { Text("连接超时（秒）") },
                keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
                singleLine = true,
            )
            OutlinedTextField(
                state.keepAliveSeconds,
                viewModel::setKeepAlive,
                second,
                label = { Text("保活间隔（秒）") },
                keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
                singleLine = true,
            )
        }
    }
}

@Composable
private fun HostPortRow(
    host: String,
    onHostChange: (String) -> Unit,
    port: String,
    onPortChange: (String) -> Unit,
    hostLabel: String,
) {
    ResponsiveFieldRow(firstWeight = 2f) { first, second ->
        OutlinedTextField(host, onHostChange, first, label = { Text(hostLabel) }, singleLine = true)
        OutlinedTextField(
            port,
            onPortChange,
            second,
            label = { Text("端口") },
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
            singleLine = true,
        )
    }
}

@Composable
private fun ResponsiveFieldRow(
    firstWeight: Float = 1f,
    fields: @Composable (Modifier, Modifier) -> Unit,
) {
    BoxWithConstraints(modifier = Modifier.fillMaxWidth()) {
        if (maxWidth < 520.dp) {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                fields(Modifier.fillMaxWidth(), Modifier.fillMaxWidth())
            }
        } else {
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                fields(Modifier.weight(firstWeight), Modifier.weight(1f))
            }
        }
    }
}

@Composable
private fun editorFilterChipColors() = FilterChipDefaults.filterChipColors(
    selectedContainerColor = MaterialTheme.colorScheme.primary.copy(alpha = 0.12f),
    selectedLabelColor = MaterialTheme.colorScheme.primary,
    selectedLeadingIconColor = MaterialTheme.colorScheme.primary,
)

@Composable
private fun EditorSection(
    title: String,
    description: String? = null,
    content: @Composable ColumnScope.() -> Unit,
) {
    Card(
        modifier = Modifier.fillMaxWidth(),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
        border = BorderStroke(1.dp, PortWeaveThemeTokens.status.border),
    ) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
                Text(title, style = MaterialTheme.typography.titleMedium)
                description?.let {
                    Text(
                        it,
                        color = PortWeaveThemeTokens.status.mutedText,
                        style = MaterialTheme.typography.bodyMedium,
                    )
                }
            }
            content()
        }
    }
}

@Composable
private fun EditorSubsectionTitle(text: String) {
    Text(text, color = MaterialTheme.colorScheme.onSurface, style = MaterialTheme.typography.labelLarge)
}

@Composable
private fun StcpFields(state: ConfigEditorState, viewModel: ConfigEditorViewModel) {
    EditorSection(
        title = "frps 连接",
        description = "设置 frps 入口、TLS 身份和登录凭据。",
    ) {
        HostPortRow(state.frpsHost, viewModel::setFrpsHost, state.frpsPort, viewModel::setFrpsPort, "frps 主机")
        HostPortRow(
            state.frpsLogicalHost,
            viewModel::setFrpsLogicalHost,
            state.frpsLogicalPort,
            viewModel::setFrpsLogicalPort,
            "TLS 逻辑身份",
        )
        Text(
            "逻辑身份独立于拨号地址，并用于 TLS SNI 和证书校验。",
            color = PortWeaveThemeTokens.status.mutedText,
            style = MaterialTheme.typography.bodyMedium,
        )
        ToggleRow("使用 frps Token", state.useFrpsToken, viewModel::setUseFrpsToken)
        if (state.useFrpsToken) {
            OutlinedTextField(
                state.frpsToken,
                viewModel::setFrpsToken,
                Modifier.fillMaxWidth(),
                label = { Text(if (state.hasStoredFrpsToken) "frps Token（留空保持不变）" else "frps Token") },
                visualTransformation = PasswordVisualTransformation(),
                singleLine = true,
            )
        }
        OutlinedTextField(
            state.frpcUser,
            viewModel::setFrpcUser,
            Modifier.fillMaxWidth(),
            label = { Text("frpc 用户（可选）") },
            singleLine = true,
        )
    }
    EditorSection(
        title = "STCP Visitor",
        description = "填写服务端名称和双方约定的访问密钥。",
    ) {
        OutlinedTextField(
            state.stcpServerUser,
            viewModel::setStcpServerUser,
            Modifier.fillMaxWidth(),
            label = { Text("STCP 服务端用户（可选）") },
            singleLine = true,
        )
        OutlinedTextField(
            state.stcpServerName,
            viewModel::setStcpServerName,
            Modifier.fillMaxWidth(),
            label = { Text("STCP 服务名") },
            singleLine = true,
        )
        OutlinedTextField(
            state.stcpSecretKey,
            viewModel::setStcpSecretKey,
            Modifier.fillMaxWidth(),
            label = { Text(if (state.hasStoredStcpSecret) "STCP 密钥（留空保持不变）" else "STCP 密钥") },
            visualTransformation = PasswordVisualTransformation(),
            singleLine = true,
        )
    }
    EditorSection(
        title = "协议与传输",
        description = "保持与 frps 服务端配置一致。",
    ) {
        FlowRow(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalArrangement = Arrangement.spacedBy(4.dp),
        ) {
            StcpWireProtocol.entries.forEach { protocol ->
                FilterChip(
                    selected = state.stcpWireProtocol == protocol,
                    onClick = { viewModel.setStcpWireProtocol(protocol) },
                    label = { Text(protocol.name) },
                    colors = editorFilterChipColors(),
                )
            }
        }
        ToggleRow("加密 STCP 载荷", state.stcpUseEncryption, viewModel::setStcpUseEncryption)
        ToggleRow("压缩 STCP 载荷", state.stcpUseCompression, viewModel::setStcpUseCompression)
        OutlinedTextField(
            state.frpsTlsLeafPin,
            viewModel::setFrpsTlsLeafPin,
            Modifier.fillMaxWidth(),
            label = { Text("TLS 叶证书 SHA-256（可选）") },
            supportingText = { Text("64 位十六进制") },
            textStyle = MaterialTheme.typography.bodyMedium.copy(fontFamily = PortWeaveThemeTokens.mono),
            singleLine = true,
        )
        OutlinedTextField(
            state.stcpConnectTimeoutSeconds,
            viewModel::setStcpConnectTimeout,
            Modifier.fillMaxWidth(),
            label = { Text("连接与就绪超时（秒）") },
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
            singleLine = true,
        )
    }
}

@Composable
private fun ToggleRow(label: String, checked: Boolean, onCheckedChange: (Boolean) -> Unit) {
    Row(
        modifier = Modifier.fillMaxWidth().semantics(mergeDescendants = true) {
            stateDescription = if (checked) "已开启" else "已关闭"
        },
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Text(label, modifier = Modifier.weight(1f))
        PortWeaveSwitch(checked = checked, onCheckedChange = onCheckedChange)
    }
}
