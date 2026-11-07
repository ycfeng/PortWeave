package dev.portweave.app.feature.editor

import android.content.ContentResolver
import android.net.Uri
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import dev.portweave.app.service.TunnelServiceClient
import dev.portweave.app.ui.format.tunnelErrorPresentation
import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.DependencyGraph
import dev.portweave.core.model.dependencyConfigId
import dev.portweave.core.model.FrpsServerIdentity
import dev.portweave.core.model.SshAuthMethod
import dev.portweave.core.model.SshCredentialRefs
import dev.portweave.core.model.SshHostKeyPolicy
import dev.portweave.core.model.SshServerIdentity
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.core.model.StcpWireProtocol
import dev.portweave.core.security.CredentialKind
import dev.portweave.core.security.CredentialRef
import dev.portweave.core.security.CredentialStore
import dev.portweave.tunnel.engine.ssh.DiscoveredHostKey
import dev.portweave.tunnel.engine.ssh.SshBackendException
import dev.portweave.tunnel.engine.ssh.SshHostKeyFingerprint
import dev.portweave.tunnel.engine.ssh.SshTunnelBackend
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.launch
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

data class ConfigEditorState(
    val isLoading: Boolean = true,
    val name: String = "",
    val description: String = "",
    val localPort: String = "",
    val kind: TunnelKind = TunnelKind.SSH,
    val autoRecover: Boolean = true,
    val dependencyConfigId: UUID? = null,
    val dependencyOptions: List<DependencyOption> = emptyList(),
    val dependencyChainPreview: List<String> = emptyList(),
    val sshHost: String = "",
    val sshPort: String = "22",
    val logicalHost: String = "",
    val logicalPort: String = "22",
    val username: String = "",
    val authMethod: SshAuthMethod = SshAuthMethod.PASSWORD,
    val password: String = "",
    val hasStoredPassword: Boolean = false,
    val hasPrivateKey: Boolean = false,
    val hasStoredPrivateKey: Boolean = false,
    val passphrase: String = "",
    val hostKeyPolicy: SshHostKeyPolicy = SshHostKeyPolicy.TOFU,
    val hostFingerprint: String = "",
    val hasStoredFingerprint: Boolean = false,
    val remoteTargetHost: String = "",
    val remoteTargetPort: String = "",
    val connectTimeoutSeconds: String = "10",
    val keepAliveSeconds: String = "30",
    val frpsHost: String = "",
    val frpsPort: String = "7000",
    val frpsLogicalHost: String = "",
    val frpsLogicalPort: String = "7000",
    val frpsToken: String = "",
    val useFrpsToken: Boolean = false,
    val hasStoredFrpsToken: Boolean = false,
    val frpcUser: String = "",
    val stcpServerUser: String = "",
    val stcpServerName: String = "",
    val stcpSecretKey: String = "",
    val hasStoredStcpSecret: Boolean = false,
    val stcpWireProtocol: StcpWireProtocol = StcpWireProtocol.V1,
    val stcpUseEncryption: Boolean = false,
    val stcpUseCompression: Boolean = false,
    val frpsTlsLeafPin: String = "",
    val stcpConnectTimeoutSeconds: String = "10",
    val discoveredHostKey: DiscoveredHostKey? = null,
    val isDiscovering: Boolean = false,
    val error: String? = null,
    val isSaving: Boolean = false,
)

data class DependencyOption(
    val id: UUID,
    val name: String,
    val disabledByCycle: Boolean,
)

class ConfigEditorViewModel(
    private val repository: ConfigRepository,
    private val credentialStore: CredentialStore,
    private val sshBackend: SshTunnelBackend,
    private val tunnelServiceClient: TunnelServiceClient,
    private val configId: UUID?,
) : ViewModel() {
    private val mutableState = MutableStateFlow(ConfigEditorState())
    val state: StateFlow<ConfigEditorState> = mutableState.asStateFlow()
    private val savedChannel = Channel<Unit>(Channel.BUFFERED)
    val saved = savedChannel.receiveAsFlow()
    private var original: TunnelConfig? = null
    private val draftId = configId ?: UUID.randomUUID()
    private var allConfigs: List<TunnelConfig> = emptyList()
    private var importedPrivateKey: ByteArray? = null

    init {
        viewModelScope.launch {
            original = configId?.let { repository.find(it) }
            val config = original
            mutableState.value = if (config == null) ConfigEditorState(isLoading = false) else config.toEditorState()
            refreshDependencyUi(mutableState.value.dependencyConfigId)
        }
        viewModelScope.launch {
            repository.configs.collectLatest { configs ->
                allConfigs = configs
                refreshDependencyUi(mutableState.value.dependencyConfigId)
            }
        }
    }

    fun setName(value: String) {
        update { copy(name = value, error = null) }
        refreshDependencyUi(mutableState.value.dependencyConfigId)
    }
    fun setDescription(value: String) = update { copy(description = value, error = null) }
    fun setLocalPort(value: String) = numeric(value, 5) { copy(localPort = it, error = null) }
    fun setKind(value: TunnelKind) = update { copy(kind = value, error = null) }
    fun setAutoRecover(value: Boolean) = update { copy(autoRecover = value) }
    fun setDependency(configId: UUID?) {
        refreshDependencyUi(configId)
        update { copy(error = null) }
    }
    fun setSshHost(value: String) = update {
        val changed = value.trim() != sshHost.trim()
        copy(
            sshHost = value,
            logicalHost = if (logicalHost.isBlank() || logicalHost == sshHost) value else logicalHost,
            hasStoredFingerprint = if (changed) false else hasStoredFingerprint,
            hostFingerprint = if (changed) "" else hostFingerprint,
            error = null,
        )
    }
    fun setSshPort(value: String) = numeric(value, 5) {
        val changed = it != sshPort
        copy(
            sshPort = it,
            logicalPort = if (logicalPort == sshPort) it else logicalPort,
            hasStoredFingerprint = if (changed) false else hasStoredFingerprint,
            hostFingerprint = if (changed) "" else hostFingerprint,
            error = null,
        )
    }
    fun setLogicalHost(value: String) = update { copy(logicalHost = value, error = null) }
    fun setLogicalPort(value: String) = numeric(value, 5) { copy(logicalPort = it, error = null) }
    fun setUsername(value: String) = update { copy(username = value, error = null) }
    fun setAuthMethod(value: SshAuthMethod) = update { copy(authMethod = value, error = null) }
    fun setPassword(value: String) = update { copy(password = value, error = null) }
    fun setPassphrase(value: String) = update { copy(passphrase = value, error = null) }
    fun setHostKeyPolicy(value: SshHostKeyPolicy) = update {
        copy(hostKeyPolicy = value, hostFingerprint = "", error = null)
    }
    fun setHostFingerprint(value: String) = update { copy(hostFingerprint = value.trim(), error = null) }
    fun setRemoteTargetHost(value: String) = update { copy(remoteTargetHost = value, error = null) }
    fun setRemoteTargetPort(value: String) = numeric(value, 5) { copy(remoteTargetPort = it, error = null) }
    fun setConnectTimeout(value: String) = numeric(value, 3) { copy(connectTimeoutSeconds = it, error = null) }
    fun setKeepAlive(value: String) = numeric(value, 3) { copy(keepAliveSeconds = it, error = null) }
    fun setFrpsHost(value: String) = update {
        copy(
            frpsHost = value,
            frpsLogicalHost = if (frpsLogicalHost.isBlank() || frpsLogicalHost == frpsHost) value else frpsLogicalHost,
            error = null,
        )
    }
    fun setFrpsPort(value: String) = numeric(value, 5) {
        copy(
            frpsPort = it,
            frpsLogicalPort = if (frpsLogicalPort == frpsPort) it else frpsLogicalPort,
            error = null,
        )
    }
    fun setFrpsLogicalHost(value: String) = update { copy(frpsLogicalHost = value, error = null) }
    fun setFrpsLogicalPort(value: String) = numeric(value, 5) { copy(frpsLogicalPort = it, error = null) }
    fun setFrpsToken(value: String) = update { copy(frpsToken = value, error = null) }
    fun setUseFrpsToken(value: Boolean) = update {
        copy(useFrpsToken = value, frpsToken = if (value) frpsToken else "", error = null)
    }
    fun setFrpcUser(value: String) = update { copy(frpcUser = value, error = null) }
    fun setStcpServerUser(value: String) = update { copy(stcpServerUser = value, error = null) }
    fun setStcpServerName(value: String) = update { copy(stcpServerName = value, error = null) }
    fun setStcpSecretKey(value: String) = update { copy(stcpSecretKey = value, error = null) }
    fun setStcpWireProtocol(value: StcpWireProtocol) = update { copy(stcpWireProtocol = value, error = null) }
    fun setStcpUseEncryption(value: Boolean) = update { copy(stcpUseEncryption = value) }
    fun setStcpUseCompression(value: Boolean) = update { copy(stcpUseCompression = value) }
    fun setFrpsTlsLeafPin(value: String) = update { copy(frpsTlsLeafPin = value.trim(), error = null) }
    fun setStcpConnectTimeout(value: String) = numeric(value, 3) {
        copy(stcpConnectTimeoutSeconds = it, error = null)
    }

    fun importPrivateKey(uri: Uri, resolver: ContentResolver) {
        viewModelScope.launch {
            runCatching {
                withContext(Dispatchers.IO) {
                    resolver.openInputStream(uri)?.use { input ->
                        val probe = ByteArray(MaxPrivateKeyBytes + 1)
                        var total = 0
                        while (total < probe.size) {
                            val read = input.read(probe, total, probe.size - total)
                            if (read < 0) break
                            if (read == 0) {
                                val single = input.read()
                                if (single < 0) break
                                probe[total++] = single.toByte()
                            } else {
                                total += read
                            }
                        }
                        require(total in 1..MaxPrivateKeyBytes) { "私钥文件必须小于 256 KiB" }
                        probe.copyOf(total).also { probe.fill(0) }
                    } ?: error("无法读取私钥文件")
                }
            }.onSuccess { bytes ->
                importedPrivateKey?.fill(0)
                importedPrivateKey = bytes
                update { copy(hasPrivateKey = true, error = null) }
            }.onFailure { update { copy(error = it.message ?: "无法读取私钥文件") } }
        }
    }

    fun discoverHostKey() {
        val snapshot = mutableState.value
        val timeout = snapshot.connectTimeoutSeconds.toIntOrNull()
        val directPort = snapshot.sshPort.toIntOrNull()
        val dependency = snapshot.dependencyConfigId?.let { id -> allConfigs.firstOrNull { it.id == id } }
        if (snapshot.username.isBlank() || timeout !in 1..120 ||
            snapshot.dependencyConfigId == null && (snapshot.sshHost.isBlank() || directPort !in 1..65535)
        ) {
            update { copy(error = "请先填写有效的 SSH 连接入口、用户名和连接超时") }
            return
        }
        if (snapshot.dependencyConfigId != null && dependency == null) {
            update { copy(error = "选择的依赖配置不存在") }
            return
        }
        viewModelScope.launch {
            update { copy(isDiscovering = true, error = null) }
            val requestId = UUID.randomUUID()
            var dependencyAcquired = false
            try {
                runCatching {
                    val endpoint = if (dependency != null) {
                        check(tunnelServiceClient.acquireHostKeyDiscovery(dependency.id, requestId)) {
                            "dependency_start_failed"
                        }
                        dependencyAcquired = true
                        EndpointSource.Remote("127.0.0.1", dependency.localPort)
                    } else {
                        EndpointSource.Remote(snapshot.sshHost.normalizedHost(), requireNotNull(directPort))
                    }
                    sshBackend.discoverHostKey(snapshot.discoveryConfig(endpoint, requireNotNull(timeout)))
                }.onSuccess { update { copy(isDiscovering = false, discoveredHostKey = it) } }
                    .onFailure { failure ->
                        update {
                            copy(
                                isDiscovering = false,
                                error = if (failure.message == "dependency_start_failed") {
                                    "无法启动依赖配置，请检查上游映射状态"
                                } else if (failure is SshBackendException) {
                                    tunnelErrorPresentation(failure.tunnelError).let { "${it.title}。${it.suggestion}" }
                                } else {
                                    "无法获取主机身份，请检查地址和网络"
                                },
                            )
                        }
                    }
            } finally {
                if (dependencyAcquired) {
                    withContext(NonCancellable) {
                        runCatching {
                            tunnelServiceClient.releaseHostKeyDiscovery(requireNotNull(dependency).id, requestId)
                        }
                    }
                }
            }
        }
    }

    fun acceptDiscoveredHostKey() {
        val discovered = mutableState.value.discoveredHostKey ?: return
        update {
            copy(
                hostFingerprint = discovered.sha256Fingerprint,
                hasStoredFingerprint = false,
                discoveredHostKey = null,
                error = null,
            )
        }
    }

    fun dismissDiscoveredHostKey() = update { copy(discoveredHostKey = null) }

    fun save() {
        val snapshot = mutableState.value
        snapshot.validateForSave(original)?.let { error ->
            update { copy(error = error) }
            return
        }
        viewModelScope.launch {
            update { copy(isSaving = true, error = null) }
            val createdRefs = mutableListOf<CredentialRef>()
            try {
                val now = System.currentTimeMillis()
                val port = requireNotNull(snapshot.localPort.toIntOrNull())
                val base = original ?: TunnelConfig.empty(snapshot.kind, snapshot.name, port, now).copy(id = draftId)
                val oldSsh = original?.spec as? TunnelSpec.Ssh
                val oldStcp = original?.spec as? TunnelSpec.Stcp
                val spec = if (snapshot.kind == TunnelKind.SSH) {
                    snapshot.toSshSpec(oldSsh, createdRefs)
                } else {
                    snapshot.toStcpSpec(oldStcp, createdRefs)
                }
                repository.save(
                    base.copy(
                        name = snapshot.name,
                        description = snapshot.description,
                        localPort = port,
                        spec = spec,
                        autoRecover = snapshot.autoRecover,
                    ),
                )
                deleteReplacedRefs(oldSsh?.credentials, (spec as? TunnelSpec.Ssh)?.credentials)
                deleteReplacedStcpRefs(oldStcp, spec as? TunnelSpec.Stcp)
                importedPrivateKey?.fill(0)
                importedPrivateKey = null
                update {
                    copy(
                        password = "",
                        passphrase = "",
                        hostFingerprint = "",
                        frpsToken = "",
                        stcpSecretKey = "",
                    )
                }
                savedChannel.send(Unit)
            } catch (failure: Exception) {
                createdRefs.forEach { ref -> runCatching { credentialStore.delete(ref) } }
                val message = when (failure) {
                    is dev.portweave.core.model.DependencyGraphException.Cycle -> {
                        val names = failure.path.map { id ->
                            if (id == draftId) snapshot.name else allConfigs.firstOrNull { it.id == id }?.name ?: id
                        }
                        "无法保存：该依赖会形成循环\n${names.joinToString(" → ")}"
                    }
                    is dev.portweave.core.model.DependencyGraphException.Missing ->
                        "无法保存：依赖配置不存在（${failure.dependencyId}）"
                    else -> "保存失败，请检查配置后重试"
                }
                update { copy(isSaving = false, error = message) }
            }
        }
    }

    private suspend fun ConfigEditorState.toSshSpec(
        old: TunnelSpec.Ssh?,
        createdRefs: MutableList<CredentialRef>,
    ): TunnelSpec.Ssh {
        suspend fun candidate(kind: CredentialKind, value: ByteArray): CredentialRef = try {
            credentialStore.put(kind, value).also(createdRefs::add)
        } finally {
            value.fill(0)
        }
        val passwordRef = if (authMethod == SshAuthMethod.PASSWORD) {
            if (password.isNotEmpty()) candidate(CredentialKind.PASSWORD, password.toByteArray())
            else old?.credentials?.password
        } else null
        val privateKeyRef = if (authMethod == SshAuthMethod.PRIVATE_KEY) {
            importedPrivateKey?.copyOf()?.let { candidate(CredentialKind.PRIVATE_KEY, it) }
                ?: old?.credentials?.privateKey
        } else null
        val passphraseRef = if (authMethod == SshAuthMethod.PRIVATE_KEY) {
            if (passphrase.isNotEmpty()) {
                candidate(CredentialKind.PRIVATE_KEY_PASSPHRASE, passphrase.toByteArray())
            }
            else old?.credentials?.passphrase
        } else null
        val fingerprintRef = if (hostFingerprint.isNotEmpty()) {
            candidate(CredentialKind.SSH_HOST_FINGERPRINT, hostFingerprint.toByteArray())
        } else old?.credentials?.hostFingerprint
        return TunnelSpec.Ssh(
            endpoint = dependencyConfigId?.let(EndpointSource::Dependency)
                ?: EndpointSource.Remote(sshHost.normalizedHost(), sshPort.toInt()),
            identity = SshServerIdentity(logicalHost.normalizedHost(), logicalPort.toInt()),
            username = username.trim(),
            authMethod = authMethod,
            credentials = SshCredentialRefs(passwordRef, privateKeyRef, passphraseRef, fingerprintRef),
            remoteTargetHost = remoteTargetHost.normalizedHost(),
            remoteTargetPort = remoteTargetPort.toInt(),
            hostKeyPolicy = hostKeyPolicy,
            connectTimeoutSeconds = connectTimeoutSeconds.toInt(),
            keepAliveSeconds = keepAliveSeconds.toInt(),
        )
    }

    private suspend fun ConfigEditorState.toStcpSpec(
        old: TunnelSpec.Stcp?,
        createdRefs: MutableList<CredentialRef>,
    ): TunnelSpec.Stcp {
        suspend fun candidate(kind: CredentialKind, text: String): CredentialRef {
            val value = text.toByteArray()
            return try {
                credentialStore.put(kind, value).also(createdRefs::add)
            } finally {
                value.fill(0)
            }
        }
        val tokenRef = when {
            !useFrpsToken -> null
            frpsToken.isNotEmpty() -> candidate(CredentialKind.TOKEN, frpsToken)
            else -> old?.tokenRef
        }
        val secretRef = if (stcpSecretKey.isNotEmpty()) {
            candidate(CredentialKind.STCP_SECRET, stcpSecretKey)
        } else {
            old?.secretKeyRef
        }
        return TunnelSpec.Stcp(
            endpoint = dependencyConfigId?.let(EndpointSource::Dependency)
                ?: EndpointSource.Remote(frpsHost.normalizedHost(), frpsPort.toInt()),
            identity = FrpsServerIdentity(
                frpsLogicalHost.normalizedHost(),
                frpsLogicalPort.toInt(),
            ),
            tokenRef = tokenRef,
            user = frpcUser.trim().takeIf(String::isNotEmpty),
            serverUser = stcpServerUser.trim().takeIf(String::isNotEmpty),
            serverName = stcpServerName.trim(),
            secretKeyRef = secretRef,
            wireProtocol = stcpWireProtocol,
            useEncryption = stcpUseEncryption,
            useCompression = stcpUseCompression,
            tlsLeafPinSha256 = frpsTlsLeafPin.lowercase().takeIf(String::isNotEmpty),
            connectTimeoutSeconds = stcpConnectTimeoutSeconds.toInt(),
        )
    }

    private suspend fun deleteReplacedRefs(old: SshCredentialRefs?, current: SshCredentialRefs?) {
        if (old == null) return
        val retained = setOfNotNull(current?.password, current?.privateKey, current?.passphrase, current?.hostFingerprint)
        setOfNotNull(old.password, old.privateKey, old.passphrase, old.hostFingerprint)
            .filterNot { it in retained }
            .forEach { runCatching { credentialStore.delete(it) } }
    }

    private suspend fun deleteReplacedStcpRefs(old: TunnelSpec.Stcp?, current: TunnelSpec.Stcp?) {
        if (old == null) return
        val retained = setOfNotNull(current?.tokenRef, current?.secretKeyRef)
        setOfNotNull(old.tokenRef, old.secretKeyRef)
            .filterNot { it in retained }
            .forEach { runCatching { credentialStore.delete(it) } }
    }

    override fun onCleared() {
        importedPrivateKey?.fill(0)
        importedPrivateKey = null
        super.onCleared()
    }

    private inline fun update(transform: ConfigEditorState.() -> ConfigEditorState) {
        mutableState.value = mutableState.value.transform()
    }

    private fun refreshDependencyUi(selected: UUID?) {
        if (allConfigs.isEmpty()) return
        val graph = DependencyGraph(allConfigs)
        val byId = allConfigs.associateBy(TunnelConfig::id)
        val options = allConfigs.filterNot { it.id == draftId }.map { config ->
            DependencyOption(config.id, config.name, graph.wouldCreateCycle(draftId, config.id))
        }
        val accepted = selected?.takeIf { id -> options.any { it.id == id && !it.disabledByCycle } }
        val preview = accepted?.let { dependencyId ->
            runCatching { graph.resolve(dependencyId).map(TunnelConfig::name) }.getOrDefault(emptyList()) +
                mutableState.value.name.takeIf { it.isNotBlank() }.orEmpty()
        }.orEmpty()
        mutableState.value = mutableState.value.copy(
            dependencyConfigId = accepted,
            dependencyOptions = options,
            dependencyChainPreview = preview.filter(String::isNotBlank),
        )
    }

    private inline fun numeric(
        value: String,
        maxLength: Int,
        transform: ConfigEditorState.(String) -> ConfigEditorState,
    ) {
        if (value.length <= maxLength && value.all(Char::isDigit)) update { transform(value) }
    }

    companion object {
        private const val MaxPrivateKeyBytes = 256 * 1024

        fun factory(
            repository: ConfigRepository,
            credentialStore: CredentialStore,
            sshBackend: SshTunnelBackend,
            tunnelServiceClient: TunnelServiceClient,
            configId: String?,
        ): ViewModelProvider.Factory = object : ViewModelProvider.Factory {
            @Suppress("UNCHECKED_CAST")
            override fun <T : ViewModel> create(modelClass: Class<T>): T = ConfigEditorViewModel(
                repository,
                credentialStore,
                sshBackend,
                tunnelServiceClient,
                configId?.let { runCatching { UUID.fromString(it) }.getOrNull() },
            ) as T
        }
    }
}

private fun TunnelConfig.toEditorState(): ConfigEditorState {
    val ssh = spec as? TunnelSpec.Ssh
    val stcp = spec as? TunnelSpec.Stcp
    val remote = ssh?.endpoint as? EndpointSource.Remote
    val frpsRemote = stcp?.endpoint as? EndpointSource.Remote
    return ConfigEditorState(
        isLoading = false,
        name = name,
        description = description,
        localPort = localPort.toString(),
        kind = kind,
        autoRecover = autoRecover,
        dependencyConfigId = dependencyConfigId,
        sshHost = remote?.host.orEmpty(),
        sshPort = remote?.port?.toString() ?: "22",
        logicalHost = ssh?.identity?.logicalHost.orEmpty(),
        logicalPort = ssh?.identity?.logicalPort?.toString() ?: "22",
        username = ssh?.username.orEmpty(),
        authMethod = ssh?.authMethod ?: SshAuthMethod.PASSWORD,
        hasStoredPassword = ssh?.credentials?.password != null,
        hasStoredPrivateKey = ssh?.credentials?.privateKey != null,
        hostKeyPolicy = ssh?.hostKeyPolicy ?: SshHostKeyPolicy.TOFU,
        hasStoredFingerprint = ssh?.credentials?.hostFingerprint != null,
        remoteTargetHost = ssh?.remoteTargetHost.orEmpty(),
        remoteTargetPort = ssh?.remoteTargetPort?.takeIf { it > 0 }?.toString().orEmpty(),
        connectTimeoutSeconds = ssh?.connectTimeoutSeconds?.toString() ?: "10",
        keepAliveSeconds = ssh?.keepAliveSeconds?.toString() ?: "30",
        frpsHost = frpsRemote?.host.orEmpty(),
        frpsPort = frpsRemote?.port?.toString() ?: "7000",
        frpsLogicalHost = stcp?.identity?.logicalHost.orEmpty(),
        frpsLogicalPort = stcp?.identity?.logicalPort?.toString() ?: "7000",
        hasStoredFrpsToken = stcp?.tokenRef != null,
        useFrpsToken = stcp?.tokenRef != null,
        frpcUser = stcp?.user.orEmpty(),
        stcpServerUser = stcp?.serverUser.orEmpty(),
        stcpServerName = stcp?.serverName.orEmpty(),
        hasStoredStcpSecret = stcp?.secretKeyRef != null,
        stcpWireProtocol = stcp?.wireProtocol ?: StcpWireProtocol.V1,
        stcpUseEncryption = stcp?.useEncryption ?: false,
        stcpUseCompression = stcp?.useCompression ?: false,
        frpsTlsLeafPin = stcp?.tlsLeafPinSha256.orEmpty(),
        stcpConnectTimeoutSeconds = stcp?.connectTimeoutSeconds?.toString() ?: "10",
    )
}

private fun ConfigEditorState.discoveryConfig(endpoint: EndpointSource.Remote, timeout: Int): TunnelConfig = TunnelConfig.empty(
    TunnelKind.SSH,
    "host-key-discovery",
    1024,
).copy(
    spec = TunnelSpec.Ssh(
        endpoint = endpoint,
        identity = SshServerIdentity(
            logicalHost.ifBlank { endpoint.host }.normalizedHost(),
            logicalPort.toIntOrNull() ?: endpoint.port,
        ),
        username = username.trim(),
        connectTimeoutSeconds = timeout,
    ),
)

private fun ConfigEditorState.validateForSave(original: TunnelConfig?): String? {
    val local = localPort.toIntOrNull()
    if (name.trim().length !in 1..64) return "配置名称长度必须为 1 到 64 个字符"
    if (local !in 1024..65535) return "本地端口必须在 1024 到 65535 之间"
    if (kind == TunnelKind.STCP) {
        if (dependencyConfigId == null && (frpsHost.isBlank() || frpsPort.toIntOrNull() !in 1..65535)) {
            return "请输入有效的 frps 地址和端口"
        }
        if (frpsLogicalHost.isBlank() || frpsLogicalPort.toIntOrNull() !in 1..65535) {
            return "请输入有效的 frps 逻辑服务身份"
        }
        if (stcpServerName.isBlank()) return "STCP 服务名不能为空"
        val oldStcp = original?.spec as? TunnelSpec.Stcp
        if (useFrpsToken && frpsToken.isEmpty() && oldStcp?.tokenRef == null) return "启用 frps Token 后必须填写 Token"
        if (stcpSecretKey.isEmpty() && oldStcp?.secretKeyRef == null) return "STCP 密钥不能为空"
        if (stcpConnectTimeoutSeconds.toIntOrNull() !in 1..120) return "连接超时必须在 1 到 120 秒之间"
        if (frpsTlsLeafPin.isNotEmpty() && !TlsPinRegex.matches(frpsTlsLeafPin)) {
            return "TLS 叶证书指纹必须是 64 位 SHA-256 十六进制值"
        }
        return null
    }
    if (dependencyConfigId == null && (sshHost.isBlank() || sshPort.toIntOrNull() !in 1..65535)) {
        return "请输入有效的 SSH 地址和端口"
    }
    if (logicalHost.isBlank() || logicalPort.toIntOrNull() !in 1..65535) return "请输入有效的逻辑主机身份"
    if (username.isBlank()) return "SSH 用户名不能为空"
    if (remoteTargetHost.isBlank() || remoteTargetPort.toIntOrNull() !in 1..65535) {
        return "请输入有效的远端目标地址和端口"
    }
    if (connectTimeoutSeconds.toIntOrNull() !in 1..120) return "连接超时必须在 1 到 120 秒之间"
    if (keepAliveSeconds.toIntOrNull() !in 1..300) return "保活间隔必须在 1 到 300 秒之间"
    val oldSsh = original?.spec as? TunnelSpec.Ssh
    if (authMethod == SshAuthMethod.PASSWORD && password.isEmpty() && oldSsh?.credentials?.password == null) {
        return "密码认证需要填写密码"
    }
    if (authMethod == SshAuthMethod.PRIVATE_KEY && !hasPrivateKey && oldSsh?.credentials?.privateKey == null) {
        return "私钥认证需要导入私钥"
    }
    if (hostFingerprint.isEmpty() && !hasStoredFingerprint) return "请先获取并确认主机指纹"
    if (hostFingerprint.isNotEmpty() && !SshHostKeyFingerprint.isValid(hostFingerprint)) {
        return "主机指纹必须是规范的 SHA256 指纹"
    }
    return null
}

private val TlsPinRegex = Regex("[0-9a-fA-F]{64}")

private fun String.normalizedHost(): String {
    val value = trim()
    return if (value.length >= 2 && value.first() == '[' && value.last() == ']') {
        value.substring(1, value.lastIndex)
    } else {
        value
    }
}
