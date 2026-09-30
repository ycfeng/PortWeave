package dev.portweave.app

import android.app.Application
import dev.portweave.core.data.repository.ConfigRepository
import dev.portweave.core.data.db.PortWeaveDatabase
import dev.portweave.core.data.repository.RoomConfigRepository
import dev.portweave.core.data.transfer.ConfigTransferService
import dev.portweave.core.diagnostics.EventRepository
import dev.portweave.core.diagnostics.RoomEventRepository
import dev.portweave.core.data.settings.DataStoreSettingsStore
import dev.portweave.core.data.settings.DataStoreDesiredRuntimeStore
import dev.portweave.core.data.settings.DesiredRuntimeStore
import dev.portweave.core.data.settings.SettingsStore
import dev.portweave.core.security.AndroidKeystoreCredentialStore
import dev.portweave.core.security.CredentialStore
import dev.portweave.app.service.RuntimeStateStore
import dev.portweave.app.service.TunnelServiceClient
import dev.portweave.tunnel.engine.ssh.SshTunnelBackend
import dev.portweave.tunnel.engine.stcp.FrpcStcpBackend
import io.github.ycfeng.ocdeck.frpcstcpvisitor.KotlinFrpcStcpVisitorClient
import dev.portweave.app.diagnostics.DiagnosticBundleExporter

class AppContainer(application: Application) {
    private val database = PortWeaveDatabase.create(application)
    val configRepository: ConfigRepository = RoomConfigRepository(database.tunnelConfigDao())
    val eventRepository: EventRepository = RoomEventRepository(database.tunnelEventDao())
    val credentialStore: CredentialStore = AndroidKeystoreCredentialStore(application)
    val configTransferService = ConfigTransferService(configRepository, credentialStore)
    val diagnosticBundleExporter = DiagnosticBundleExporter(
        application,
        configRepository,
        eventRepository,
        configTransferService,
        workCounters = { runtimeStateStore.workCounters.snapshot() },
    )
    val settingsStore: SettingsStore = DataStoreSettingsStore(application)
    val desiredRuntimeStore: DesiredRuntimeStore = DataStoreDesiredRuntimeStore(application)
    val runtimeStateStore = RuntimeStateStore()
    val workPolicy = dev.portweave.tunnel.engine.RuntimeWorkPolicy(runtimeStateStore.workCounters)
    val sshTunnelBackend = SshTunnelBackend(workPolicy)
    val stcpBackend = FrpcStcpBackend(
        clientFactory = { KotlinFrpcStcpVisitorClient(application) },
        workPolicy = workPolicy,
    )
    val tunnelServiceClient = TunnelServiceClient(application)
}
