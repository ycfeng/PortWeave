package dev.portweave.core.data.db

import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.FrpsServerIdentity
import dev.portweave.core.model.StcpWireProtocol
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.core.security.CredentialRef
import java.util.UUID
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test

class StcpEntityMappingTest {
    @Test
    fun stcpDetailRoundTripsWithoutPersistingSecrets() {
        val tokenRef = CredentialRef(UUID.randomUUID())
        val secretRef = CredentialRef(UUID.randomUUID())
        val config = TunnelConfig.empty(TunnelKind.STCP, "STCP", 15432, 1).copy(
            spec = TunnelSpec.Stcp(
                endpoint = EndpointSource.Remote("dial.example", 7000),
                identity = FrpsServerIdentity("identity.example", 7443),
                tokenRef = tokenRef,
                user = "visitor-user",
                serverUser = "provider-user",
                serverName = "database",
                secretKeyRef = secretRef,
                wireProtocol = StcpWireProtocol.V2,
                useEncryption = true,
                useCompression = true,
                tlsLeafPinSha256 = "ab".repeat(32),
                connectTimeoutSeconds = 30,
            ),
        )

        val detail = requireNotNull(config.toStcpDetailEntity())
        val restored = TunnelConfigRecord(config.toEntity(), ssh = null, stcp = detail).toDomain()

        assertEquals(config, restored)
        assertEquals(tokenRef.value.toString(), detail.tokenRef)
        assertEquals(secretRef.value.toString(), detail.secretKeyRef)
        assertFalse(detail.toString().contains("visitor-user"))
        assertFalse(detail.toString().contains("database"))
    }
}
