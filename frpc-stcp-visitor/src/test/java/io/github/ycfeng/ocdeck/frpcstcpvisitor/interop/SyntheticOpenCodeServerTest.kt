package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.util.concurrent.atomic.AtomicBoolean
import org.junit.Test

class SyntheticOpenCodeServerTest {
    @Test
    fun checkpointPublishedAfterInitialEventFlushIsNotLost() {
        val published = AtomicBoolean(false)
        lateinit var server: SyntheticOpenCodeServer
        server = SyntheticOpenCodeServer(
            afterInitialSseEventFlushed = {
                if (published.compareAndSet(false, true)) server.publishSseCheckpoint()
            },
        )
        try {
            TunnelHttpProbe.openSseSession(server.port, "/global/event", "{\"scope\":\"global\"}").use { session ->
                session.awaitCheckpoint(expectedCheckpoint = 1, timeoutMillis = 5_000L)
            }
            server.requireHealthy()
        } finally {
            server.close()
        }
    }

    @Test
    fun concurrentHealthParticipatesInTheTrafficGate() {
        val server = SyntheticOpenCodeServer()
        val gate = server.beginConcurrentTraffic(expectedRequests = 1)
        try {
            TunnelHttpProbe.health(server.port, concurrentTraffic = true)
            gate.requireSatisfied()
            server.requireHealthy()
        } finally {
            server.endConcurrentTraffic(gate)
            server.close()
        }
    }

    @Test
    fun appFixtureSupportsSnapshotQueriesAndGenerationAdvance() {
        val server = SyntheticOpenCodeServer(appFixtureEnabled = true)
        try {
            check(
                TunnelHttpProbe.json(server.port, "/session?directory=%2Fworkspace&roots=true&limit=70") ==
                    """[{"id":"session-snapshot","directory":"/workspace","title":"snapshot-1","time":{"updated":100}}]""",
            )
            check(
                TunnelHttpProbe.json(
                    server.port,
                    "/session/session-snapshot/message?directory=%2Fworkspace&limit=200",
                ) == "[]",
            )
            check(server.advanceAppFixture() == 2)
            check(
                TunnelHttpProbe.json(server.port, "/session?directory=%2Fworkspace&roots=true&limit=70") ==
                    """[{"id":"session-snapshot","directory":"/workspace","title":"snapshot-2","time":{"updated":200}}]""",
            )
            TunnelHttpProbe.openSseSession(
                server.port,
                "/event?directory=%2Fworkspace",
                """{"type":"session.updated","properties":{"info":{"id":"session-project-sse","title":"project-sse-2","directory":"/workspace","time":{"updated":"201"}}}}""",
            ).use { }
            try {
                TunnelHttpProbe.json(server.port, "/session?directory=%2Fwrong&roots=true&limit=70")
                error("App fixture accepted an invalid directory query")
            } catch (_: TunnelProbeException) {
                // Expected: the fixture verifies the real App request contract.
            }
            server.requireHealthy()
        } finally {
            server.close()
        }
    }
}
