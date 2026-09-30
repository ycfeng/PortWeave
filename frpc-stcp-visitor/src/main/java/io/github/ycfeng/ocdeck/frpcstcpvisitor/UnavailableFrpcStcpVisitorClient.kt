package io.github.ycfeng.ocdeck.frpcstcpvisitor

class UnavailableFrpcStcpVisitorClient(
    @Suppress("UNUSED_PARAMETER") reason: String = GO_MOBILE_BRIDGE_UNAVAILABLE_MESSAGE,
) : FrpcStcpVisitorClient {
    override suspend fun startSession(config: FrpcSessionConfig): String = unavailable()

    override suspend fun ensureVisitor(
        sessionId: String,
        visitor: FrpcStcpVisitorConfig,
    ): FrpcEnsureVisitorResult = unavailable()

    override suspend fun waitVisitorReady(
        sessionId: String,
        visitorName: String,
        desiredRevision: Long,
        timeoutMillis: Long,
    ): FrpcVisitorReadyResult = unavailable()

    override suspend fun stopVisitor(sessionId: String, visitorName: String) {
        // Nothing to stop when the native binding is unavailable.
    }

    override suspend fun stopSession(sessionId: String, timeoutMillis: Long): FrpcStopSessionResult =
        FrpcStopSessionResult(sessionId = sessionId, phase = "closed")

    override suspend fun getState(sessionId: String): FrpcStcpVisitorState = FrpcStcpVisitorState(
        sessionId = sessionId,
        phase = "unavailable",
        lastError = null,
    )

    private fun unavailable(): Nothing = throw GoMobileBridgeUnavailableException()
}
