package io.github.ycfeng.ocdeck.frpcstcpvisitor

import java.io.IOException

enum class KotlinFrpcStcpVisitorFailure(val description: String) {
    INVALID_CONFIGURATION("invalid configuration"),
    UNSUPPORTED_CONFIGURATION("unsupported configuration"),
    CLIENT_CLOSED("client is closed"),
    SESSION_LIMIT("session limit exceeded"),
    SESSION_NOT_FOUND("session is not found"),
    SESSION_CLOSED("session is closed"),
    MAILBOX_FULL("session command limit exceeded"),
    VISITOR_LIMIT("visitor limit exceeded"),
    BIND_PORT_CONFLICT("local bind port is already configured"),
    LISTENER_BIND_FAILED("local listener bind failed"),
    CONTROL_FAILED("frp control failed"),
    VISITOR_FAILED("visitor failed"),
    VISITOR_NOT_FOUND("visitor is not found"),
    VISITOR_SUPERSEDED("visitor revision was superseded"),
    WAIT_TIMEOUT("visitor readiness timed out"),
    RELAY_LIMIT("relay limit exceeded"),
    VISITOR_HANDSHAKE_FAILED("visitor handshake failed"),
    VISITOR_HANDSHAKE_TIMEOUT("visitor handshake timed out"),
    RELAY_FAILED("visitor relay failed"),
    STOP_TIMEOUT("session stop timed out"),
}

class KotlinFrpcStcpVisitorException(
    val failure: KotlinFrpcStcpVisitorFailure,
    cause: Throwable? = null,
) : IOException(
    "Kotlin frpc STCP visitor failure: ${failure.description}",
    cause,
)
