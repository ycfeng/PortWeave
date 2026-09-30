package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol

import java.io.IOException

internal abstract class FrpSafeIOException(message: String, cause: Throwable? = null) : IOException(message, cause) {
    final override fun toString(): String =
        "${this::class.java.simpleName}(message=${message ?: "unavailable"})"
}

internal enum class FrpProtocolFailure(val description: String) {
    INVALID_MAGIC("invalid magic"),
    UNKNOWN_TYPE("unknown type"),
    UNEXPECTED_TYPE("unexpected type"),
    INVALID_FLAGS("invalid flags"),
    NEGATIVE_LENGTH("negative length"),
    LENGTH_LIMIT("length limit exceeded"),
    TRUNCATED_MAGIC("truncated magic"),
    TRUNCATED_HEADER("truncated header"),
    TRUNCATED_BODY("truncated body"),
    INVALID_JSON("invalid json"),
    INVALID_FIELD("invalid field"),
    INVALID_HELLO("invalid hello"),
    UNSUPPORTED("unsupported value"),
    IO_FAILURE("stream io failure"),
    WRITE_FAILED("stream write failure"),
}

internal class FrpProtocolException(
    val failure: FrpProtocolFailure,
) : FrpSafeIOException("frp protocol failure: ${failure.description}")
