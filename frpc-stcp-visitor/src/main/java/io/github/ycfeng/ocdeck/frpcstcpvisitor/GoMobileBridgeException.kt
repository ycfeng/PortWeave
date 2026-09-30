package io.github.ycfeng.ocdeck.frpcstcpvisitor

internal const val GO_MOBILE_BRIDGE_UNAVAILABLE_MESSAGE = "GoMobile bridge is unavailable."
internal const val GO_MOBILE_BRIDGE_API_MISMATCH_MESSAGE = "GoMobile bridge API is incompatible."

/** The generated GoMobile bridge class is not available at runtime. */
class GoMobileBridgeUnavailableException : IllegalStateException(GO_MOBILE_BRIDGE_UNAVAILABLE_MESSAGE)

/** The generated GoMobile bridge does not match the Kotlin bridge contract. */
class GoMobileBridgeApiMismatchException : IllegalStateException(GO_MOBILE_BRIDGE_API_MISMATCH_MESSAGE)
