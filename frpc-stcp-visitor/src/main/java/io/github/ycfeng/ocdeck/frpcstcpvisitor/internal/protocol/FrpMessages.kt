@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol

import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

internal sealed interface FrpMessage

@Serializable
internal data class ClientSpec(
    val type: String = "",
    @SerialName("always_auth_pass")
    val alwaysAuthPass: Boolean = false,
)

@Serializable
internal data class Login(
    val version: String = "",
    val hostname: String = "",
    val os: String = "",
    val arch: String = "",
    val user: String = "",
    @SerialName("privilege_key")
    val privilegeKey: String = "",
    val timestamp: Long = 0,
    @SerialName("run_id")
    val runId: String = "",
    @SerialName("client_id")
    val clientId: String = "",
    val metas: Map<String, String> = emptyMap(),
    @EncodeDefault(EncodeDefault.Mode.ALWAYS)
    @SerialName("client_spec")
    val clientSpec: ClientSpec = ClientSpec(),
    @SerialName("pool_count")
    val poolCount: Int = 0,
) : FrpMessage {
    override fun toString(): String =
        "Login(versionPresent=${version.isNotEmpty()}, hostnamePresent=${hostname.isNotEmpty()}, " +
            "userPresent=${user.isNotEmpty()}, privilegeKey=<redacted>, timestampPresent=${timestamp != 0L}, " +
            "runIdPresent=${runId.isNotEmpty()}, clientIdPresent=${clientId.isNotEmpty()}, " +
            "metaCount=${metas.size}, poolCount=$poolCount)"
}

@Serializable
internal data class LoginResp(
    val version: String = "",
    @SerialName("run_id")
    val runId: String = "",
    val error: String = "",
) : FrpMessage {
    override fun toString(): String =
        "LoginResp(versionPresent=${version.isNotEmpty()}, runIdPresent=${runId.isNotEmpty()}, " +
            "errorPresent=${error.isNotEmpty()})"
}

@Serializable
internal data class NewWorkConn(
    @SerialName("run_id")
    val runId: String = "",
    @SerialName("privilege_key")
    val privilegeKey: String = "",
    val timestamp: Long = 0,
) : FrpMessage {
    override fun toString(): String =
        "NewWorkConn(runIdPresent=${runId.isNotEmpty()}, privilegeKey=<redacted>, " +
            "timestampPresent=${timestamp != 0L})"
}

@Serializable
internal class ReqWorkConn : FrpMessage {
    override fun equals(other: Any?): Boolean = other is ReqWorkConn

    override fun hashCode(): Int = javaClass.hashCode()

    override fun toString(): String = "ReqWorkConn()"
}

@Serializable
internal data class StartWorkConn(
    @SerialName("proxy_name")
    val proxyName: String = "",
    @SerialName("src_addr")
    val srcAddr: String = "",
    @SerialName("dst_addr")
    val dstAddr: String = "",
    @SerialName("src_port")
    val srcPort: Int = 0,
    @SerialName("dst_port")
    val dstPort: Int = 0,
    val error: String = "",
) : FrpMessage {
    override fun toString(): String =
        "StartWorkConn(proxyNamePresent=${proxyName.isNotEmpty()}, srcAddrPresent=${srcAddr.isNotEmpty()}, " +
            "dstAddrPresent=${dstAddr.isNotEmpty()}, srcPortPresent=${srcPort != 0}, " +
            "dstPortPresent=${dstPort != 0}, " +
            "errorPresent=${error.isNotEmpty()})"
}

@Serializable
internal data class NewVisitorConn(
    @SerialName("run_id")
    val runId: String = "",
    @SerialName("proxy_name")
    val proxyName: String = "",
    @SerialName("sign_key")
    val signKey: String = "",
    val timestamp: Long = 0,
    @SerialName("use_encryption")
    val useEncryption: Boolean = false,
    @SerialName("use_compression")
    val useCompression: Boolean = false,
) : FrpMessage {
    override fun toString(): String =
        "NewVisitorConn(runIdPresent=${runId.isNotEmpty()}, proxyNamePresent=${proxyName.isNotEmpty()}, " +
            "signKey=<redacted>, timestampPresent=${timestamp != 0L}, " +
            "useEncryption=$useEncryption, useCompression=$useCompression)"
}

@Serializable
internal data class NewVisitorConnResp(
    @SerialName("proxy_name")
    val proxyName: String = "",
    val error: String = "",
) : FrpMessage {
    override fun toString(): String =
        "NewVisitorConnResp(proxyNamePresent=${proxyName.isNotEmpty()}, errorPresent=${error.isNotEmpty()})"
}

@Serializable
internal data class Ping(
    @SerialName("privilege_key")
    val privilegeKey: String = "",
    val timestamp: Long = 0,
) : FrpMessage {
    override fun toString(): String =
        "Ping(privilegeKey=<redacted>, timestampPresent=${timestamp != 0L})"
}

@Serializable
internal data class Pong(
    val error: String = "",
) : FrpMessage {
    override fun toString(): String = "Pong(errorPresent=${error.isNotEmpty()})"
}

internal object FrpMessageTypes {
    const val V1_LOGIN = 'o'.code
    const val V1_LOGIN_RESP = '1'.code
    const val V1_NEW_WORK_CONN = 'w'.code
    const val V1_REQ_WORK_CONN = 'r'.code
    const val V1_START_WORK_CONN = 's'.code
    const val V1_NEW_VISITOR_CONN = 'v'.code
    const val V1_NEW_VISITOR_CONN_RESP = '3'.code
    const val V1_PING = 'h'.code
    const val V1_PONG = '4'.code

    const val V2_LOGIN = 1
    const val V2_LOGIN_RESP = 2
    const val V2_NEW_WORK_CONN = 6
    const val V2_REQ_WORK_CONN = 7
    const val V2_START_WORK_CONN = 8
    const val V2_NEW_VISITOR_CONN = 9
    const val V2_NEW_VISITOR_CONN_RESP = 10
    const val V2_PING = 11
    const val V2_PONG = 12

    fun v1TypeOf(message: FrpMessage): Int = when (message) {
        is Login -> V1_LOGIN
        is LoginResp -> V1_LOGIN_RESP
        is NewWorkConn -> V1_NEW_WORK_CONN
        is ReqWorkConn -> V1_REQ_WORK_CONN
        is StartWorkConn -> V1_START_WORK_CONN
        is NewVisitorConn -> V1_NEW_VISITOR_CONN
        is NewVisitorConnResp -> V1_NEW_VISITOR_CONN_RESP
        is Ping -> V1_PING
        is Pong -> V1_PONG
    }

    fun v2TypeOf(message: FrpMessage): Int = when (message) {
        is Login -> V2_LOGIN
        is LoginResp -> V2_LOGIN_RESP
        is NewWorkConn -> V2_NEW_WORK_CONN
        is ReqWorkConn -> V2_REQ_WORK_CONN
        is StartWorkConn -> V2_START_WORK_CONN
        is NewVisitorConn -> V2_NEW_VISITOR_CONN
        is NewVisitorConnResp -> V2_NEW_VISITOR_CONN_RESP
        is Ping -> V2_PING
        is Pong -> V2_PONG
    }

    fun isKnownV1(type: Int): Boolean = type == V1_LOGIN ||
        type == V1_LOGIN_RESP ||
        type == V1_NEW_WORK_CONN ||
        type == V1_REQ_WORK_CONN ||
        type == V1_START_WORK_CONN ||
        type == V1_NEW_VISITOR_CONN ||
        type == V1_NEW_VISITOR_CONN_RESP ||
        type == V1_PING ||
        type == V1_PONG

    fun isKnownV2(type: Int): Boolean = type == V2_LOGIN ||
        type == V2_LOGIN_RESP ||
        type == V2_NEW_WORK_CONN ||
        type == V2_REQ_WORK_CONN ||
        type == V2_START_WORK_CONN ||
        type == V2_NEW_VISITOR_CONN ||
        type == V2_NEW_VISITOR_CONN_RESP ||
        type == V2_PING ||
        type == V2_PONG
}
