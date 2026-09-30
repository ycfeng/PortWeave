package io.github.ycfeng.ocdeck.frpcstcpvisitor

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Modifier
import java.net.BindException
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

class GoMobileFrpcStcpVisitorClient private constructor(
    private val json: Json,
    private val bridge: GoMobileFrpcStcpVisitorBridge,
) : FrpcStcpVisitorClient, FrpcStcpVisitorRuntimeDiagnostics {
    constructor(json: Json) : this(json, ReflectiveGoMobileFrpcStcpVisitorBridge())

    internal constructor(
        json: Json,
        bridge: GoMobileFrpcStcpVisitorBridge,
        @Suppress("UNUSED_PARAMETER") testOnly: Unit,
    ) : this(json, bridge)

    override suspend fun startSession(config: FrpcSessionConfig): String = withContext(Dispatchers.IO) {
        bridge.startSession(json.encodeToString(config))
    }

    override suspend fun ensureVisitor(
        sessionId: String,
        visitor: FrpcStcpVisitorConfig,
    ): FrpcEnsureVisitorResult {
        val resultJson = invokeBindBridge {
            bridge.ensureVisitor(sessionId, json.encodeToString(visitor))
        }
        return decodeBridgeResult(resultJson)
    }

    override suspend fun waitVisitorReady(
        sessionId: String,
        visitorName: String,
        desiredRevision: Long,
        timeoutMillis: Long,
    ): FrpcVisitorReadyResult {
        val resultJson = invokeBindBridge {
            bridge.waitVisitorReady(sessionId, visitorName, desiredRevision, timeoutMillis)
        }
        return decodeBridgeResult(resultJson)
    }

    override suspend fun stopVisitor(sessionId: String, visitorName: String) = withContext(Dispatchers.IO) {
        bridge.stopVisitor(sessionId, visitorName)
    }

    override suspend fun stopSession(
        sessionId: String,
        timeoutMillis: Long,
    ): FrpcStopSessionResult = withContext(Dispatchers.IO) {
        decodeBridgeResult(bridge.stopSession(sessionId, timeoutMillis))
    }

    override suspend fun getState(sessionId: String): FrpcStcpVisitorState = withContext(Dispatchers.IO) {
        decodeBridgeResult(bridge.getState(sessionId))
    }

    override suspend fun captureRuntimeDiagnostics(): FrpcRuntimeDiagnostics = withContext(Dispatchers.IO) {
        decodeBridgeResult(bridge.getRuntimeDiagnostics())
    }

    private suspend fun invokeBindBridge(block: () -> String): String =
        try {
            withContext(Dispatchers.IO) { block() }
        } catch (exception: Exception) {
            exception.bridgeJvmErrorCauseOrNull()?.let { throw it }
            if (exception is CancellationException) throw exception
            currentCoroutineContext().ensureActive()
            exception.bridgeCancellationCauseOrNull()?.let { throw it }
            if (exception.hasLegacyBindConflictMessage()) throw BindException()
            throw exception
        }

    private inline fun <reified T> decodeBridgeResult(resultJson: String): T =
        try {
            json.decodeFromString(resultJson)
        } catch (_: IllegalArgumentException) {
            throw GoMobileBridgeApiMismatchException()
        }
}

private inline fun <reified T : Throwable> Throwable.bridgeCauseOrNull(): T? {
    val seen = Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())
    var current: Throwable? = this
    while (current != null && seen.add(current)) {
        if (current is T) return current
        current = current.cause
    }
    return null
}

private fun Throwable.bridgeJvmErrorCauseOrNull(): Error? = bridgeCauseOrNull()

private fun Throwable.bridgeCancellationCauseOrNull(): CancellationException? = bridgeCauseOrNull()

private fun Exception.hasLegacyBindConflictMessage(): Boolean {
    val detail = message ?: return false
    return detail.contains("address already in use", ignoreCase = true) ||
        detail.contains("eaddrinuse", ignoreCase = true)
}

internal interface GoMobileFrpcStcpVisitorBridge {
    fun startSession(configJson: String): String

    fun ensureVisitor(sessionId: String, visitorJson: String): String

    fun waitVisitorReady(
        sessionId: String,
        visitorName: String,
        desiredRevision: Long,
        timeoutMillis: Long,
    ): String

    fun stopVisitor(sessionId: String, visitorName: String)

    fun stopSession(sessionId: String, timeoutMillis: Long): String

    fun getState(sessionId: String): String

    fun getRuntimeDiagnostics(): String
}

internal class ReflectiveGoMobileFrpcStcpVisitorBridge(
    private val className: String = GO_MOBILE_CLASS_NAME,
) : GoMobileFrpcStcpVisitorBridge {
    private val bridgeClass: Class<*> by lazy(LazyThreadSafetyMode.PUBLICATION) {
        try {
            Class.forName(className)
        } catch (_: ClassNotFoundException) {
            throw aarMissingError()
        } catch (_: SecurityException) {
            throw apiMismatchError()
        }
    }

    override fun startSession(configJson: String): String =
        invokeString(listOf("startSession", "StartSession"), configJson)

    override fun ensureVisitor(sessionId: String, visitorJson: String): String =
        invokeString(listOf("ensureVisitor", "EnsureVisitor"), sessionId, visitorJson)

    override fun waitVisitorReady(
        sessionId: String,
        visitorName: String,
        desiredRevision: Long,
        timeoutMillis: Long,
    ): String = invokeString(
        listOf("waitVisitorReady", "WaitVisitorReady"),
        sessionId,
        visitorName,
        desiredRevision,
        timeoutMillis,
    )

    override fun stopVisitor(sessionId: String, visitorName: String) {
        invokeUnit(listOf("stopVisitor", "StopVisitor"), sessionId, visitorName)
    }

    override fun stopSession(sessionId: String, timeoutMillis: Long): String =
        invokeString(listOf("stopSession", "StopSession"), sessionId, timeoutMillis)

    override fun getState(sessionId: String): String =
        invokeString(listOf("getState", "GetState"), sessionId)

    override fun getRuntimeDiagnostics(): String =
        invokeString(listOf("getRuntimeDiagnostics", "GetRuntimeDiagnostics"))

    private fun invokeString(names: List<String>, vararg args: Any): String =
        invoke(names, String::class.java, *args) as? String
            ?: throw apiMismatchError()

    private fun invokeUnit(names: List<String>, vararg args: Any) {
        invoke(names, Void.TYPE, *args)
    }

    private fun invoke(names: List<String>, expectedReturnType: Class<*>, vararg args: Any): Any? {
        val parameterTypes = args.map(::parameterType).toTypedArray()
        val method = findMethod(names, parameterTypes)
        if (!Modifier.isStatic(method.modifiers) || method.returnType != expectedReturnType) {
            throw apiMismatchError()
        }

        return try {
            method.invoke(null, *args)
        } catch (error: InvocationTargetException) {
            throw (error.targetException ?: apiMismatchError())
        } catch (_: IllegalAccessException) {
            throw apiMismatchError()
        } catch (_: IllegalArgumentException) {
            throw apiMismatchError()
        } catch (_: SecurityException) {
            throw apiMismatchError()
        }
    }

    private fun findMethod(names: List<String>, parameterTypes: Array<Class<*>>): Method {
        names.forEach { name ->
            try {
                return bridgeClass.getMethod(name, *parameterTypes)
            } catch (_: NoSuchMethodException) {
                // Try the alternate GoMobile capitalization.
            } catch (_: SecurityException) {
                throw apiMismatchError()
            }
        }
        throw apiMismatchError()
    }

    private fun parameterType(value: Any): Class<*> = when (value) {
        is String -> String::class.java
        is Long -> Long::class.javaPrimitiveType!!
        else -> throw apiMismatchError()
    }

    private fun aarMissingError(): GoMobileBridgeUnavailableException = GoMobileBridgeUnavailableException()

    private fun apiMismatchError(): GoMobileBridgeApiMismatchException = GoMobileBridgeApiMismatchException()

    private companion object {
        const val GO_MOBILE_CLASS_NAME = "io.github.ycfeng.ocdeck.frpcstcpvisitor.gobridge.frpcstcpvisitor.Frpcstcpvisitor"
    }
}
