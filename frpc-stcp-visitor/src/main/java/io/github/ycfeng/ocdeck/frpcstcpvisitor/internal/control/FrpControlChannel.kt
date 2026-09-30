package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpAeadRecordStreams
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpAeadRole
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpV1Cfb
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpV2Crypto
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.BootstrapInfo
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpMessage
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV1
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV2
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.Login
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.LoginResp
import java.io.InputStream
import java.io.OutputStream
import java.security.SecureRandom
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean

internal class FrpMessageChannel(
    val protocol: FrpWireProtocol,
    private val input: InputStream,
    private val output: OutputStream,
    private val closeOwner: AutoCloseable? = null,
) : AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val writeLock = Any()

    fun readMessage(): FrpMessage? {
        if (closed.get()) throw FrpControlException(FrpControlFailure.CLOSED)
        return when (protocol) {
            FrpWireProtocol.V1 -> FrpWireV1.readMessage(input)
            FrpWireProtocol.V2 -> FrpWireV2.readMessage(input)
        }
    }

    fun writeMessage(message: FrpMessage) {
        synchronized(writeLock) {
            if (closed.get()) throw FrpControlException(FrpControlFailure.CLOSED)
            val encoded = when (protocol) {
                FrpWireProtocol.V1 -> FrpWireV1.encodeMessage(message)
                FrpWireProtocol.V2 -> FrpWireV2.encodeMessage(message)
            }
            try {
                output.write(encoded)
            } finally {
                encoded.fill(0)
            }
            output.flush()
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        synchronized(writeLock) {
            var fatalFailure: Throwable? = closeFatal(closeOwner, null)
            fatalFailure = closeFatal(output, fatalFailure)
            fatalFailure = closeFatal(input, fatalFailure)
            fatalFailure?.let { throw it }
        }
    }

    override fun toString(): String = "FrpMessageChannel(protocol=$protocol, closed=${closed.get()})"

    private fun closeFatal(closeable: AutoCloseable?, current: Throwable?): Throwable? {
        if (closeable == null) return current
        return try {
            closeable.close()
            current
        } catch (failure: CancellationException) {
            preferFatal(current, failure)
        } catch (failure: Error) {
            preferFatal(current, failure)
        } catch (_: Exception) {
            current
        }
    }

    private fun preferFatal(current: Throwable?, candidate: Throwable): Throwable {
        if (current == null || current === candidate) return candidate
        val selected = when {
            candidate is Error && current !is Error -> candidate
            current is Error -> current
            candidate is CancellationException && current !is CancellationException -> candidate
            else -> current
        }
        val secondary = if (selected === current) candidate else current
        if (selected !== secondary) selected.addSuppressed(secondary)
        return selected
    }
}

internal data class FrpLoggedInControl(
    val runId: String,
    val channel: FrpMessageChannel,
) {
    override fun toString(): String =
        "FrpLoggedInControl(runIdPresent=${runId.isNotEmpty()}, channel=$channel)"
}

internal class FrpControlBootstrap(
    private val config: FrpControlConfig,
    private val secureRandom: SecureRandom = SecureRandom(),
) {
    fun exchange(
        input: InputStream,
        output: OutputStream,
        login: Login,
        closeOwner: AutoCloseable? = null,
    ): FrpLoggedInControl = when (config.wireProtocol) {
        FrpWireProtocol.V1 -> exchangeV1(input, output, login, closeOwner)
        FrpWireProtocol.V2 -> exchangeV2(input, output, login, closeOwner)
    }

    private fun exchangeV1(
        input: InputStream,
        output: OutputStream,
        login: Login,
        closeOwner: AutoCloseable?,
    ): FrpLoggedInControl {
        FrpWireV1.writeMessage(output, login)
        output.flush()
        val response = FrpWireV1.readMessage(input) as? LoginResp
            ?: throw FrpControlException(FrpControlFailure.LOGIN_PROTOCOL_FAILED)
        validateLoginResponse(response)
        val encryptedInput = FrpV1Cfb.decrypting(input, config.token)
        val encryptedOutput = try {
            FrpV1Cfb.encrypting(output, config.token, secureRandom)
        } catch (failure: CancellationException) {
            closeAfterConstructionFailure(encryptedInput, failure)
        } catch (failure: Error) {
            closeAfterConstructionFailure(encryptedInput, failure)
        } catch (failure: Exception) {
            closeAfterConstructionFailure(encryptedInput, failure)
        }
        return FrpLoggedInControl(
            runId = response.runId,
            channel = FrpMessageChannel(
                protocol = FrpWireProtocol.V1,
                input = encryptedInput,
                output = encryptedOutput,
                closeOwner = closeOwner,
            ),
        )
    }

    private fun exchangeV2(
        input: InputStream,
        output: OutputStream,
        login: Login,
        closeOwner: AutoCloseable?,
    ): FrpLoggedInControl {
        FrpWireV2.writeMagic(output)
        val clientHello = FrpWireV2.createClientHello(
            bootstrap = BootstrapInfo(transport = "tcp", tls = config.tlsEnabled, tcpMux = true),
            algorithms = config.preferredAeadAlgorithms,
            secureRandom = secureRandom,
        )
        val rawClientHello = FrpWireV2.writeClientHello(output, clientHello)
        FrpWireV2.writeMessage(output, login)
        output.flush()

        val rawServerHello = FrpWireV2.readServerHello(input)
        if (rawServerHello.value.error.isNotEmpty()) {
            throw FrpControlException(FrpControlFailure.LOGIN_REJECTED)
        }
        val cryptoContext = FrpV2Crypto.context(rawClientHello, rawServerHello)
        val response = FrpWireV2.readMessage(input) as? LoginResp
            ?: throw FrpControlException(FrpControlFailure.LOGIN_PROTOCOL_FAILED)
        validateLoginResponse(response)

        val keys = FrpV2Crypto.deriveControlKeys(config.token, cryptoContext)
        try {
            val readKey = keys.readKey(FrpAeadRole.CLIENT)
            val encryptedInput = try {
                FrpAeadRecordStreams.decrypting(input, readKey, cryptoContext.algorithm)
            } finally {
                readKey.fill(0)
            }
            val writeKey = keys.writeKey(FrpAeadRole.CLIENT)
            val encryptedOutput = try {
                FrpAeadRecordStreams.encrypting(output, writeKey, cryptoContext.algorithm, secureRandom)
            } catch (failure: CancellationException) {
                closeAfterConstructionFailure(encryptedInput, failure)
            } catch (failure: Error) {
                closeAfterConstructionFailure(encryptedInput, failure)
            } catch (failure: Exception) {
                closeAfterConstructionFailure(encryptedInput, failure)
            } finally {
                writeKey.fill(0)
            }
            return FrpLoggedInControl(
                runId = response.runId,
                channel = FrpMessageChannel(
                    protocol = FrpWireProtocol.V2,
                    input = encryptedInput,
                    output = encryptedOutput,
                    closeOwner = closeOwner,
                ),
            )
        } finally {
            keys.destroy()
        }
    }

    override fun toString(): String = "FrpControlBootstrap(protocol=${config.wireProtocol})"

    private fun validateLoginResponse(response: LoginResp) {
        if (response.error.isNotEmpty()) {
            throw FrpControlException(FrpControlFailure.LOGIN_REJECTED)
        }
        if (response.runId.isBlank()) {
            throw FrpControlException(FrpControlFailure.LOGIN_PROTOCOL_FAILED)
        }
    }

    private fun closeAfterConstructionFailure(closeable: AutoCloseable, failure: Throwable): Nothing {
        var selected = failure
        try {
            closeable.close()
        } catch (closeFailure: CancellationException) {
            selected = preferBootstrapFailure(selected, closeFailure)
        } catch (closeFailure: Error) {
            selected = preferBootstrapFailure(selected, closeFailure)
        } catch (_: Exception) {
            // The construction failure remains authoritative.
        }
        throw selected
    }
}

private fun preferBootstrapFailure(current: Throwable, candidate: Throwable): Throwable {
    if (current === candidate) return current
    val selected = when {
        candidate is Error && current !is Error -> candidate
        current is Error -> current
        candidate is CancellationException && current !is CancellationException -> candidate
        else -> current
    }
    val secondary = if (selected === current) candidate else current
    if (selected !== secondary) selected.addSuppressed(secondary)
    return selected
}
