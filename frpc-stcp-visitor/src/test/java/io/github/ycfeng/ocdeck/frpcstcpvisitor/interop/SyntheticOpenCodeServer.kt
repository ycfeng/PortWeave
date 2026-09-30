package io.github.ycfeng.ocdeck.frpcstcpvisitor.interop

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale
import java.util.Random
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BrokenBarrierException
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

internal class SyntheticOpenCodeServer(
    private val afterInitialSseEventFlushed: (() -> Unit)? = null,
    private val appFixtureEnabled: Boolean = false,
) : AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val activeSockets = ConcurrentHashMap.newKeySet<Socket>()
    private val workerSequence = AtomicInteger(0)
    private val fatalFailure = AtomicReference<Throwable?>(null)
    private val concurrentTrafficGate = AtomicReference<ConcurrentTrafficGate?>(null)
    private val sseCheckpoint = AtomicInteger(0)
    private val appFixtureGeneration = AtomicInteger(1)
    private val serverSocket = ServerSocket().apply {
        reuseAddress = false
        bind(InetSocketAddress(IPV4_LOOPBACK, 0), SERVER_BACKLOG)
    }
    private val executor = ThreadPoolExecutor(
        SERVER_WORKERS,
        SERVER_WORKERS,
        0L,
        TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(SERVER_QUEUE_CAPACITY),
        ThreadFactory { runnable ->
            Thread(runnable, "frp-interop-http-${workerSequence.incrementAndGet()}").apply {
                isDaemon = true
                uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, failure ->
                    fatalFailure.compareAndSet(null, failure)
                }
            }
        },
    )
    private val acceptThread = Thread(::acceptLoop, "frp-interop-http-accept").apply {
        isDaemon = true
        uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, failure ->
            fatalFailure.compareAndSet(null, failure)
        }
        start()
    }

    val port: Int
        get() = serverSocket.localPort

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        concurrentTrafficGate.getAndSet(null)?.cancel()
        try {
            serverSocket.close()
        } catch (_: Exception) {
            // Active accepted sockets are closed below.
        }
        activeSockets.forEach { socket -> closeSocket(socket) }
        executor.shutdownNow()
        try {
            if (!executor.awaitTermination(SERVER_STOP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                throw InteropFailure("synthetic server workers did not terminate within their cleanup deadline")
            }
            acceptThread.join(SERVER_STOP_TIMEOUT_MILLIS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            throw InteropFailure("synthetic server cleanup was interrupted")
        }
        if (acceptThread.isAlive) {
            throw InteropFailure("synthetic server accept loop did not terminate within its cleanup deadline")
        }
        throwIfFatal()
    }

    override fun toString(): String = "SyntheticOpenCodeServer(open=${!closed.get()})"

    fun requireHealthy() = throwIfFatal()

    fun beginConcurrentTraffic(
        expectedRequests: Int,
        publishSseCheckpointOnSuccess: Boolean = false,
    ): ConcurrentTrafficGate {
        if (closed.get()) throw InteropFailure("synthetic server was already closed")
        val gate = ConcurrentTrafficGate(
            expectedRequests = expectedRequests,
            onAllSuccessful = if (publishSseCheckpointOnSuccess) {
                { publishSseCheckpoint() }
            } else {
                null
            },
        )
        if (!concurrentTrafficGate.compareAndSet(null, gate)) {
            throw InteropFailure("synthetic server already had an active traffic gate")
        }
        return gate
    }

    fun endConcurrentTraffic(gate: ConcurrentTrafficGate) {
        if (!concurrentTrafficGate.compareAndSet(gate, null)) {
            throw InteropFailure("synthetic server traffic gate ownership was lost")
        }
        gate.cancel()
    }

    fun publishSseCheckpoint(): Int {
        if (closed.get()) throw InteropFailure("synthetic server was already closed")
        return sseCheckpoint.incrementAndGet().takeIf { it > 0 }
            ?: throw InteropFailure("synthetic server SSE checkpoint overflowed")
    }

    fun advanceAppFixture(): Int {
        if (!appFixtureEnabled || closed.get()) throw InteropFailure("synthetic App fixture was unavailable")
        return appFixtureGeneration.incrementAndGet().takeIf { it > 1 }
            ?: throw InteropFailure("synthetic App fixture generation overflowed")
    }

    private fun throwIfFatal() {
        fatalFailure.get()?.let { throw it }
    }

    private fun acceptLoop() {
        while (!closed.get()) {
            val socket = try {
                serverSocket.accept()
            } catch (_: Exception) {
                if (closed.get()) return
                continue
            }
            if (closed.get()) {
                closeSocket(socket)
                return
            }
            activeSockets += socket
            try {
                executor.execute {
                    try {
                        handle(socket)
                    } finally {
                        activeSockets -= socket
                        closeSocket(socket)
                    }
                }
            } catch (_: RejectedExecutionException) {
                activeSockets -= socket
                closeSocket(socket)
            }
        }
    }

    private fun handle(socket: Socket) {
        try {
            socket.soTimeout = SERVER_SOCKET_TIMEOUT_MILLIS
            socket.tcpNoDelay = true
            val input = socket.getInputStream()
            val output = socket.getOutputStream()
            val request = readHttpRequest(input)
            if (!appFixtureEnabled) requireNoQuery(request)
            when {
                request.method == "GET" && request.path == "/global/health" -> {
                    requireNoQuery(request)
                    requireNoRequestBody(request)
                    sendResponse(
                        output,
                        200,
                        "application/json; charset=utf-8",
                        HEALTH_BODY,
                    )
                }
                request.method == "GET" && request.path == "/global/health/traffic" -> {
                    requireNoQuery(request)
                    requireNoRequestBody(request)
                    requireConcurrentTrafficGate().participate {
                        sendResponse(
                            output,
                            200,
                            "application/json; charset=utf-8",
                            HEALTH_BODY,
                        )
                    }
                }
                request.method == "POST" && request.path == "/echo" -> {
                    requireNoQuery(request)
                    val length = request.contentLength
                        ?: throw SyntheticHttpException()
                    if (length > MAXIMUM_ECHO_BODY_BYTES) throw SyntheticHttpException()
                    requireConcurrentTrafficGate().participate {
                        val digest = digestExactBody(input, length)
                        val body = "$length\n$digest\n".toByteArray(StandardCharsets.US_ASCII)
                        sendResponse(output, 200, "text/plain; charset=us-ascii", body)
                    }
                }
                request.method == "GET" && request.path == "/global/event" -> {
                    if (appFixtureEnabled) requireAppFixtureQuery(request) else requireNoQuery(request)
                    requireNoRequestBody(request)
                    sendLiveSseResponse(
                        output,
                        if (appFixtureEnabled) appGlobalEventBody(appFixtureGeneration.get()) else GLOBAL_EVENT_BODY,
                    )
                }
                request.method == "GET" && request.path == "/event" -> {
                    if (appFixtureEnabled) requireAppFixtureQuery(request) else requireNoQuery(request)
                    requireNoRequestBody(request)
                    sendLiveSseResponse(
                        output,
                        if (appFixtureEnabled) appProjectEventBody(appFixtureGeneration.get()) else PROJECT_EVENT_BODY,
                    )
                }
                appFixtureEnabled && request.method == "GET" -> {
                    requireAppFixtureQuery(request)
                    requireNoRequestBody(request)
                    appFixtureResponse(request.path, appFixtureGeneration.get())?.let { body ->
                        sendResponse(output, 200, "application/json; charset=utf-8", body)
                    } ?: sendResponse(output, 404, "text/plain; charset=us-ascii", NOT_FOUND_BODY)
                }
                request.method == "GET" && request.path == "/large" -> {
                    requireNoQuery(request)
                    requireNoRequestBody(request)
                    requireConcurrentTrafficGate().participate {
                        sendLargeResponse(output)
                    }
                }
                else -> sendResponse(output, 404, "text/plain; charset=us-ascii", NOT_FOUND_BODY)
            }
        } catch (_: SyntheticHttpException) {
            try {
                sendResponse(
                    socket.getOutputStream(),
                    400,
                    "text/plain; charset=us-ascii",
                    BAD_REQUEST_BODY,
                )
            } catch (_: Exception) {
                // The bounded parser already rejected the connection.
            }
        } catch (_: Exception) {
            // Socket shutdown and malformed peer traffic are isolated per connection.
        }
    }

    private fun readHttpRequest(input: InputStream): HttpRequest {
        var headerBytes = 0
        val requestLine = readStrictCrlfLine(input, MAXIMUM_REQUEST_LINE_BYTES).also {
            headerBytes += it.length + 2
        }
        val parts = requestLine.split(' ')
        if (parts.size != 3 || parts[0] !in setOf("GET", "POST") ||
            !parts[1].startsWith('/') || '#' in parts[1] || parts[2] != "HTTP/1.1"
        ) {
            throw SyntheticHttpException()
        }
        val target = parts[1]
        val querySeparator = target.indexOf('?')
        if (querySeparator >= 0 && target.indexOf('?', querySeparator + 1) >= 0) throw SyntheticHttpException()
        val path = if (querySeparator >= 0) target.substring(0, querySeparator) else target
        if (path.isEmpty()) throw SyntheticHttpException()
        val query = if (querySeparator >= 0) parseQuery(target.substring(querySeparator + 1)) else emptyMap()
        val headers = LinkedHashMap<String, String>()
        repeat(MAXIMUM_HEADER_COUNT + 1) { index ->
            val line = readStrictCrlfLine(input, MAXIMUM_HEADER_LINE_BYTES)
            headerBytes += line.length + 2
            if (headerBytes > MAXIMUM_HEADER_BYTES) throw SyntheticHttpException()
            if (line.isEmpty()) {
                if (headers["host"].isNullOrBlank()) throw SyntheticHttpException()
                if (headers.containsKey("transfer-encoding")) throw SyntheticHttpException()
                val contentLength = headers["content-length"]?.let(::parseContentLength)
                return HttpRequest(parts[0], path, query, contentLength)
            }
            if (index == MAXIMUM_HEADER_COUNT || line[0] == ' ' || line[0] == '\t') {
                throw SyntheticHttpException()
            }
            val separator = line.indexOf(':')
            if (separator <= 0) throw SyntheticHttpException()
            val name = line.substring(0, separator).lowercase(Locale.ROOT)
            if (!HTTP_HEADER_NAME.matches(name) || headers.containsKey(name)) throw SyntheticHttpException()
            val value = line.substring(separator + 1).trim()
            if (value.any { it.code < 0x20 && it != '\t' }) throw SyntheticHttpException()
            headers[name] = value
        }
        throw SyntheticHttpException()
    }

    private fun requireNoRequestBody(request: HttpRequest) {
        if ((request.contentLength ?: 0L) != 0L) throw SyntheticHttpException()
    }

    private fun requireNoQuery(request: HttpRequest) {
        if (request.query.isNotEmpty()) throw SyntheticHttpException()
    }

    private fun requireAppFixtureQuery(request: HttpRequest) {
        val expected = when (request.path) {
            "/global/event" -> emptyMap()
            "/event", "/path", "/project/current", "/session/status", "/provider", "/agent",
            "/command", "/permission", "/question", "/mcp", "/lsp", "/config" ->
                mapOf("directory" to APP_DIRECTORY)
            "/session" -> mapOf(
                "directory" to APP_DIRECTORY,
                "roots" to "true",
                "limit" to "70",
            )
            "/session/session-snapshot/message" -> mapOf(
                "directory" to APP_DIRECTORY,
                "limit" to "200",
            )
            else -> return
        }
        if (request.query != expected) throw SyntheticHttpException()
    }

    private fun parseQuery(rawQuery: String): Map<String, String> {
        if (rawQuery.isEmpty()) throw SyntheticHttpException()
        val fields = rawQuery.split('&')
        if (fields.size > MAXIMUM_QUERY_FIELDS) throw SyntheticHttpException()
        val parsed = LinkedHashMap<String, String>()
        fields.forEach { field ->
            val separator = field.indexOf('=')
            if (separator <= 0) throw SyntheticHttpException()
            val name = decodeQueryComponent(field.substring(0, separator))
            val value = decodeQueryComponent(field.substring(separator + 1))
            if (!QUERY_NAME.matches(name) || parsed.put(name, value) != null) throw SyntheticHttpException()
        }
        return parsed
    }

    private fun decodeQueryComponent(value: String): String = try {
        URLDecoder.decode(value, StandardCharsets.UTF_8).also { decoded ->
            if (decoded.any { it.code < 0x20 || it.code == 0x7f }) throw SyntheticHttpException()
        }
    } catch (_: IllegalArgumentException) {
        throw SyntheticHttpException()
    }

    private fun requireConcurrentTrafficGate(): ConcurrentTrafficGate =
        concurrentTrafficGate.get() ?: throw SyntheticHttpException()

    private fun digestExactBody(input: InputStream, byteCount: Long): String {
        val digest = MessageDigest.getInstance("SHA-256")
        val buffer = ByteArray(BODY_BUFFER_BYTES)
        var remaining = byteCount
        while (remaining > 0L) {
            val count = input.read(buffer, 0, minOf(buffer.size.toLong(), remaining).toInt())
            if (count < 0) throw SyntheticHttpException()
            digest.update(buffer, 0, count)
            remaining -= count
        }
        buffer.fill(0)
        return digest.digest().joinToString("") { byte -> "%02x".format(byte) }
    }

    private fun sendResponse(output: OutputStream, status: Int, contentType: String, body: ByteArray) {
        if (body.size > MAXIMUM_RESPONSE_BODY_BYTES) throw SyntheticHttpException()
        writeResponseHead(output, status, contentType, body.size.toLong())
        output.write(body)
        output.flush()
    }

    private fun sendLargeResponse(output: OutputStream) {
        writeResponseHead(output, 200, "application/octet-stream", INTEROP_LARGE_BODY_BYTES.toLong())
        val random = Random(INTEROP_LARGE_BODY_SEED)
        val buffer = ByteArray(BODY_BUFFER_BYTES)
        var remaining = INTEROP_LARGE_BODY_BYTES
        try {
            while (remaining > 0) {
                random.nextBytes(buffer)
                val count = minOf(buffer.size, remaining)
                output.write(buffer, 0, count)
                remaining -= count
            }
            output.flush()
        } finally {
            buffer.fill(0)
        }
    }

    private fun sendLiveSseResponse(output: OutputStream, firstEvent: ByteArray) {
        val observedCheckpoint = sseCheckpoint.get()
        writeResponseHead(output, 200, "text/event-stream; charset=utf-8", contentLength = null)
        output.write(firstEvent)
        output.flush()
        afterInitialSseEventFlushed?.invoke()
        while (!closed.get()) {
            sleepBounded(SSE_KEEPALIVE_INTERVAL_MILLIS)
            val currentCheckpoint = sseCheckpoint.get()
            if (currentCheckpoint != observedCheckpoint) {
                output.write(sseCheckpointBody(currentCheckpoint))
                output.flush()
                // The checkpoint proves the stream overlapped the traffic gate; close it so
                // completed profiles cannot retain synthetic-server workers.
                return
            } else {
                output.write(SSE_KEEPALIVE_BODY)
            }
            output.flush()
        }
    }

    private fun sseCheckpointBody(checkpoint: Int): ByteArray =
        "event: checkpoint\ndata: $checkpoint\n\n".toByteArray(StandardCharsets.UTF_8)

    private fun appFixtureResponse(path: String, generation: Int): ByteArray? = when (path) {
        "/path" -> APP_PATH_BODY
        "/project/current" -> APP_PROJECT_BODY
        "/session" -> appSessionBody(generation)
        "/session/status" -> APP_SESSION_STATUS_BODY
        "/provider", "/mcp", "/lsp", "/config" -> EMPTY_OBJECT_BODY
        "/agent", "/command", "/permission", "/question" -> EMPTY_ARRAY_BODY
        "/session/session-snapshot/message" -> EMPTY_ARRAY_BODY
        else -> null
    }

    private fun appSessionBody(generation: Int): ByteArray =
        """[{"id":"session-snapshot","directory":"/workspace","title":"snapshot-$generation","time":{"updated":${generation * 100L}}}]"""
            .toByteArray(StandardCharsets.UTF_8)

    private fun appProjectEventBody(generation: Int): ByteArray = sseJsonBody(
        """{"type":"session.updated","properties":{"info":{"id":"session-project-sse","title":"project-sse-$generation","directory":"/workspace","time":{"updated":"${generation * 100L + 1L}"}}}}""",
    )

    private fun appGlobalEventBody(generation: Int): ByteArray = sseJsonBody(
        """{"directory":"/workspace","payload":{"type":"sync","syncEvent":{"type":"session.updated","properties":{"info":{"id":"session-global-sse","title":"global-sse-$generation","directory":"/workspace","time":{"updated":"${generation * 100L + 2L}"}}}}}}""",
    )

    private fun sseJsonBody(json: String): ByteArray =
        "event: ready\ndata: $json\n\n".toByteArray(StandardCharsets.UTF_8)

    private fun writeResponseHead(
        output: OutputStream,
        status: Int,
        contentType: String,
        contentLength: Long?,
    ) {
        val reason = when (status) {
            200 -> "OK"
            400 -> "Bad Request"
            404 -> "Not Found"
            else -> throw SyntheticHttpException()
        }
        val head = buildString {
            append("HTTP/1.1 $status $reason\r\n")
            append("Content-Type: $contentType\r\n")
            if (contentLength != null) append("Content-Length: $contentLength\r\n")
            append("Connection: close\r\n")
            append("Cache-Control: no-store\r\n")
            append("\r\n")
        }.toByteArray(StandardCharsets.US_ASCII)
        output.write(head)
    }

    private data class HttpRequest(
        val method: String,
        val path: String,
        val query: Map<String, String>,
        val contentLength: Long?,
    )

    private class SyntheticHttpException : Exception()

    private companion object {
        val IPV4_LOOPBACK: InetAddress = InetAddress.getByAddress(byteArrayOf(127, 0, 0, 1))
        val HTTP_HEADER_NAME = Regex("[a-z0-9!#$%&'*+.^_`|~-]+")
        val QUERY_NAME = Regex("[a-z][a-z0-9_]*")
        val HEALTH_BODY = "{\"healthy\":true,\"version\":\"interop\"}".toByteArray(StandardCharsets.UTF_8)
        val GLOBAL_EVENT_BODY = "event: ready\ndata: {\"scope\":\"global\"}\n\n".toByteArray(StandardCharsets.UTF_8)
        val PROJECT_EVENT_BODY = "event: ready\ndata: {\"scope\":\"project\"}\n\n".toByteArray(StandardCharsets.UTF_8)
        val APP_PATH_BODY = "{\"directory\":\"/workspace\",\"worktree\":\"/workspace\"}"
            .toByteArray(StandardCharsets.UTF_8)
        val APP_PROJECT_BODY =
            "{\"id\":\"project-e2e\",\"worktree\":\"/workspace\",\"name\":\"E2E\"}"
                .toByteArray(StandardCharsets.UTF_8)
        val APP_SESSION_STATUS_BODY = "{\"session-snapshot\":{\"type\":\"idle\"}}"
            .toByteArray(StandardCharsets.UTF_8)
        val EMPTY_OBJECT_BODY = "{}".toByteArray(StandardCharsets.UTF_8)
        val EMPTY_ARRAY_BODY = "[]".toByteArray(StandardCharsets.UTF_8)
        val SSE_KEEPALIVE_BODY = ": keepalive\n\n".toByteArray(StandardCharsets.UTF_8)
        val BAD_REQUEST_BODY = "bad request\n".toByteArray(StandardCharsets.US_ASCII)
        val NOT_FOUND_BODY = "not found\n".toByteArray(StandardCharsets.US_ASCII)
        const val SERVER_BACKLOG = 32
        const val SERVER_WORKERS = 8
        const val SERVER_QUEUE_CAPACITY = 32
        const val SERVER_SOCKET_TIMEOUT_MILLIS = 10_000
        const val SERVER_STOP_TIMEOUT_MILLIS = 5_000L
        const val MAXIMUM_REQUEST_LINE_BYTES = 4 * 1024
        const val MAXIMUM_HEADER_LINE_BYTES = 8 * 1024
        const val MAXIMUM_HEADER_BYTES = 32 * 1024
        const val MAXIMUM_HEADER_COUNT = 64
        const val MAXIMUM_QUERY_FIELDS = 8
        const val MAXIMUM_ECHO_BODY_BYTES = 2L * 1024L * 1024L
        const val MAXIMUM_RESPONSE_BODY_BYTES = 64 * 1024
        const val BODY_BUFFER_BYTES = 16 * 1024
        const val SSE_KEEPALIVE_INTERVAL_MILLIS = 100L
        const val APP_DIRECTORY = "/workspace"
    }
}

internal object TunnelHttpProbe {
    fun health(port: Int, concurrentTraffic: Boolean = false) {
        withDeadlineSocket(port) { socket ->
            val output = socket.getOutputStream()
            val path = if (concurrentTraffic) "/global/health/traffic" else "/global/health"
            writeRequestHead(output, "GET", path, contentLength = null)
            output.flush()
            val response = readResponseHead(socket.getInputStream())
            if (response.status != 200 || !response.contentType.startsWith("application/json")) {
                throw TunnelProbeException()
            }
            val body = readExactBody(
                socket.getInputStream(),
                response.contentLength ?: throw TunnelProbeException(),
                MAXIMUM_SMALL_RESPONSE_BYTES,
            )
            val value = String(body, StandardCharsets.UTF_8)
            if (value != "{\"healthy\":true,\"version\":\"interop\"}") throw TunnelProbeException()
            expectEndOfStream(socket.getInputStream())
        }
    }

    fun echo(port: Int, payload: ByteArray) {
        if (payload.size > MAXIMUM_ECHO_REQUEST_BYTES) throw TunnelProbeException()
        withDeadlineSocket(port) { socket ->
            val output = socket.getOutputStream()
            writeRequestHead(output, "POST", "/echo", payload.size.toLong())
            var offset = 0
            while (offset < payload.size) {
                val count = minOf(WRITE_CHUNK_BYTES, payload.size - offset)
                output.write(payload, offset, count)
                offset += count
            }
            output.flush()
            val response = readResponseHead(socket.getInputStream())
            if (response.status != 200 || !response.contentType.startsWith("text/plain")) {
                throw TunnelProbeException()
            }
            val body = readExactBody(
                socket.getInputStream(),
                response.contentLength ?: throw TunnelProbeException(),
                MAXIMUM_SMALL_RESPONSE_BYTES,
            )
            val expected = "${payload.size}\n${sha256(payload)}\n"
            if (String(body, StandardCharsets.US_ASCII) != expected) throw TunnelProbeException()
            expectEndOfStream(socket.getInputStream())
        }
    }

    fun json(port: Int, path: String): String {
        if (!path.startsWith('/') || path.length > MAXIMUM_PROBE_PATH_CHARS ||
            path.any { it.code !in 0x21..0x7e }
        ) {
            throw TunnelProbeException()
        }
        return withDeadlineSocket(port) { socket ->
            val output = socket.getOutputStream()
            writeRequestHead(output, "GET", path, contentLength = null)
            output.flush()
            val input = socket.getInputStream()
            val response = readResponseHead(input)
            if (response.status != 200 || !response.contentType.startsWith("application/json")) {
                throw TunnelProbeException()
            }
            val body = readExactBody(
                input,
                response.contentLength ?: throw TunnelProbeException(),
                MAXIMUM_SMALL_RESPONSE_BYTES,
            )
            expectEndOfStream(input)
            String(body, StandardCharsets.UTF_8)
        }
    }

    fun firstSseEvent(port: Int, path: String, expectedData: String) {
        openSseSession(port, path, expectedData).use { }
    }

    fun openSseSession(port: Int, path: String, expectedData: String): LiveSseSession {
        if (path.length > MAXIMUM_PROBE_PATH_CHARS || path.any { it.code !in 0x21..0x7e } ||
            path.substringBefore('?') !in setOf("/global/event", "/event")
        ) {
            throw TunnelProbeException()
        }
        val connection = DeadlineSocket.open(port, LIVE_SSE_HARD_TIMEOUT_MILLIS)
        try {
            val socket = connection.socket
            val output = socket.getOutputStream()
            writeRequestHead(output, "GET", path, contentLength = null)
            output.flush()
            val input = socket.getInputStream()
            val response = readResponseHead(input)
            if (response.status != 200 || !response.contentType.startsWith("text/event-stream") ||
                response.contentLength != null
            ) {
                throw TunnelProbeException()
            }
            var event = "message"
            val data = StringBuilder()
            repeat(MAXIMUM_SSE_LINES) {
                val line = readFlexibleLine(input, MAXIMUM_SSE_LINE_BYTES)
                    ?: throw TunnelProbeException()
                if (line.isEmpty()) {
                    if (event != "ready" || data.toString() != expectedData) throw TunnelProbeException()
                    return LiveSseSession(connection)
                }
                when {
                    line.startsWith("event:") -> event = line.substringAfter(':').trimStart()
                    line.startsWith("data:") -> {
                        if (data.isNotEmpty()) data.append('\n')
                        data.append(line.substringAfter(':').trimStart())
                        if (data.length > MAXIMUM_SSE_DATA_CHARS) throw TunnelProbeException()
                    }
                    line.startsWith(":") -> Unit
                    else -> throw TunnelProbeException()
                }
            }
            throw TunnelProbeException()
        } catch (failure: Throwable) {
            try {
                connection.close()
            } catch (_: Exception) {
                // The original probe failure remains authoritative.
            }
            when (failure) {
                is Error -> throw failure
                is TunnelProbeException -> throw failure
                else -> throw TunnelProbeException()
            }
        }
    }

    fun largeDownload(port: Int) {
        withDeadlineSocket(port) { socket ->
            val output = socket.getOutputStream()
            writeRequestHead(output, "GET", "/large", contentLength = null)
            output.flush()
            val input = socket.getInputStream()
            val response = readResponseHead(input)
            if (response.status != 200 || !response.contentType.startsWith("application/octet-stream") ||
                response.contentLength != INTEROP_LARGE_BODY_BYTES.toLong()
            ) {
                throw TunnelProbeException()
            }
            val responseLength = response.contentLength
            val digest = MessageDigest.getInstance("SHA-256")
            val buffer = ByteArray(BODY_READ_BUFFER_BYTES)
            var remaining = responseLength
            try {
                while (remaining > 0L) {
                    val count = input.read(buffer, 0, minOf(buffer.size.toLong(), remaining).toInt())
                    if (count < 0) throw TunnelProbeException()
                    digest.update(buffer, 0, count)
                    remaining -= count
                }
                if (!digest.digest().contentEquals(EXPECTED_LARGE_BODY_SHA256)) throw TunnelProbeException()
                expectEndOfStream(input)
            } finally {
                buffer.fill(0)
            }
        }
    }

    private fun writeRequestHead(output: OutputStream, method: String, path: String, contentLength: Long?) {
        val head = buildString {
            append("$method $path HTTP/1.1\r\n")
            append("Host: 127.0.0.1\r\n")
            append("Accept-Encoding: identity\r\n")
            append("Connection: close\r\n")
            if (contentLength != null) {
                append("Content-Type: application/octet-stream\r\n")
                append("Content-Length: $contentLength\r\n")
            }
            append("\r\n")
        }.toByteArray(StandardCharsets.US_ASCII)
        output.write(head)
    }

    private fun readResponseHead(input: InputStream): HttpResponseHead {
        var headerBytes = 0
        val statusLine = readStrictCrlfLine(input, MAXIMUM_STATUS_LINE_BYTES).also {
            headerBytes += it.length + 2
        }
        val statusParts = statusLine.split(' ', limit = 3)
        if (statusParts.size < 2 || statusParts[0] != "HTTP/1.1") throw TunnelProbeException()
        val status = statusParts[1].toIntOrNull() ?: throw TunnelProbeException()
        val headers = LinkedHashMap<String, String>()
        repeat(MAXIMUM_HEADER_COUNT + 1) { index ->
            val line = readStrictCrlfLine(input, MAXIMUM_HEADER_LINE_BYTES)
            headerBytes += line.length + 2
            if (headerBytes > MAXIMUM_HEADER_BYTES) throw TunnelProbeException()
            if (line.isEmpty()) {
                if (headers.containsKey("transfer-encoding")) throw TunnelProbeException()
                val contentLength = headers["content-length"]?.let(::parseContentLength)
                val contentType = headers["content-type"]?.lowercase(Locale.ROOT)
                    ?: throw TunnelProbeException()
                return HttpResponseHead(status, contentType, contentLength)
            }
            if (index == MAXIMUM_HEADER_COUNT || line[0] == ' ' || line[0] == '\t') {
                throw TunnelProbeException()
            }
            val separator = line.indexOf(':')
            if (separator <= 0) throw TunnelProbeException()
            val name = line.substring(0, separator).lowercase(Locale.ROOT)
            if (!HTTP_HEADER_NAME.matches(name) || headers.containsKey(name)) throw TunnelProbeException()
            val value = line.substring(separator + 1).trim()
            if (value.any { it.code < 0x20 && it != '\t' }) throw TunnelProbeException()
            headers[name] = value
        }
        throw TunnelProbeException()
    }

    private fun readExactBody(input: InputStream, length: Long, maximumBytes: Long): ByteArray {
        if (length < 0L || length > maximumBytes || length > Int.MAX_VALUE) throw TunnelProbeException()
        val body = ByteArray(length.toInt())
        var offset = 0
        while (offset < body.size) {
            val count = input.read(body, offset, body.size - offset)
            if (count < 0) throw TunnelProbeException()
            offset += count
        }
        return body
    }

    private fun expectEndOfStream(input: InputStream) {
        if (input.read() != -1) throw TunnelProbeException()
    }

    private fun sha256(value: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(value).joinToString("") { byte -> "%02x".format(byte) }

    private data class HttpResponseHead(
        val status: Int,
        val contentType: String,
        val contentLength: Long?,
    )

    internal val IPV4_LOOPBACK: InetAddress = InetAddress.getByAddress(byteArrayOf(127, 0, 0, 1))
    private val HTTP_HEADER_NAME = Regex("[a-z0-9!#$%&'*+.^_`|~-]+")
    internal const val PROBE_CONNECT_TIMEOUT_MILLIS = 2_000
    internal const val PROBE_READ_TIMEOUT_MILLIS = 10_000
    private const val PROBE_HARD_TIMEOUT_MILLIS = 20_000L
    private const val LIVE_SSE_HARD_TIMEOUT_MILLIS = 120_000L
    private const val MAXIMUM_STATUS_LINE_BYTES = 4 * 1024
    private const val MAXIMUM_HEADER_LINE_BYTES = 8 * 1024
    private const val MAXIMUM_HEADER_BYTES = 32 * 1024
    private const val MAXIMUM_HEADER_COUNT = 64
    private const val MAXIMUM_SMALL_RESPONSE_BYTES = 64L * 1024L
    private const val MAXIMUM_PROBE_PATH_CHARS = 1024
    private const val MAXIMUM_ECHO_REQUEST_BYTES = 2 * 1024 * 1024
    private const val WRITE_CHUNK_BYTES = 16 * 1024
    private const val MAXIMUM_SSE_LINE_BYTES = 8 * 1024
    private const val MAXIMUM_SSE_LINES = 64
    private const val MAXIMUM_SSE_DATA_CHARS = 32 * 1024
    private const val BODY_READ_BUFFER_BYTES = 16 * 1024
    private val EXPECTED_LARGE_BODY_SHA256 = deterministicInteropSha256(
        INTEROP_LARGE_BODY_BYTES,
        INTEROP_LARGE_BODY_SEED,
    )

    private inline fun <T> withDeadlineSocket(port: Int, block: (Socket) -> T): T = try {
        DeadlineSocket.open(port, PROBE_HARD_TIMEOUT_MILLIS).use { connection ->
            block(connection.socket)
        }
    } catch (failure: TunnelProbeException) {
        throw failure
    } catch (_: Exception) {
        throw TunnelProbeException()
    }
}

internal class LiveSseSession(
    private val connection: DeadlineSocket,
) : AutoCloseable {
    fun awaitCheckpoint(expectedCheckpoint: Int, timeoutMillis: Long) {
        if (expectedCheckpoint <= 0) throw TunnelProbeException()
        val deadline = InteropDeadline.afterMillis(timeoutMillis)
        val input = connection.socket.getInputStream()
        var event = "message"
        val data = StringBuilder()
        var lineCount = 0
        while (!deadline.isExpired()) {
            connection.throwIfTimedOut()
            connection.socket.soTimeout = minOf(
                SSE_CHECKPOINT_READ_TIMEOUT_MILLIS,
                deadline.remainingMillis().coerceAtLeast(1L).toInt(),
            )
            val line = try {
                readFlexibleLine(input, MAXIMUM_SSE_CHECKPOINT_LINE_BYTES)
                    ?: throw TunnelProbeException()
            } catch (_: SocketTimeoutException) {
                continue
            } catch (failure: TunnelProbeException) {
                throw failure
            } catch (_: Exception) {
                connection.throwIfTimedOut()
                throw TunnelProbeException()
            }
            lineCount += 1
            if (lineCount > MAXIMUM_SSE_CHECKPOINT_LINES) throw TunnelProbeException()
            if (line.isEmpty()) {
                if (event == "checkpoint") {
                    if (data.toString() != expectedCheckpoint.toString()) throw TunnelProbeException()
                    return
                }
                if (event != "message" || data.isNotEmpty()) throw TunnelProbeException()
                event = "message"
                data.setLength(0)
                continue
            }
            when {
                line.startsWith(":") -> Unit
                line.startsWith("event:") -> event = line.substringAfter(':').trimStart()
                line.startsWith("data:") -> {
                    if (data.isNotEmpty()) data.append('\n')
                    data.append(line.substringAfter(':').trimStart())
                    if (data.length > MAXIMUM_SSE_CHECKPOINT_DATA_CHARS) throw TunnelProbeException()
                }
                else -> throw TunnelProbeException()
            }
        }
        throw TunnelProbeException()
    }

    fun awaitDisconnected(timeoutMillis: Long) {
        val deadline = InteropDeadline.afterMillis(timeoutMillis)
        val input = connection.socket.getInputStream()
        val buffer = ByteArray(1024)
        try {
            while (!deadline.isExpired()) {
                connection.throwIfTimedOut()
                connection.socket.soTimeout = minOf(
                    SSE_DISCONNECT_READ_TIMEOUT_MILLIS,
                    deadline.remainingMillis().coerceAtLeast(1L).toInt(),
                )
                try {
                    if (input.read(buffer) < 0) return
                } catch (_: SocketTimeoutException) {
                    continue
                } catch (_: Exception) {
                    connection.throwIfTimedOut()
                    return
                }
            }
            throw TunnelProbeException()
        } finally {
            buffer.fill(0)
        }
    }

    override fun close() = connection.close()

    override fun toString(): String = "LiveSseSession(open=${!connection.isClosed})"

    private companion object {
        const val SSE_DISCONNECT_READ_TIMEOUT_MILLIS = 500
        const val SSE_CHECKPOINT_READ_TIMEOUT_MILLIS = 500
        const val MAXIMUM_SSE_CHECKPOINT_LINE_BYTES = 8 * 1024
        // A live session has a 120-second hard lifetime. At two lines per 100 ms keepalive,
        // this bounded budget covers its full valid backlog plus the checkpoint event.
        const val MAXIMUM_SSE_CHECKPOINT_LINES = 4096
        const val MAXIMUM_SSE_CHECKPOINT_DATA_CHARS = 128
    }
}

internal class ConcurrentTrafficGate(
    expectedRequests: Int,
    private val onAllSuccessful: (() -> Unit)? = null,
) {
    private val expected = expectedRequests.also {
        if (it <= 0) throw InteropFailure("concurrent traffic gate expected count was invalid")
    }
    private val barrier = CyclicBarrier(expectedRequests)
    private val participants = AtomicInteger(0)
    private val completed = AtomicInteger(0)
    private val successful = AtomicInteger(0)
    private val completionCallbackStarted = AtomicBoolean(false)
    private val completionCallbackSucceeded = AtomicBoolean(onAllSuccessful == null)
    private val failure = AtomicReference<InteropFailure?>(null)

    fun participate(block: () -> Unit) {
        val participant = participants.incrementAndGet()
        if (participant > expected) {
            val overflow = InteropFailure("concurrent traffic gate received too many requests")
            failure.compareAndSet(null, overflow)
            throw overflow
        }
        var requestSucceeded = false
        try {
            try {
                barrier.await(OVERLAP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                throw recordFailure("concurrent traffic gate was interrupted")
            } catch (_: BrokenBarrierException) {
                throw recordFailure("concurrent traffic gate was broken")
            } catch (_: TimeoutException) {
                throw recordFailure("concurrent traffic did not overlap within its deadline")
            }
            block()
            requestSucceeded = true
        } catch (failure: Error) {
            throw failure
        } catch (failure: Exception) {
            recordFailure("concurrent traffic request failed")
            throw failure
        } finally {
            if (requestSucceeded) successful.incrementAndGet()
            val completedCount = completed.incrementAndGet()
            if (completedCount == expected && successful.get() == expected && failure.get() == null) {
                runCompletionCallback()
            }
        }
    }

    fun requireSatisfied() {
        failure.get()?.let { throw it }
        if (participants.get() != expected || completed.get() != expected || successful.get() != expected ||
            !completionCallbackSucceeded.get()
        ) {
            throw InteropFailure("concurrent traffic did not complete the required overlap")
        }
    }

    fun cancel() {
        barrier.reset()
    }

    override fun toString(): String =
        "ConcurrentTrafficGate(expected=$expected, participants=${participants.get()}, " +
            "completed=${completed.get()}, successful=${successful.get()})"

    private fun runCompletionCallback() {
        if (!completionCallbackStarted.compareAndSet(false, true)) {
            throw recordFailure("concurrent traffic completion callback ownership was lost")
        }
        try {
            onAllSuccessful?.invoke()
            completionCallbackSucceeded.set(true)
        } catch (failure: Error) {
            throw failure
        } catch (_: Exception) {
            throw recordFailure("concurrent traffic completion callback failed")
        }
    }

    private fun recordFailure(message: String): InteropFailure {
        val candidate = InteropFailure(message)
        failure.compareAndSet(null, candidate)
        return failure.get() ?: candidate
    }

    private companion object {
        const val OVERLAP_TIMEOUT_MILLIS = 10_000L
    }
}

internal class DeadlineSocket private constructor(
    val socket: Socket,
    private val timedOut: AtomicBoolean,
    private val watchdog: Thread,
) : AutoCloseable {
    private val closed = AtomicBoolean(false)

    val isClosed: Boolean
        get() = closed.get()

    fun throwIfTimedOut() {
        if (timedOut.get()) throw TunnelProbeException()
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        watchdog.interrupt()
        closeSocket(socket)
        try {
            watchdog.join(WATCHDOG_JOIN_TIMEOUT_MILLIS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            throw TunnelProbeException()
        }
        if (watchdog.isAlive) throw TunnelProbeException()
        throwIfTimedOut()
    }

    companion object {
        fun open(port: Int, hardTimeoutMillis: Long): DeadlineSocket {
            if (port !in 1..65_535 || hardTimeoutMillis <= 0L) throw TunnelProbeException()
            val socket = try {
                Socket().apply {
                    tcpNoDelay = true
                    soTimeout = TunnelHttpProbe.PROBE_READ_TIMEOUT_MILLIS
                    connect(
                        InetSocketAddress(TunnelHttpProbe.IPV4_LOOPBACK, port),
                        TunnelHttpProbe.PROBE_CONNECT_TIMEOUT_MILLIS,
                    )
                }
            } catch (_: Exception) {
                throw TunnelProbeException()
            }
            val timedOut = AtomicBoolean(false)
            val watchdog = Thread(
                {
                    try {
                        Thread.sleep(hardTimeoutMillis)
                        timedOut.set(true)
                        closeSocket(socket)
                    } catch (_: InterruptedException) {
                        Thread.currentThread().interrupt()
                    }
                },
                "frp-interop-socket-deadline",
            ).apply {
                isDaemon = true
                start()
            }
            return DeadlineSocket(socket, timedOut, watchdog)
        }

        private const val WATCHDOG_JOIN_TIMEOUT_MILLIS = 1_000L
    }
}

internal class TunnelProbeException : Exception()

internal const val INTEROP_LARGE_BODY_BYTES = 768 * 1024 + 17
internal const val INTEROP_LARGE_BODY_SEED = 0x4f434445434bL

internal fun deterministicInteropBytes(size: Int, seed: Long): ByteArray {
    if (size < 0) throw IllegalArgumentException("size must be non-negative")
    return ByteArray(size).also { Random(seed).nextBytes(it) }
}

private fun deterministicInteropSha256(size: Int, seed: Long): ByteArray {
    val digest = MessageDigest.getInstance("SHA-256")
    val random = Random(seed)
    val buffer = ByteArray(16 * 1024)
    var remaining = size
    try {
        while (remaining > 0) {
            random.nextBytes(buffer)
            val count = minOf(buffer.size, remaining)
            digest.update(buffer, 0, count)
            remaining -= count
        }
        return digest.digest()
    } finally {
        buffer.fill(0)
    }
}

private fun parseContentLength(value: String): Long {
    if (value.isEmpty() || value.length > 19 || value.any { it !in '0'..'9' }) {
        throw TunnelProbeException()
    }
    return value.toLongOrNull()?.takeIf { it >= 0L } ?: throw TunnelProbeException()
}

private fun readStrictCrlfLine(input: InputStream, maximumBytes: Int): String {
    val output = ByteArrayOutputStream(minOf(maximumBytes, 256))
    var previousWasCr = false
    while (true) {
        val value = input.read()
        if (value < 0) throw TunnelProbeException()
        if (previousWasCr) {
            if (value != '\n'.code) throw TunnelProbeException()
            return output.toString(StandardCharsets.US_ASCII)
        }
        if (value == '\r'.code) {
            previousWasCr = true
        } else {
            if (value == '\n'.code || value > 0x7f || output.size() >= maximumBytes) {
                throw TunnelProbeException()
            }
            output.write(value)
        }
    }
}

private fun readFlexibleLine(input: InputStream, maximumBytes: Int): String? {
    val output = ByteArrayOutputStream(minOf(maximumBytes, 256))
    while (true) {
        val value = input.read()
        if (value < 0) {
            if (output.size() == 0) return null
            return output.toString(StandardCharsets.UTF_8).removeSuffix("\r")
        }
        if (value == '\n'.code) return output.toString(StandardCharsets.UTF_8).removeSuffix("\r")
        if (output.size() >= maximumBytes) throw TunnelProbeException()
        output.write(value)
    }
}

private fun closeSocket(socket: Socket) {
    try {
        socket.close()
    } catch (_: Exception) {
        // Socket ownership has already been invalidated.
    }
}
