package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux

internal data class YamuxConfig(
    val receiveWindowSize: Long = YamuxProtocol.INITIAL_STREAM_WINDOW,
    val maxDataFrameSize: Int = receiveWindowSize.toInt(),
    val outboundDataFrameSize: Int = 16 * 1024,
    val receiveSegmentSize: Int = 16 * 1024,
    val maxBufferedReceiveBytes: Long = 8L * 1024L * 1024L,
    val maxStreams: Int = 32,
    val maxPendingOpens: Int = 32,
    val acceptBacklog: Int = 32,
    val tombstoneCapacity: Int = 128,
    val writerMailboxCapacity: Int = 64,
    val writerQueuedByteCapacity: Long = 512L * 1024L,
    val writeTimeoutMillis: Long = 10_000L,
    val streamOpenTimeoutMillis: Long = 75_000L,
    val streamCloseTimeoutMillis: Long = 5L * 60L * 1000L,
    val keepAliveEnabled: Boolean = true,
    val keepAliveIntervalMillis: Long = 30_000L,
    val keepAliveTimeoutMillis: Long = 10_000L,
    val initialLocalStreamId: Long = 1L,
) {
    init {
        require(receiveWindowSize in YamuxProtocol.INITIAL_STREAM_WINDOW..Int.MAX_VALUE.toLong())
        require(maxDataFrameSize.toLong() == receiveWindowSize)
        require(outboundDataFrameSize in 1..maxDataFrameSize)
        require(receiveSegmentSize in 1..maxDataFrameSize)
        require(maxBufferedReceiveBytes >= maxDataFrameSize.toLong())
        require(maxStreams > 0)
        require(maxPendingOpens > 0)
        require(acceptBacklog > 0 && acceptBacklog <= maxStreams)
        require(tombstoneCapacity > 0)
        require(writerMailboxCapacity > 0)
        require(
            writerQueuedByteCapacity in
                (YamuxProtocol.HEADER_SIZE.toLong() + outboundDataFrameSize)..Int.MAX_VALUE.toLong(),
        )
        require(writeTimeoutMillis > 0L)
        require(streamOpenTimeoutMillis > 0L)
        require(streamCloseTimeoutMillis > 0L)
        require(!keepAliveEnabled || keepAliveIntervalMillis > 0L)
        require(keepAliveTimeoutMillis > 0L)
        require(initialLocalStreamId in 1L..YamuxProtocol.MAX_UINT32 && initialLocalStreamId and 1L == 1L)
    }

    internal val receiveWindowDelta: Long
        get() = receiveWindowSize - YamuxProtocol.INITIAL_STREAM_WINDOW

    internal val windowUpdateThreshold: Long
        get() = receiveWindowSize / 2L
}
