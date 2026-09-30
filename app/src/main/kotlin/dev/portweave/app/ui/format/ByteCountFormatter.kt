package dev.portweave.app.ui.format

import java.util.Locale

private const val UnitBase = 1024.0
private const val PromotionThreshold = 10_000.0
private val ByteUnits = arrayOf("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB")

/** Keeps the displayed number below five digits by promoting it to the next binary unit. */
fun formatByteCount(byteCount: Long): String {
    var value = byteCount.coerceAtLeast(0).toDouble()
    var unitIndex = 0
    while (value >= PromotionThreshold && unitIndex < ByteUnits.lastIndex) {
        value /= UnitBase
        unitIndex += 1
    }
    return if (unitIndex == 0) {
        "${value.toLong()} ${ByteUnits[unitIndex]}"
    } else {
        String.format(Locale.ROOT, "%.1f %s", value, ByteUnits[unitIndex])
    }
}
