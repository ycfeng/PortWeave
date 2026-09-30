package dev.portweave.app.ui.format

import org.junit.Assert.assertEquals
import org.junit.Test

class ByteCountFormatterTest {
    @Test
    fun keepsValuesBelowFiveDigitsInTheCurrentUnit() {
        assertEquals("0 B", formatByteCount(0))
        assertEquals("9999 B", formatByteCount(9_999))
    }

    @Test
    fun promotesFiveDigitValuesToTheNextUnit() {
        assertEquals("9.8 KiB", formatByteCount(10_000))
        assertEquals("12.8 KiB", formatByteCount(13_082))
        assertEquals("539.9 KiB", formatByteCount(552_842))
    }

    @Test
    fun promotesAgainWhenTheConvertedValueReachesFiveDigits() {
        assertEquals("9999.0 KiB", formatByteCount(9_999L * 1_024))
        assertEquals("9.8 MiB", formatByteCount(10_000L * 1_024))
        assertEquals("8192.0 PiB", formatByteCount(Long.MAX_VALUE))
    }

    @Test
    fun clampsUnexpectedNegativeCounters() {
        assertEquals("0 B", formatByteCount(-1))
    }
}
