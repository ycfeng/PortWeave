package dev.portweave.app.ui.component

import org.junit.Assert.assertEquals
import org.junit.Test

class AdaptiveLayoutTest {
    @Test
    fun expandedBreakpointUsesTwoColumns() {
        assertEquals(1, adaptiveColumnCount(839))
        assertEquals(2, adaptiveColumnCount(840))
        assertEquals(2, adaptiveColumnCount(1_200))
    }
}
