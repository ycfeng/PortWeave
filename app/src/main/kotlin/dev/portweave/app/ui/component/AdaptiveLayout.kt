package dev.portweave.app.ui.component

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxScope
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.widthIn
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp

const val ExpandedLayoutBreakpointDp = 840

fun adaptiveColumnCount(widthDp: Int): Int = if (widthDp >= ExpandedLayoutBreakpointDp) 2 else 1

@Composable
fun AdaptiveScreen(
    modifier: Modifier = Modifier,
    maxContentWidthDp: Int = ExpandedLayoutBreakpointDp,
    content: @Composable BoxScope.() -> Unit,
) {
    Box(
        modifier = modifier.fillMaxSize(),
        contentAlignment = Alignment.TopCenter,
    ) {
        Box(
            modifier = Modifier.fillMaxSize().widthIn(max = maxContentWidthDp.dp),
            content = content,
        )
    }
}
