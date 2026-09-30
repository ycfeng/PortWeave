package dev.portweave.app.ui.component

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Switch
import androidx.compose.material3.SwitchDefaults
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.unit.dp
import dev.portweave.app.ui.theme.PortWeaveThemeTokens
import dev.portweave.core.model.TunnelKind

@Composable
fun TunnelStatusPill(
    text: String,
    modifier: Modifier = Modifier,
    color: Color = PortWeaveThemeTokens.status.mutedText,
) {
    Surface(
        modifier = modifier,
        shape = CircleShape,
        color = color.copy(alpha = 0.12f),
        contentColor = color,
        border = BorderStroke(1.dp, color.copy(alpha = 0.26f)),
    ) {
        Row(
            modifier = Modifier.padding(horizontal = 10.dp, vertical = 6.dp),
            horizontalArrangement = Arrangement.spacedBy(6.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Box(
                modifier = Modifier
                    .size(8.dp)
                    .background(color = color, shape = CircleShape),
            )
            Text(text = text, style = MaterialTheme.typography.labelMedium)
        }
    }
}

@Composable
fun PortWeaveSwitch(
    checked: Boolean,
    onCheckedChange: ((Boolean) -> Unit)?,
    modifier: Modifier = Modifier,
    enabled: Boolean = true,
) {
    Switch(
        checked = checked,
        onCheckedChange = onCheckedChange,
        modifier = modifier,
        enabled = enabled,
        colors = SwitchDefaults.colors(
            checkedThumbColor = MaterialTheme.colorScheme.onPrimary,
            checkedTrackColor = MaterialTheme.colorScheme.primary,
            checkedBorderColor = MaterialTheme.colorScheme.primary,
            uncheckedThumbColor = MaterialTheme.colorScheme.surface,
            uncheckedTrackColor = PortWeaveThemeTokens.status.mutedText.copy(alpha = 0.24f),
            uncheckedBorderColor = PortWeaveThemeTokens.status.mutedText.copy(alpha = 0.55f),
            disabledUncheckedThumbColor = MaterialTheme.colorScheme.surface.copy(alpha = 0.72f),
            disabledUncheckedTrackColor = PortWeaveThemeTokens.status.mutedText.copy(alpha = 0.12f),
            disabledUncheckedBorderColor = PortWeaveThemeTokens.status.mutedText.copy(alpha = 0.28f),
        ),
    )
}

fun TunnelKind.displayName(): String = when (this) {
    TunnelKind.SSH -> "SSH 本地转发"
    TunnelKind.STCP -> "STCP visitor"
}
