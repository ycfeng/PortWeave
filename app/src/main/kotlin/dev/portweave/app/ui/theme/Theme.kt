package dev.portweave.app.ui.theme

import android.app.Activity
import android.os.Build
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Shapes
import androidx.compose.material3.Typography
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.dynamicDarkColorScheme
import androidx.compose.material3.dynamicLightColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.Immutable
import androidx.compose.runtime.ReadOnlyComposable
import androidx.compose.runtime.SideEffect
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalView
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.core.view.WindowCompat
import dev.portweave.core.data.settings.AppSettings
import dev.portweave.core.data.settings.ThemeMode

@Immutable
data class PortWeaveStatusColors(
    val success: Color,
    val onSuccess: Color,
    val warning: Color,
    val onWarning: Color,
    val mutedText: Color,
    val border: Color,
)

private val LightStatusColors = PortWeaveStatusColors(
    success = Color(0xFF18875A),
    onSuccess = Color.White,
    warning = Color(0xFFA86100),
    onWarning = Color.White,
    mutedText = Color(0xFF657084),
    border = Color(0xFFD7DFEA),
)

private val DarkStatusColors = PortWeaveStatusColors(
    success = Color(0xFF67D9A3),
    onSuccess = Color(0xFF003824),
    warning = Color(0xFFFFB86B),
    onWarning = Color(0xFF492900),
    mutedText = Color(0xFFAAB4C5),
    border = Color(0x4DFFFFFF),
)

private val LocalPortWeaveStatusColors = staticCompositionLocalOf { LightStatusColors }

object PortWeaveThemeTokens {
    val status: PortWeaveStatusColors
        @Composable
        @ReadOnlyComposable
        get() = LocalPortWeaveStatusColors.current

    val mono: FontFamily = FontFamily.Monospace
}

private val LightColors = lightColorScheme(
    primary = Color(0xFF3157D5),
    onPrimary = Color.White,
    secondary = Color(0xFF00897B),
    onSecondary = Color.White,
    background = Color(0xFFF5F7FB),
    onBackground = Color(0xFF182130),
    surface = Color.White,
    onSurface = Color(0xFF182130),
    surfaceVariant = Color(0xFFEAF0F8),
    onSurfaceVariant = Color(0xFF657084),
    outline = Color(0xFFD7DFEA),
    error = Color(0xFFBA1A1A),
)

private val DarkColors = darkColorScheme(
    primary = Color(0xFFA9BBFF),
    onPrimary = Color(0xFF002A78),
    secondary = Color(0xFF79D7C5),
    onSecondary = Color(0xFF00372F),
    background = Color(0xFF0C111B),
    onBackground = Color(0xFFE8EDF7),
    surface = Color(0xFF141B28),
    onSurface = Color(0xFFE8EDF7),
    surfaceVariant = Color(0xFF1D2737),
    onSurfaceVariant = Color(0xFFAAB4C5),
    outline = Color(0x4DFFFFFF),
    error = Color(0xFFFFB4AB),
)

private val PortWeaveTypography = Typography(
    titleLarge = TextStyle(fontSize = 22.sp, lineHeight = 28.sp, fontWeight = FontWeight.SemiBold),
    titleMedium = TextStyle(fontSize = 16.sp, lineHeight = 24.sp, fontWeight = FontWeight.SemiBold),
    bodyMedium = TextStyle(fontSize = 14.sp, lineHeight = 20.sp),
    labelMedium = TextStyle(fontSize = 12.sp, lineHeight = 16.sp, fontWeight = FontWeight.Medium),
)

private val PortWeaveShapes = Shapes(
    extraLarge = androidx.compose.foundation.shape.RoundedCornerShape(20.dp),
    large = androidx.compose.foundation.shape.RoundedCornerShape(16.dp),
    medium = androidx.compose.foundation.shape.RoundedCornerShape(12.dp),
    small = androidx.compose.foundation.shape.RoundedCornerShape(8.dp),
)

@Composable
fun PortWeaveTheme(
    settings: AppSettings,
    content: @Composable () -> Unit,
) {
    val systemDark = isSystemInDarkTheme()
    val dark = when (settings.themeMode) {
        ThemeMode.SYSTEM -> systemDark
        ThemeMode.LIGHT -> false
        ThemeMode.DARK -> true
    }
    val context = LocalContext.current
    val colors = if (settings.dynamicColor && Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
        if (dark) dynamicDarkColorScheme(context) else dynamicLightColorScheme(context)
    } else if (dark) {
        DarkColors
    } else {
        LightColors
    }
    val statusColors = if (dark) DarkStatusColors else LightStatusColors
    val view = LocalView.current

    if (!view.isInEditMode) {
        SideEffect {
            val window = (view.context as? Activity)?.window ?: return@SideEffect
            WindowCompat.getInsetsController(window, view).apply {
                isAppearanceLightStatusBars = !dark
                isAppearanceLightNavigationBars = !dark
            }
        }
    }

    androidx.compose.runtime.CompositionLocalProvider(LocalPortWeaveStatusColors provides statusColors) {
        MaterialTheme(
            colorScheme = colors,
            typography = PortWeaveTypography,
            shapes = PortWeaveShapes,
            content = content,
        )
    }
}
