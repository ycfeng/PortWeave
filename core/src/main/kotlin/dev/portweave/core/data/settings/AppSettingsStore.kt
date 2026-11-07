package dev.portweave.core.data.settings

import android.content.Context
import androidx.datastore.preferences.core.booleanPreferencesKey
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.intPreferencesKey
import androidx.datastore.preferences.core.longPreferencesKey
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

enum class ThemeMode {
    SYSTEM,
    LIGHT,
    DARK,
}

/**
 * How hard background tunnels trade responsiveness for battery. [REALTIME] keeps configured keepalives and never
 * pauses retries while the screen is off; [BALANCED] adapts keepalives; [SAVER] also pauses retries sooner and lets
 * idle SSH tunnels drop their session until a client connects again.
 */
enum class PowerProfile {
    REALTIME,
    BALANCED,
    SAVER,
}

data class AppSettings(
    val themeMode: ThemeMode = ThemeMode.SYSTEM,
    val dynamicColor: Boolean = false,
    val logRetentionDays: Int = 7,
    val powerProfile: PowerProfile = PowerProfile.BALANCED,
    /** Timestamp of the last process exit already shown in the background-restriction hint. */
    val acknowledgedExitTimestamp: Long = 0L,
)

interface SettingsStore {
    val settings: Flow<AppSettings>
    suspend fun setThemeMode(mode: ThemeMode)
    suspend fun setDynamicColor(enabled: Boolean)
    suspend fun setLogRetentionDays(days: Int)
    suspend fun setPowerProfile(profile: PowerProfile) = Unit
    suspend fun acknowledgeExit(timestampMillis: Long) = Unit
}

private val Context.dataStore by preferencesDataStore(name = "app_settings")

class DataStoreSettingsStore(
    private val context: Context,
) : SettingsStore {
    override val settings: Flow<AppSettings> = context.dataStore.data.map { values ->
        AppSettings(
            themeMode = values[ThemeModeKey]?.let { stored ->
                ThemeMode.entries.firstOrNull { it.name == stored }
            } ?: ThemeMode.SYSTEM,
            dynamicColor = values[DynamicColorKey] ?: false,
            logRetentionDays = (values[LogRetentionDaysKey] ?: 7).coerceIn(1, 90),
            powerProfile = values[PowerProfileKey]?.let { stored ->
                PowerProfile.entries.firstOrNull { it.name == stored }
            } ?: PowerProfile.BALANCED,
            acknowledgedExitTimestamp = values[AcknowledgedExitKey] ?: 0L,
        )
    }

    override suspend fun setThemeMode(mode: ThemeMode) {
        context.dataStore.edit { it[ThemeModeKey] = mode.name }
    }

    override suspend fun setDynamicColor(enabled: Boolean) {
        context.dataStore.edit { it[DynamicColorKey] = enabled }
    }

    override suspend fun setLogRetentionDays(days: Int) {
        context.dataStore.edit { it[LogRetentionDaysKey] = days.coerceIn(1, 90) }
    }

    override suspend fun setPowerProfile(profile: PowerProfile) {
        context.dataStore.edit { it[PowerProfileKey] = profile.name }
    }

    override suspend fun acknowledgeExit(timestampMillis: Long) {
        context.dataStore.edit { it[AcknowledgedExitKey] = maxOf(it[AcknowledgedExitKey] ?: 0L, timestampMillis) }
    }

    private companion object {
        val PowerProfileKey = stringPreferencesKey("power_profile")
        val AcknowledgedExitKey = longPreferencesKey("acknowledged_exit_timestamp")
        val ThemeModeKey = stringPreferencesKey("theme_mode")
        val DynamicColorKey = booleanPreferencesKey("dynamic_color")
        val LogRetentionDaysKey = intPreferencesKey("log_retention_days")
    }
}
