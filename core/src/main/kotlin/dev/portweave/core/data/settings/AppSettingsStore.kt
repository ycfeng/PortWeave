package dev.portweave.core.data.settings

import android.content.Context
import androidx.datastore.preferences.core.booleanPreferencesKey
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.intPreferencesKey
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

enum class ThemeMode {
    SYSTEM,
    LIGHT,
    DARK,
}

data class AppSettings(
    val themeMode: ThemeMode = ThemeMode.SYSTEM,
    val dynamicColor: Boolean = false,
    val logRetentionDays: Int = 7,
)

interface SettingsStore {
    val settings: Flow<AppSettings>
    suspend fun setThemeMode(mode: ThemeMode)
    suspend fun setDynamicColor(enabled: Boolean)
    suspend fun setLogRetentionDays(days: Int)
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

    private companion object {
        val ThemeModeKey = stringPreferencesKey("theme_mode")
        val DynamicColorKey = booleanPreferencesKey("dynamic_color")
        val LogRetentionDaysKey = intPreferencesKey("log_retention_days")
    }
}
