package dev.portweave.core.data.settings

import android.content.Context
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.stringSetPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map

interface DesiredRuntimeStore {
    val desiredConfigIds: Flow<Set<UUID>>
    suspend fun add(id: UUID)
    suspend fun remove(id: UUID)
    suspend fun clear()
    suspend fun snapshot(): Set<UUID> = desiredConfigIds.first()
}

private val Context.desiredRuntimeDataStore by preferencesDataStore(name = "desired_runtime")

class DataStoreDesiredRuntimeStore(private val context: Context) : DesiredRuntimeStore {
    override val desiredConfigIds: Flow<Set<UUID>> = context.desiredRuntimeDataStore.data.map { values ->
        values[DesiredIds].orEmpty().mapNotNullTo(mutableSetOf()) { value ->
            runCatching { UUID.fromString(value) }.getOrNull()
        }
    }

    override suspend fun add(id: UUID) {
        context.desiredRuntimeDataStore.edit { values ->
            values[DesiredIds] = values[DesiredIds].orEmpty() + id.toString()
        }
    }

    override suspend fun remove(id: UUID) {
        context.desiredRuntimeDataStore.edit { values ->
            values[DesiredIds] = values[DesiredIds].orEmpty() - id.toString()
        }
    }

    override suspend fun clear() {
        context.desiredRuntimeDataStore.edit { it.remove(DesiredIds) }
    }

    private companion object {
        val DesiredIds = stringSetPreferencesKey("desired_config_ids")
    }
}
