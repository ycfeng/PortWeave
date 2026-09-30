package dev.portweave.core.data.repository

import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import dev.portweave.core.data.db.PortWeaveDatabase
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner

@RunWith(RobolectricTestRunner::class)
class RoomConfigRepositoryTransactionTest {
    @Test
    fun batchImportRollsBackDeletesAndEarlierInsertsWhenLaterInsertFails() = runTest {
        val context = ApplicationProvider.getApplicationContext<android.content.Context>()
        val database = Room.inMemoryDatabaseBuilder(context, PortWeaveDatabase::class.java)
            .allowMainThreadQueries()
            .build()
        try {
            val repository = RoomConfigRepository(database.tunnelConfigDao(), nowEpochMillis = { 100L })
            val existing = config("existing", 10_001)
            repository.save(existing)
            database.openHelper.writableDatabase.execSQL(
                "CREATE TRIGGER reject_import BEFORE INSERT ON tunnel_config " +
                    "WHEN NEW.name = 'reject' BEGIN SELECT RAISE(ABORT, 'test failure'); END",
            )
            val first = config("first", 10_002)
            val reject = config("reject", 10_003)

            val failure = runCatching {
                repository.replaceBatch(listOf(first, reject), setOf(existing.id))
            }.exceptionOrNull()

            assertTrue(failure != null)
            assertEquals(existing.id, repository.find(existing.id)?.id)
            assertNull(repository.find(first.id))
            assertNull(repository.find(reject.id))
        } finally {
            database.close()
        }
    }
}

private fun config(name: String, port: Int): TunnelConfig =
    TunnelConfig.empty(TunnelKind.SSH, name, port, nowEpochMillis = 1L)
