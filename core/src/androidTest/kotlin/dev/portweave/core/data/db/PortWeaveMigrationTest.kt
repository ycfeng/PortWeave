package dev.portweave.core.data.db

import androidx.room.testing.MigrationTestHelper
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class PortWeaveMigrationTest {
    @get:Rule
    val helper = MigrationTestHelper(
        InstrumentationRegistry.getInstrumentation(),
        PortWeaveDatabase::class.java,
    )

    @Test fun migrate1ToLatest() = migrateFrom(1)
    @Test fun migrate2ToLatest() = migrateFrom(2)
    @Test fun migrate3ToLatest() = migrateFrom(3)

    private fun migrateFrom(version: Int) {
        helper.createDatabase(DatabaseName, version).close()
        helper.runMigrationsAndValidate(DatabaseName, 4, true, *PortWeaveDatabase.AllMigrations).use { database ->
            database.query("SELECT COUNT(*) FROM tunnel_event").use { cursor ->
                check(cursor.moveToFirst())
            }
        }
    }

    private companion object { const val DatabaseName = "migration-test" }
}
