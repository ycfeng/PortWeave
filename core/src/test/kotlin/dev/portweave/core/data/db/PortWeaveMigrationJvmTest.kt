package dev.portweave.core.data.db

import androidx.room.Room
import androidx.sqlite.db.SupportSQLiteDatabase
import androidx.sqlite.db.SupportSQLiteOpenHelper
import androidx.sqlite.db.framework.FrameworkSQLiteOpenHelperFactory
import androidx.test.core.app.ApplicationProvider
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner

@RunWith(RobolectricTestRunner::class)
class PortWeaveMigrationJvmTest {
    private val context get() = ApplicationProvider.getApplicationContext<android.content.Context>()

    @Test
    fun allPublishedSchemasMigrateToVersionFour() {
        (1..3).forEach { version ->
            val name = "migration-jvm-$version"
            context.deleteDatabase(name)
            createDatabaseFromExportedSchema(name, version)

            val room = Room.databaseBuilder(context, PortWeaveDatabase::class.java, name)
                .addMigrations(*PortWeaveDatabase.AllMigrations)
                .allowMainThreadQueries()
                .build()
            try {
                    room.openHelper.writableDatabase.query("SELECT COUNT(*) FROM tunnel_event").use { cursor ->
                        check(cursor.moveToFirst())
                    }
            } finally {
                room.close()
            }
            context.deleteDatabase(name)
        }
    }

    private fun createDatabaseFromExportedSchema(name: String, version: Int) {
        val path = "dev.portweave.core.data.db.PortWeaveDatabase/$version.json"
        val root = context.assets.open(path).bufferedReader().use { Json.parseToJsonElement(it.readText()).jsonObject }
        val database = root.getValue("database").jsonObject
        val entities = database.getValue("entities").jsonArray.map { it.jsonObject }
        val setup = database.getValue("setupQueries").jsonArray.map { it.jsonPrimitive.content }
        val callback = object : SupportSQLiteOpenHelper.Callback(version) {
            override fun onCreate(db: SupportSQLiteDatabase) {
                entities.forEach { entity ->
                    val table = entity.getValue("tableName").jsonPrimitive.content
                    val sql = entity.getValue("createSql").jsonPrimitive.content.replace("${'$'}{TABLE_NAME}", table)
                    db.execSQL(sql)
                    entity["indices"]?.jsonArray.orEmpty().forEach { index ->
                        val indexSql = index.jsonObject.getValue("createSql").jsonPrimitive.content
                            .replace("${'$'}{TABLE_NAME}", table)
                        db.execSQL(indexSql)
                    }
                }
                setup.forEach(db::execSQL)
            }

            override fun onUpgrade(db: SupportSQLiteDatabase, oldVersion: Int, newVersion: Int) = Unit
        }
        FrameworkSQLiteOpenHelperFactory().create(
            SupportSQLiteOpenHelper.Configuration.builder(context).name(name).callback(callback).build(),
        ).use { it.writableDatabase }
    }
}
