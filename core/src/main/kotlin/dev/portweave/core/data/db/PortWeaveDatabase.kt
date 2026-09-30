package dev.portweave.core.data.db

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.migration.Migration
import androidx.sqlite.db.SupportSQLiteDatabase

@Database(
    entities = [
        TunnelConfigEntity::class,
        SshDetailEntity::class,
        StcpDetailEntity::class,
        TunnelEventEntity::class,
    ],
    version = 4,
    exportSchema = true,
)
abstract class PortWeaveDatabase : RoomDatabase() {
    abstract fun tunnelConfigDao(): TunnelConfigDao
    abstract fun tunnelEventDao(): TunnelEventDao

    companion object {
        fun create(context: Context): PortWeaveDatabase = Room.databaseBuilder(
            context.applicationContext,
            PortWeaveDatabase::class.java,
            "portweave.db",
        ).addMigrations(Migration1To2, Migration2To3, Migration3To4).build()

        internal val Migration1To2 = object : Migration(1, 2) {
            override fun migrate(db: SupportSQLiteDatabase) {
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `ssh_detail` (
                        `config_id` TEXT NOT NULL,
                        `dial_host` TEXT NOT NULL,
                        `dial_port` INTEGER NOT NULL,
                        `dependency_config_id` TEXT,
                        `logical_host` TEXT NOT NULL,
                        `logical_port` INTEGER NOT NULL,
                        `username` TEXT NOT NULL,
                        `auth_method` TEXT NOT NULL,
                        `password_ref` TEXT,
                        `private_key_ref` TEXT,
                        `passphrase_ref` TEXT,
                        `host_fingerprint_ref` TEXT,
                        `remote_target_host` TEXT NOT NULL,
                        `remote_target_port` INTEGER NOT NULL,
                        `host_key_policy` TEXT NOT NULL,
                        `connect_timeout_seconds` INTEGER NOT NULL,
                        `keep_alive_seconds` INTEGER NOT NULL,
                        PRIMARY KEY(`config_id`),
                        FOREIGN KEY(`config_id`) REFERENCES `tunnel_config`(`id`) ON UPDATE NO ACTION ON DELETE CASCADE
                    )
                    """.trimIndent(),
                )
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_ssh_detail_config_id` ON `ssh_detail` (`config_id`)")
            }
        }

        internal val Migration2To3 = object : Migration(2, 3) {
            override fun migrate(db: SupportSQLiteDatabase) {
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `stcp_detail` (
                        `config_id` TEXT NOT NULL,
                        `dial_host` TEXT NOT NULL,
                        `dial_port` INTEGER NOT NULL,
                        `dependency_config_id` TEXT,
                        `logical_host` TEXT NOT NULL,
                        `logical_port` INTEGER NOT NULL,
                        `token_ref` TEXT,
                        `user` TEXT,
                        `server_user` TEXT,
                        `server_name` TEXT NOT NULL,
                        `secret_key_ref` TEXT,
                        `wire_protocol` TEXT NOT NULL,
                        `use_encryption` INTEGER NOT NULL,
                        `use_compression` INTEGER NOT NULL,
                        `tls_leaf_pin_sha256` TEXT,
                        `connect_timeout_seconds` INTEGER NOT NULL,
                        PRIMARY KEY(`config_id`),
                        FOREIGN KEY(`config_id`) REFERENCES `tunnel_config`(`id`) ON UPDATE NO ACTION ON DELETE CASCADE
                    )
                    """.trimIndent(),
                )
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_stcp_detail_config_id` ON `stcp_detail` (`config_id`)")
            }
        }

        internal val Migration3To4 = object : Migration(3, 4) {
            override fun migrate(db: SupportSQLiteDatabase) {
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `tunnel_event` (
                        `id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        `config_id` TEXT NOT NULL,
                        `generation` INTEGER NOT NULL,
                        `level` TEXT NOT NULL,
                        `event_code` TEXT NOT NULL,
                        `message_key` TEXT NOT NULL,
                        `sanitized_args_json` TEXT NOT NULL,
                        `elapsed_realtime_millis` INTEGER NOT NULL,
                        `occurred_at` INTEGER NOT NULL,
                        `trace_id` TEXT NOT NULL
                    )
                    """.trimIndent(),
                )
                db.execSQL(
                    "CREATE INDEX IF NOT EXISTS `index_tunnel_event_config_id_occurred_at` " +
                        "ON `tunnel_event` (`config_id`, `occurred_at`)",
                )
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_tunnel_event_occurred_at` ON `tunnel_event` (`occurred_at`)")
            }
        }

        internal val AllMigrations: Array<Migration> = arrayOf(Migration1To2, Migration2To3, Migration3To4)
    }
}
