package dev.portweave.app.service

import android.app.NotificationManager
import android.content.Context
import android.content.Intent
import android.os.SystemClock
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import dev.portweave.app.MainActivity
import dev.portweave.app.PortWeaveApplication
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class ForegroundServiceReleaseQualityTest {
    @Test
    fun failedStartRemovesDesiredRootAndForegroundNotification() {
        val instrumentation = InstrumentationRegistry.getInstrumentation()
        val context = ApplicationProvider.getApplicationContext<Context>()
        val application = context as PortWeaveApplication
        val notifications = context.getSystemService(NotificationManager::class.java)
        notifications.cancelAll()
        runBlocking { application.container.desiredRuntimeStore.clear() }

        val activity = instrumentation.startActivitySync(
            Intent(context, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK),
        )
        try {
            TunnelForegroundService.start(context, UUID.randomUUID())
            val deadline = SystemClock.elapsedRealtime() + TimeoutMillis
            var clean = false
            while (SystemClock.elapsedRealtime() < deadline) {
                val desiredEmpty = runBlocking {
                    application.container.desiredRuntimeStore.snapshot().isEmpty()
                }
                val notificationRemoved = notifications.activeNotifications.isEmpty()
                if (desiredEmpty && notificationRemoved) {
                    clean = true
                    break
                }
                SystemClock.sleep(PollMillis)
            }
            assertTrue("Failed start left a desired root or foreground notification", clean)
        } finally {
            activity.finish()
        }
    }

    private companion object {
        const val TimeoutMillis = 10_000L
        const val PollMillis = 50L
    }
}
