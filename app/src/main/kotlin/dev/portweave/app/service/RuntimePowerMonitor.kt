package dev.portweave.app.service

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.PowerManager
import androidx.core.content.ContextCompat
import dev.portweave.tunnel.engine.RuntimeWorkPolicy

/** Owned by the foreground service; no polling and no wake locks. */
class RuntimePowerMonitor(private val context: Context, private val policy: RuntimeWorkPolicy) : AutoCloseable {
    private val power = context.getSystemService(PowerManager::class.java)
    private val receiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context, intent: Intent) = refresh()
    }

    init {
        ContextCompat.registerReceiver(
            context,
            receiver,
            IntentFilter().apply {
                addAction(Intent.ACTION_SCREEN_ON)
                addAction(Intent.ACTION_SCREEN_OFF)
                addAction(PowerManager.ACTION_DEVICE_IDLE_MODE_CHANGED)
                addAction(PowerManager.ACTION_POWER_SAVE_MODE_CHANGED)
            },
            ContextCompat.RECEIVER_NOT_EXPORTED,
        )
        refresh()
    }

    private fun refresh() {
        policy.interactive.value = power.isInteractive
        policy.deviceIdle.value = power.isDeviceIdleMode
        policy.powerSave.value = power.isPowerSaveMode
    }

    override fun close() { context.unregisterReceiver(receiver) }
}
