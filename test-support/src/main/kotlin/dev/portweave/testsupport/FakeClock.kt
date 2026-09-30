package dev.portweave.testsupport

fun interface FakeClock {
    fun nowEpochMillis(): Long
}
