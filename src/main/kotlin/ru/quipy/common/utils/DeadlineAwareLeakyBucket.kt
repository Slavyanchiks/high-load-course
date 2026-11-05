package ru.quipy.common.utils

import java.time.Duration

class DeadlineAwareLeakyBucket(
    private val rate: Int,
    window: Duration,
    processingTime: Duration,
) {
    private val windowMs = window.toMillis()
    private val processingMs = processingTime.toMillis()

    private val startLog = LongArray(rate)
    private var head = 0

    private var waiting = 0

    @Synchronized
    fun tryReserve(deadline: Long): Long? {
        val now = System.currentTimeMillis()
        val finishAt = expectedStartAt(waiting, now) + processingMs
        if (finishAt > deadline) return now + (finishAt - deadline)
        waiting++
        return null
    }

    fun awaitSlot(): Long {
        val sleepMs = synchronized(this) { startLog[head] + windowMs - System.currentTimeMillis() }
        if (sleepMs > 0) Thread.sleep(sleepMs)

        synchronized(this) {
            val now = System.currentTimeMillis()
            startLog[head] = now
            head = (head + 1) % rate
            waiting--
            return now
        }
    }

    @Synchronized
    fun waiting() = waiting

    private fun expectedStartAt(position: Int, now: Long): Long {
        val slot = startLog[(head + position % rate) % rate].coerceAtLeast(now - windowMs)
        return maxOf(now, slot + windowMs * (position / rate + 1))
    }
}
