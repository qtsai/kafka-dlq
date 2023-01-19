package nl.tsai.kafka.dlq.control.util

import kotlinx.coroutines.delay

suspend fun mockingSomeDelayToCheckConcurrency(delay: Long = 2000) {
    delay(delay)
}