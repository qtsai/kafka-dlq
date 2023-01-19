package nl.tsai.kafka.dlq.boundary.producer

import nl.tsai.kafka.dlq.control.model.Order
import nl.tsai.kafka.dlq.control.util.mockingSomeDelayToCheckConcurrency
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class OrderRetryL1Producer(val orderKafkaTemplate: KafkaTemplate<Int, Order>) {

    @Value("\${kafka-dlq-service.order-retry.topic.name}")
    lateinit var topicName: String

    suspend fun sendMessage(order: Order) {
        logger.info("Sending order with id : ${order.id} to $topicName topic")
        mockingSomeDelayToCheckConcurrency()
        orderKafkaTemplate.send(topicName, order)
        logger.info("Sent order with id : ${order.id} to $topicName topic")
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(this::class.java)
    }
}