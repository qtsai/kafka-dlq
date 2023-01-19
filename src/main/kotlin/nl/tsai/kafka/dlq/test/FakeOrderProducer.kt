package nl.tsai.kafka.dlq.test

import nl.tsai.kafka.dlq.control.model.Order
import nl.tsai.kafka.dlq.control.util.mockingSomeDelayToCheckConcurrency
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class FakeOrderProducer(val orderKafkaTemplate: KafkaTemplate<Int, Order>,  val stringKafkaTemplate: KafkaTemplate<Int, String>) {

    @Value("\${kafka-dlq-service.order.topic.name}")
    lateinit var topicName: String

    suspend fun sendMessage(order: Order) {
        logger.info("Sending order with id : ${order.id}")
        mockingSomeDelayToCheckConcurrency()
        orderKafkaTemplate.send(topicName, order)
        logger.info("Sent order with id : ${order.id}")
    }

    suspend fun sendStringMessage(message: String) {
        logger.info("Sending message")
        stringKafkaTemplate.send(topicName, message)
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(this::class.java)
    }
}