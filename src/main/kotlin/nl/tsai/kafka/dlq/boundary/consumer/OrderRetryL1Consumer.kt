package nl.tsai.kafka.dlq.boundary.consumer

import kotlinx.coroutines.runBlocking
import nl.tsai.kafka.dlq.boundary.producer.OrderDlqProducer
import nl.tsai.kafka.dlq.control.OrderService
import nl.tsai.kafka.dlq.control.error.OrderProcessingException
import nl.tsai.kafka.dlq.control.model.Order
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class OrderRetryL1Consumer(
    val orderService: OrderService,
    val orderDlqProducer: OrderDlqProducer
) {

    @KafkaListener(
        id = "\${kafka-dlq-service.order-retry.id}",
        topics = ["\${kafka-dlq-service.order-retry.topic.name}"],
        containerFactory = "orderKafkaListenerContainerFactory"
    )
    fun handleRetryOrderMessage(order: Order) {
        logger.info("Received order to retry : ${order.id}")
        runBlocking {
            try {
                orderService.processOrder(order)
            } catch (e: OrderProcessingException) {
                orderDlqProducer.sendMessage(order)
            }
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(this::class.java)
    }

}