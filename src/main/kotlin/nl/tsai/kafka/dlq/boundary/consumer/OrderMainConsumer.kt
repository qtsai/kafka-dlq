package nl.tsai.kafka.dlq.boundary.consumer

import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import nl.tsai.kafka.dlq.boundary.producer.OrderRetryL1Producer
import nl.tsai.kafka.dlq.control.OrderService
import nl.tsai.kafka.dlq.control.error.OrderProcessingException
import nl.tsai.kafka.dlq.control.model.Order
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.*

@Component
class OrderMainConsumer(
    val orderService: OrderService,
    val orderRetryL1Producer: OrderRetryL1Producer
) {

    @KafkaListener(
        id = "\${kafka-dlq-service.order.id}",
        topics = ["\${kafka-dlq-service.order.topic.name}"],
        containerFactory = "orderKafkaListenerContainerFactory"
    )
    fun handleOrderMessage(orders: List<Order>) {
        val startTime = System.currentTimeMillis()
        val batchId = UUID.randomUUID()

        logger.info("Received batch $batchId with ${orders.size} orders")
        runBlocking {
            orders.map { order ->
                async {
                    logger.info("Received order : ${order.id}")
                    try {
                        orderService.processOrder(order)
                    } catch (e: OrderProcessingException) {
                        orderRetryL1Producer.sendMessage(order)
                    }
                }
            }.map {
                it.await()
            }
        }
        logger.info("Finished processing batch $batchId in ${System.currentTimeMillis() - startTime}ms")

    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(this::class.java)
    }

}