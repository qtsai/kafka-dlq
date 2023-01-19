package nl.tsai.kafka.dlq.control

import nl.tsai.kafka.dlq.control.error.OrderProcessingException
import nl.tsai.kafka.dlq.control.model.Order
import nl.tsai.kafka.dlq.control.util.mockingSomeDelayToCheckConcurrency
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import kotlin.random.Random

@Service
class OrderService(val orderRepository: OrderRepository) {

    /**
     * @throws OrderProcessingException when any error occurs
     */
    suspend fun processOrder(order: Order) {
        logger.info("Start processing order with id : ${order.id}")
        mockingSomeDelayToCheckConcurrency(2500)
        mockThrowException(order)
        logger.info("Finished processing order with id : ${order.id}")
        orderRepository.save(order)
    }

    fun mockThrowException(order: Order) {
        if (Random.nextBoolean()) {
            logger.info("Failed to process order with id ${order.id}")
            throw OrderProcessingException("Oops... something went wrong trying to send ${order.id}")
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(this::class.java)
    }

}