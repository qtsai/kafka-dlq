package nl.tsai.kafka.dlq.entity.persistence

import nl.tsai.kafka.dlq.control.OrderRepository
import nl.tsai.kafka.dlq.control.model.Order
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import java.util.concurrent.ConcurrentHashMap

@Repository
class InMemOrderRepository : OrderRepository {

    private val orders: ConcurrentHashMap<Long, Order> = ConcurrentHashMap()

    override fun save(order: Order) {
        logger.info("Saving order with id : ${order.id}")
        if (orders[order.id] == null) {
            orders[order.id] = order
        } else {
            logger.info("Order with id ${order.id} was already processed")
        }
    }

    override fun findAll(): Flux<Order> {
        return Flux.fromIterable(orders.values)
    }

    override fun removeAll() {
        orders.clear()
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(this::class.java)
    }
}