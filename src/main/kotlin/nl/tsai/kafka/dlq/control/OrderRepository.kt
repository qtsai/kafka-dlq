package nl.tsai.kafka.dlq.control

import nl.tsai.kafka.dlq.control.model.Order
import reactor.core.publisher.Flux

interface OrderRepository {
    fun save(order: Order)
    fun findAll(): Flux<Order>
    fun removeAll()
}