package nl.tsai.kafka.dlq.boundary.rest

import nl.tsai.kafka.dlq.control.OrderRepository
import nl.tsai.kafka.dlq.control.model.Order
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux

@RestController
@RequestMapping("/orders")
class OrderController(val orderRepository: OrderRepository) {

    @GetMapping
    fun getAllOrders(): Flux<Order> {
        return orderRepository.findAll()
    }
}