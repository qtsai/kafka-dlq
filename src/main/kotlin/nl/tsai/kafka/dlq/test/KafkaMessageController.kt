package nl.tsai.kafka.dlq.test

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import nl.tsai.kafka.dlq.control.model.Order
import nl.tsai.kafka.dlq.control.model.Product
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import java.util.*

@RestController
@RequestMapping("/message")
class KafkaMessageController(val fakeOrderProducer: FakeOrderProducer) {

    @PostMapping
    fun sendMessage(@RequestBody message: String, @RequestParam(defaultValue = "5") count: Int) {
        logger.info("Received message : $message")
        runBlocking {
            val tasks: MutableList<Deferred<Unit>> = mutableListOf()

            repeat(count) {
                tasks.add(async {
                    fakeOrderProducer.sendMessage(createRandomOrder(message))
                })
            }
            awaitAll(*tasks.toTypedArray())

            fakeOrderProducer.sendStringMessage(message)
        }
    }

    @PostMapping("/order")
    fun sendOrder(@RequestBody order: Order) {
        logger.info("Received order $order")
        runBlocking {
            fakeOrderProducer.sendMessage(order)
        }
    }

    private fun createRandomOrder(message: String) = Order(
        message = message,
        products = listOf(
            createRandomProduct(),
            createRandomProduct()
        )
    )

    private fun createRandomProduct() = Product(Random().nextLong(9999L), Random().nextInt())

    companion object {
        val logger: Logger = LoggerFactory.getLogger(this::class.java)
    }
}
