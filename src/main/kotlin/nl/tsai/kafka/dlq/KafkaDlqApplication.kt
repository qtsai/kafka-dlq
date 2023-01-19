package nl.tsai.kafka.dlq

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaDlqApplication

fun main(args: Array<String>) {
	runApplication<KafkaDlqApplication>(*args)
}
