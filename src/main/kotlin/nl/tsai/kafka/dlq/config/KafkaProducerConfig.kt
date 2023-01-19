package nl.tsai.kafka.dlq.config

import nl.tsai.kafka.dlq.control.model.Order
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.IntegerSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
class KafkaProducerConfig {

    @Bean
    fun orderProducerFactory(defaultKafkaProperties: KafkaProperties): ProducerFactory<Int, Order> {
        return DefaultKafkaProducerFactory(
            defaultKafkaProperties.buildProducerProperties().plus(
                mapOf(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to IntegerSerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java,
                )
            )
        )
    }

    @Bean
    fun orderKafkaTemplate(orderProducerFactory: ProducerFactory<Int, Order>): KafkaTemplate<Int, Order> {
        return KafkaTemplate(orderProducerFactory)
    }
}