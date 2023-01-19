package nl.tsai.kafka.dlq.test

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

@Configuration
class FakeMessageProducer {

    @Value("\${spring.kafka.bootstrap-servers}")
    lateinit var bootStrapServers: String

    @Bean
    fun stringProducerFactory(): ProducerFactory<Int, String> {
        return DefaultKafkaProducerFactory(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootStrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to IntegerSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            )
        )
    }

    @Bean
    fun stringMessageProducer(stringProducerFactory: ProducerFactory<Int, String>): KafkaTemplate<Int, String> {
        return KafkaTemplate(stringProducerFactory)
    }
}