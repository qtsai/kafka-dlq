package nl.tsai.kafka.dlq.config

import nl.tsai.kafka.dlq.control.model.Order
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConsumerRecordRecoverer
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.FixedBackOff


@Configuration
@EnableKafka
class KafkaConsumerConfig {

    @Value("\${kafka-dlq-service.order-dlq.topic.name}")
    lateinit var dlq: String

    @Bean
    fun orderConsumerFactory(defaultKafkaProperties: KafkaProperties): ConsumerFactory<Int, Order> {
        return DefaultKafkaConsumerFactory(
            defaultKafkaProperties.buildConsumerProperties().plus(
                mapOf(
                    ConsumerConfig.GROUP_ID_CONFIG to "groupId",
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "10",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
                    ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS to IntegerDeserializer::class.java,
                    ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS to JsonDeserializer::class.java
                )
            )
        )
    }

    @Bean
    fun orderKafkaListenerContainerFactory(
        orderConsumerFactory: ConsumerFactory<Int, Order>,
        orderKafkaTemplate: KafkaTemplate<Int, Order>
    ): ConcurrentKafkaListenerContainerFactory<Int, Order> {
        return ConcurrentKafkaListenerContainerFactory<Int, Order>().apply {
            consumerFactory = orderConsumerFactory
            isBatchListener = true
            val recoverer = DeadLetterPublishingRecoverer(orderKafkaTemplate) { rec, ex -> TopicPartition(dlq, 0) }
            setCommonErrorHandler(CustomErrorHandler(recoverer, FixedBackOff(3000, 2)))
        }
    }
}

class CustomErrorHandler(recoverer: ConsumerRecordRecoverer, backOff: BackOff) : DefaultErrorHandler(recoverer, backOff) {

    override fun handleOne(
        thrownException: Exception,
        record: ConsumerRecord<*, *>,
        consumer: Consumer<*, *>,
        container: MessageListenerContainer
    ): Boolean {
        logger.info("handlesOne")
        return super.handleOne(thrownException, record, consumer, container)
    }

    override fun handleBatch(
        thrownException: Exception,
        data: ConsumerRecords<*, *>,
        consumer: Consumer<*, *>,
        container: MessageListenerContainer,
        invokeListener: Runnable
    ) {
        logger.info("handlesBatch")
        super.handleBatch(thrownException, data, consumer, container, invokeListener)
    }
}