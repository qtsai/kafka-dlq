package nl.tsai.kafka.dlq.config

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.DescribeClusterOptions
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.health.AbstractHealthIndicator
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.TimeUnit

@Configuration
class KafkaHealthConfig {

    private var counter: Int = 0

    @Bean
    fun test1KafkaAdminClient(defaultKafkaProperties: KafkaProperties): KafkaAdminClient {
        return AdminClient.create(defaultKafkaProperties.buildAdminProperties()) as KafkaAdminClient
    }

    @Bean
    fun test2KafkaAdminClient(defaultKafkaProperties: KafkaProperties): KafkaAdminClient { // same for now for testing purposes, could have different ssl context
        return AdminClient.create(defaultKafkaProperties.buildAdminProperties()) as KafkaAdminClient
    }

    @Bean
    fun kafkaHealthIndicator(
        test1KafkaAdminClient: KafkaAdminClient,
        test2KafkaAdminClient: KafkaAdminClient,
        @Value("\${test.message:nope}") testMessage: String // used to test configmap
    ): HealthIndicator {
        return object : AbstractHealthIndicator() {
            override fun doHealthCheck(builder: Health.Builder) {
                val status1 = checkFor("test1KafkaAdminClient", test1KafkaAdminClient, builder)
                val status2 = checkFor("test2KafkaAdminClient", test2KafkaAdminClient, builder)
                builder.withDetail("count", counter)
                if (counter <= 50 && status1 && status2) {
                    builder.up()
                } else {
                    builder.down()
                }
                counter++
            }
        }
    }

    fun checkFor(key: String, kafkaAdminClient: KafkaAdminClient, builder: Health.Builder): Boolean {
        return try {
            kafkaAdminClient.describeCluster(DescribeClusterOptions().timeoutMs(5000)).nodes().get(5000, TimeUnit.MILLISECONDS)
            builder.withDetail(key, "UP")
            true
        } catch (e: Exception) {
            builder.withDetail(key, "DOWN").withDetail("$key - error", "${e.javaClass.name}: ${e.message}")
            logger.error("KafkaHealthCheck Failed with exception: ${e.message}")
            false
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(this::class.java)
    }
}