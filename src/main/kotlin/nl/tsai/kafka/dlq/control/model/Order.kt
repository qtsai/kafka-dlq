package nl.tsai.kafka.dlq.control.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonSetter
import java.io.Serializable
import kotlin.random.Random

@JsonIgnoreProperties(ignoreUnknown = true)
data class Order(
    @JsonSetter("id") val id: Long = Random.nextLong(99999),
    @JsonSetter("message") val message: String,
    @JsonSetter("products") val products: List<Product>
) : Serializable

@JsonIgnoreProperties(ignoreUnknown = true)
data class Product(
    @JsonSetter("id") val id: Long,
    @JsonSetter("quantity") val quantity: Int
) : Serializable
