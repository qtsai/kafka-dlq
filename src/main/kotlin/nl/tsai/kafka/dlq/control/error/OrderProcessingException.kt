package nl.tsai.kafka.dlq.control.error

class OrderProcessingException(message: String) : RuntimeException(message) {
}