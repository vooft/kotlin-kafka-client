
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

fun main() {
    val producerProperties = Properties()
    producerProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    producerProperties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    producerProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

    val producer = KafkaProducer<String, String>(producerProperties)

    Thread.sleep(Long.MAX_VALUE)
}
