package org.apn.spark.sql.streaming

import java.util.{Map => JMap}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConversions._

class KafkaSink(createProducer: () => KafkaProducer[ String, String ]) extends Serializable {
  lazy private val producer = createProducer ( )
  def send(topic: String, value: String): Unit = producer.send ( new ProducerRecord ( topic, value ) )
}

object KafkaSink {

  private val defaultConfig = Map (
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer]
  )

  def apply(config: JMap[ String, Object ]): KafkaSink = {
    val finalConfig = defaultConfig ++ config
    val f = () => {
      val producer = new KafkaProducer[ String, String ]( finalConfig )

      sys.addShutdownHook {
        producer.close ( )
      }
      producer
    }
    new KafkaSink ( f )
  }
}
