package org.apn.spark.structured

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{Ignore, WordSpec}

/**
  * @author amit.nema
  */
@Ignore
class WordCountTest extends WordSpec with EmbeddedKafka {

  "runs with embedded kafka" should {
    "work0" in {
      val userDefinedConfig = EmbeddedKafkaConfig( kafkaPort = 0, zooKeeperPort = 0 )

      withRunningKafkaOnFoundPort( userDefinedConfig ) { implicit actualConfig =>
        // now a kafka broker is listening on actualConfig.kafkaPort
        publishStringMessageToKafka( "topic", "message" )
        println(">>"+consumeFirstStringMessageFrom( "topic" ))
//        consumeFirstStringMessageFrom( "topic" ) shouldBe "message"
      }
    }

    "work1" in {
      EmbeddedKafka.start( )
      publishStringMessageToKafka( "topic", "message01" )
      println(">>"+consumeFirstStringMessageFrom( "topic" ))
      // ... code goes here

      EmbeddedKafka.stop( )
    }

  }
}