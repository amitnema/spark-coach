package org.apn.spark.sql.streaming

import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.wjoel.spark.streaming.wikiedits.{WikipediaEditEvent, WikipediaEditReceiver}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
  *
  * Problem : Wikipedia provides an IRC channel where all edits to the wiki are logged.
  * Read this channel and count the number of bytes per user per minute.
  * <p>
  * Reads the message from Wikipedia channel and does the analysis for the bytes editeds per user per minute.
  * </p>
  * <p>
  * Here, we are using third party library to consume the wikipedia and publishing to Kafka and then consuming kafka streaming using spark structured stream.
  * </p>
  * <pre>
  * Usage: WikipediaAnalysis &lt;bootstrap-servers&gt; &lt;subscribe-type&gt; &lt;topics&gt;
  * [&lt;checkpoint-location&gt;]
  * &lt;bootstrap-servers&gt; The Kafka "bootstrap.servers" configuration. A
  * comma-separated list of host:port.
  * &lt;subscribe-type&gt; There are three kinds of type, i.e. 'assign', 'subscribe',
  * 'subscribePattern'.
  * |- &lt;assign&gt; Specific TopicPartitions to consume. Json string
  * |  {"topicA":[0,1],"topicB":[2,4]}.
  * |- &lt;subscribe&gt; The topic list to subscribe. A comma-separated list of
  * |  topics.
  * |- &lt;subscribePattern&gt; The pattern used to subscribe to topic(s).
  * |  Java regex string.
  * |- Only one of "assign, "subscribe" or "subscribePattern" options can be
  * |  specified for Kafka source.
  * &lt;topics&gt; Different value format depends on the value of 'subscribe-type'.
  * &lt;checkpoint-location&gt; Directory in which to create checkpoints. If not
  * provided, defaults to a randomized directory in /tmp.
  *
  * Example:
  * `$ spark-submit \
  * --class org.apn.spark.sql.streaming.WikipediaAnalysis \
  * --master local[*] \
  * spark-coach-<version>-SNAPSHOT-jar-with-dependencies.jar \
  * host1:port1,host2:port2 \
  * subscribe topic1,topic2`
  * </pre>
  *
  * @author Amit Nema
  */
object WikipediaAnalysis {

  def main(args: Array[ String ]): Unit = {

    if (args.length < 3) {
      System.err.println ( "Usage: WikipediaAnalysis <bootstrap-servers> " +
        "<subscribe-type> <topics> [<checkpoint-location>]" )
      System.exit ( 1 )
    }

    val Array ( bootstrapServers, subscribeType, topics, _* ) = args
    val checkpointLocation =
      if (args.length > 3) args ( 3 ) else "/tmp/temporary-" + UUID.randomUUID.toString


    val spark = SparkSession
      .builder
      .appName ( this.getClass.getName )
      .getOrCreate

    val props = Map (
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers
    )

    produceToKafka ( spark, props, topics ).start
    analyseWikipedia ( spark, props, subscribeType, topics, checkpointLocation )
    spark.streams.awaitAnyTermination ( )
  }

  def analyseWikipedia(spark: SparkSession, props: Map[ String, String ], subscribeType: String, topic: String, checkpointLocation: String): Unit = {
    import spark.implicits._
    val schema = ScalaReflection.schemaFor [ WikiEdits ].dataType.asInstanceOf [ StructType ]
    val kafkaParams = props.map { case (k, v) => ("kafka." + k, v) }.+ ( subscribeType -> topic ).+ ( "startingOffsets" -> "latest" )

    // Create DataSet representing the stream of input lines from kafka
    spark
      .readStream
      .format ( "kafka" ).options ( kafkaParams )
      .load ( )
      .select ( $"value".cast ( "string" ) )
      .select ( from_json ( col ( "value" ), schema ) as "json" )
      .select ( "json.*" )
      .as [ WikiEdits ]
      .filter { editEvent =>
        !editEvent.title.contains ( ":" )
      }
      // Group the data by window and title and compute the count of each group
      .groupBy ( window ( ($"timestamp" / 1000).cast ( TimestampType ), "1 minutes" ), $"title" )
      .sum ( "byteDiff" )
      .orderBy ( abs ( $"title" ).desc )
      .writeStream
      .outputMode ( OutputMode.Complete )
      .format ( "console" )
      .option ( "truncate", value = false )
      .option ( "checkpointLocation", checkpointLocation )
      .start ( )
  }

  private def produceToKafka(spark: SparkSession, props: Map[ String, String ], topic: String) = {
    implicit val encoder: Encoder[ WikipediaEditEvent ] = org.apache.spark.sql.Encoders.bean ( classOf [ WikipediaEditEvent ] )
    val ssc = new StreamingContext ( spark.sparkContext, Seconds ( 5 ) )
    val kafkaSink = spark.sparkContext.broadcast ( KafkaSink ( props ) )

    ssc.receiverStream ( new WikipediaEditReceiver )
      .foreachRDD {
        _.foreach { message =>
          kafkaSink.value.send ( topic, new ObjectMapper ( ).writeValueAsString ( message ) )
        }
      }
    ssc
  }
}

case class WikiEdits(botEdit: Boolean, byteDiff: Long, channel: String, diffUrl: String,
                     minor: Boolean, /*`new`: Boolean,*/ special: Boolean, state: Array[ String ],
                     summary: String, talk: Boolean, timestamp: Long, title: String,
                     unPatrolled: Boolean, user: String)