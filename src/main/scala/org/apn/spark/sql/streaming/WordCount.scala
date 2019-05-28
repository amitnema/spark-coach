package org.apn.spark.sql.streaming

import java.io.PrintWriter
import java.util.UUID

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

object WordCount {

  type WordCountHandler = (Dataset[ WordCount ], Long) => Unit

  def main(args: Array[ String ]): Unit = {
    if (args.length < 3) {
      System.err.println ( "Usage: StructuredKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics> [<checkpoint-location>]" )
      System.exit ( 1 )
    }

    val Array ( bootstrapServers, subscribeType, topics, _* ) = args
    val checkpointLocation =
      if (args.length > 3) args ( 3 ) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName ( "StructuredKafkaWordCount" )
      .getOrCreate ( )

    var results = ListBuffer.empty [ Array[ WordCount ] ]

    countWordKafka ( spark, bootstrapServers, subscribeType, topics, checkpointLocation ) { (wordsCount: Dataset[ WordCount ], _: Long) =>
      results += wordsCount.collect ( )
      wordsCount.foreach ( x => println ( x.word, x.wc ) )
    }

    // Create DataSet representing the stream of input lines from kafka
   /* val lines = spark
      .readStream
      .format ( "kafka" )
      .option ( "kafka.bootstrap.servers", bootstrapServers )
      .option ( subscribeType, topics )
      .load ( )
      .selectExpr ( "CAST(value AS STRING)" )
      .as [ String ]

    lines.createOrReplaceTempView ( "stream_dataset" )
    val delim = StringUtils.SPACE
    val wordCounts = spark.sql ( s"SELECT word, count(1) AS wc FROM (select explode(split(value, '$delim')) as word from stream_dataset) group by word order by word" )

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode ( "complete" )
      .format ( "console" )
      .option ( "checkpointLocation", checkpointLocation )
      .start ( )

    query.awaitTermination ( )*/
  }

  def countWordSave(spark: SparkSession, pathOut: String, bootstrapServers: String, subscribeType: String, topics: String, checkpointLocation: String, delim: String = StringUtils.SPACE)(handler: WordCountHandler): Unit = {
    var results = ListBuffer.empty [ Array[ WordCount ] ]
    countWordKafka ( spark, bootstrapServers, subscribeType, topics, checkpointLocation, delim ) { (wordsCount: Dataset[ WordCount ], _: Long) =>
      results += wordsCount.collect ( )
    }
    new PrintWriter ( pathOut ) {
      write ( results.mkString )
      close ( )
    }
  }

  def countWordKafka(spark: SparkSession, bootstrapServers: String, subscribeType: String, topics: String, checkpointLocation: String, delim: String = StringUtils.SPACE)(handler: WordCountHandler): Unit = {
    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format ( "kafka" )
      .option ( "kafka.bootstrap.servers", bootstrapServers )
      .option ( subscribeType, topics )
      .load ( )
      .selectExpr ( "CAST(value AS STRING)" )
      .as [ String ]

    lines.createOrReplaceTempView ( "stream_dataset" )
    val wordCounts = spark.sql ( s"SELECT word, count(1) AS wc FROM (select explode(split(value, '$delim')) as word from stream_dataset) group by word order by word" ).as[WordCount]

    // Start running the query that prints the running counts to the console
    /*val query = wordCounts.writeStream
      .outputMode ( "complete" )
      .format ( "console" )
      .option ( "checkpointLocation", checkpointLocation )
      .start ( )*/


    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
            .outputMode ( "complete" )
      //      .format ( "console" )
      .option ( "checkpointLocation", checkpointLocation )
      .foreachBatch ( (batchDF: Dataset[ WordCount ], batchId: Long) => {
        handler ( batchDF.sort ( $"word" ), batchId )
      } )
      .start ( )

    query.awaitTermination ( )


  }
}

case class WordCount(word: String, wc: BigInt)
