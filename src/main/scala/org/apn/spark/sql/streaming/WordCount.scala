package org.apn.spark.sql.streaming

import java.util.UUID

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.streaming.OutputMode.{Append, Complete}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  *
  * @author Amit Nema
  */
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
      .appName ( getClass.getName )
      .getOrCreate ( )

    countWordConsole ( spark, bootstrapServers, subscribeType, topics, checkpointLocation )

    /*countWordBatch ( spark, bootstrapServers, subscribeType, topics, checkpointLocation ) {
      (wordsCount: Dataset[ WordCount ], _: Long) =>
        wordsCount.foreach ( x => println ( x.word, x.wc ) )
    }*/

    //countWordSave ( spark, bootstrapServers, subscribeType, topics, checkpointLocation, "spark-data" )
    //spark.streams.awaitAnyTermination()

  }

  def countWordConsole(spark: SparkSession, bootstrapServers: String, subscribeType: String, topics: String, checkpointLocation: String, delim: String = StringUtils.SPACE): Unit = {

    // Start running the query that sends the running counts to the handler
    val query = countWord ( spark, bootstrapServers, subscribeType, topics, delim ).writeStream
      .outputMode ( Complete ( ) )
      .option ( "checkpointLocation", checkpointLocation )
      .option ( "truncate", false )
      .format ( "console" )
      .start ( )

    query.awaitTermination ( )
  }

  private def countWord(spark: SparkSession, bootstrapServers: String, subscribeType: String, topics: String, delim: String = StringUtils.SPACE) = {
    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format ( "kafka" )
      .option ( "kafka.bootstrap.servers", bootstrapServers )
      .option ( subscribeType, topics )
      .load ( )
      .selectExpr ( "CAST(value AS STRING)", "CAST(timestamp AS timestamp)" )
      .withWatermark ( "timestamp", "30 seconds" )
      .as [ (String, String) ]

    lines.printSchema ( )
    lines.createOrReplaceTempView ( "stream_dataset" )

    spark.sql ( s"SELECT word, win, count(1) AS wc FROM (select explode(split(value, '$delim')) as word, window(timestamp, '1 minutes', '1 minutes') win from stream_dataset) group by word, win" ).as [ WordCount ]
  }

  def countWordSave(spark: SparkSession, bootstrapServers: String, subscribeType: String, topics: String, checkpointLocation: String, pathOut: String, delim: String = StringUtils.SPACE): Unit = {
    // Start running the query that sends the running counts to the handler
    //*** Data source json/csv does not support Complete output mode;
    //*** Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode;
    val query = countWord ( spark, bootstrapServers, subscribeType, topics, delim ).writeStream
      .outputMode ( Append )
      .option ( "checkpointLocation", checkpointLocation )
      .format ( "json" ) // can be "orc", "json", "csv", etc.
      .option ( "path", pathOut )
      .start ( )

    query.awaitTermination ( )
  }

  def countWordBatch(spark: SparkSession, bootstrapServers: String, subscribeType: String, topics: String, checkpointLocation: String, delim: String = StringUtils.SPACE)(handler: WordCountHandler): Unit = {
    import spark.implicits._

    // Start running the query that sends the running counts to the handler
    val query = countWord ( spark, bootstrapServers, subscribeType, topics, delim ).writeStream
      .outputMode ( Complete ( ) )
      .option ( "checkpointLocation", checkpointLocation )
      .foreachBatch ( (batchDF: Dataset[ WordCount ], batchId: Long) => {
        handler ( batchDF.sort ( $"word" ), batchId )
      } )
      .start ( )

    query.awaitTermination ( )
  }
}

case class WordCount(word: String, wc: BigInt)
