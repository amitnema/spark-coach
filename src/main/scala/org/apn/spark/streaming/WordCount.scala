package org.apn.spark.streaming

import java.io.PrintWriter

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.collection.mutable.ListBuffer

/**
  * @author amit.nema
  */
object WordCount {

  type WordCountHandler = (RDD[ WordCount ], Time) => Unit

  def main(args: Array[ String ]) {
    if (args.length < 1) {
      System.err.println ( "Usage: WordCount <directory>" )
      System.exit ( 1 )
    }
    println ( "Directory:" + args ( 0 ) )
    val spark = SparkSession.builder ( ).appName ( getClass.getName ).getOrCreate ( )

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent a starvation scenario.
    // Create the context
    val ssc: StreamingContext = new StreamingContext ( spark.sparkContext, Seconds ( 1 ) )
    var results = ListBuffer.empty [ Array[ WordCount ] ]

    countWord ( ssc, args ( 0 ) ) { (wordsCount: RDD[ WordCount ], _: Time) =>
      results += wordsCount.collect ( )
      wordsCount.foreach ( x => println ( x.word, x.count ) )
    }
    ssc.start ( )
    ssc.awaitTermination ( )
  }

  def countWordSave(ssc: StreamingContext, pathIn: String, pathOut: String, delim: String = StringUtils.SPACE)(handler: WordCountHandler): Unit = {
    var results = ListBuffer.empty [ Array[ WordCount ] ]
    countWord ( ssc, pathIn, delim ) { (wordsCount: RDD[ WordCount ], _: Time) =>
      results += wordsCount.collect ( )
    }
    new PrintWriter ( pathOut ) {
      write ( results.mkString )
      close ( )
    }
  }

  def countWord(ssc: StreamingContext, pathIn: String, delim: String = StringUtils.SPACE)(handler: WordCountHandler): Unit = {
    ssc.textFileStream ( pathIn )
      .flatMap ( _.split ( delim ) )
      .map ( (_, 1) )
      .reduceByKey ( _ + _ )
      .map ( x => WordCount ( x._1, x._2 ) )
      .foreachRDD ( (rdd: RDD[ WordCount ], time: Time) => {
        handler ( rdd.sortBy ( _.word ), time )
      } )
  }
}

case class WordCount(word: String, count: Int)