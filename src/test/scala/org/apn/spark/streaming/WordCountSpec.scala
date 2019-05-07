package org.apn.spark.streaming

import java.io.PrintWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, Time}
import org.apn.spark.SparkStreamingSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

import scala.collection.mutable.ListBuffer
import scala.util.Random

class WordCountSpec extends FlatSpec
  with SparkStreamingSpec with GivenWhenThen with Matchers with Eventually {

  // default timeout for eventually trait
  implicit override val patienceConfig =
    PatienceConfig ( timeout = scaled ( Span ( 5000, Millis ) ) )
  private val windowDuration = Seconds ( 4 )
  private val slideDuration = Seconds ( 2 )
  private val pathIn = getClass.getClassLoader.getResource ( "streaming-wc" ).getPath

  "Sample set" should "be counted" in {
    Given ( "streaming context is initialized" )
    println("Directory:"+pathIn)
    var results = ListBuffer.empty [ Array[ WordCount ] ]

    WordCount.countWord ( ssc, pathIn ) {
      (wordsCount: RDD[ WordCount ], _: Time) =>
        results += wordsCount.collect ( )
        results.foreach(x=> println(x.length))
    }

    ssc.start ( )

    When ( "first set of words queued" )
    //    lines += sc.makeRDD ( Seq ( "a", "b" ) )
    new PrintWriter ( pathIn +"/wordcount_"+ Random.nextInt ( 2 ) + ".dat" ) {
      write ( sc.textFile ( getClass.getClassLoader.getResource ( "wordcount.txt" ).getPath ).collect ( ).mkString );
      close
    }

    Then ( "words counted after first slide" )
    advanceClock ( slideDuration )
    eventually {
      //      results.last should equal ( Array (
      //        WordCount ( "a", 1 ),
      //        WordCount ( "b", 1 ) ) )
      println ( "1~~~~~~~~~~~" + results.mkString )
    }

        When ( "second set of words queued" )
//        lines += sc.makeRDD ( Seq ( "b", "c" ) )
    new PrintWriter ( pathIn +"/wordcount_"+ Random.nextInt ( 2 ) + ".dat" ) {
      write ( sc.textFile ( getClass.getClassLoader.getResource ( "wordcount.txt" ).getPath ).collect ( ).mkString );
      close
    }

        Then ( "words counted after second slide" )
        advanceClock ( slideDuration )
        eventually {
          println ( "2~~~~~~~~~~~" + results.mkString )

//          results.last should equal ( Array (
//            WordCount ( "a", 1 ),
//            WordCount ( "b", 2 ),
//            WordCount ( "c", 1 ) ) )
        }

    /*When ( "nothing more queued" )

    Then ( "word counted after third slide" )
    advanceClock ( slideDuration )
    eventually {
      results.last should equal ( Array (
        WordCount ( "a", 0 ),
        WordCount ( "b", 1 ),
        WordCount ( "c", 1 ) ) )
    }*/

    When ( "nothing more queued" )

    Then ( "word counted after fourth slide" )
    advanceClock ( slideDuration )
    eventually {
      /*results.last should equal ( Array (
        WordCount ( "a", 0 ),
        WordCount ( "b", 0 ),
        WordCount ( "c", 0 ) ) )
    }*/
      println ( "9~~~~~~~~~~~" + results.mkString )

    }
  }
}
