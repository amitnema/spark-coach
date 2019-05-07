package org.apn.spark.sql

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apn.spark.SparkTestNGBase
import org.testng.annotations.Test

/**
  * @author amit.nema
  */
class WordCountTest extends SparkTestNGBase {

  @Test( dataProvider = "dpWordCount" ) def testCountWords(pathIn: String, word: String, count: Long) {
    WordCount.countWord( spark, pathIn ).createOrReplaceTempView( "count_words_df" )
    spark.sql( s"select wc from count_words_df where word='$word'" )
      .collect( )( 0 ).getLong( 0 ) should be( count )
  }

  @Test( dataProvider = "dpWordCount" ) def testCountWordCsv(pathIn: String, word: String, count: Long) {
    val pathOut = getPathOut( pathIn )
    WordCount.countWordCsv( spark, pathIn, pathOut )

    val schema = StructType( Array( StructField( "word", DataTypes.StringType ), StructField( "count", DataTypes.LongType ) ) )
    //assertion
    spark.read
      .schema( schema )
      .csv( pathOut )
      .createOrReplaceTempView( "count_word_csv" )
    spark.sql( s"select count from count_word_csv where word='$word'" )
      .collect( )( 0 ).getLong( 0 ) should be( count )
  }
}