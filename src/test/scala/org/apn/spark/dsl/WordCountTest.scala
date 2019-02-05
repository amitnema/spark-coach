package org.apn.spark.dsl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apn.spark.TestBase
import org.testng.annotations.Test

/**
  * @author amit.nema
  */
class WordCountTest extends TestBase[WordCountTest] {

  @Test( dataProvider = "dpWordCount" ) def testCountWord(pathIn: String, word: String, count: Long) {
    val ds = WordCount.countWord( spark, pathIn )
    ds.filter( col( "word" ) === word )
      .select( "count" )
      .collect( )( 0 )
      .get( 0 ) should be( 3 )
  }

  @Test( dataProvider = "dpWordCount" ) def testCountWordCsv(pathIn: String, word: String, count: Long) {
    val pathOut = getPathOut( pathIn )
    WordCount.countWordCsv( spark, pathIn, getPathOut( pathIn ) )

    //assertion
    val schema = StructType( Array( StructField( "word", DataTypes.StringType ), StructField( "count", DataTypes.LongType ) ) )
    val sp = spark
    import sp.implicits._
    spark.read
      //.option( "inferSchema", "true" )
      .schema( schema )
      .csv( filterFileExtension( pathOut, ".csv" )( 0 ) )
      .filter( $"word" === word )
      .select( $"count" )
      .collect( )( 0 )
      .get( 0 ) should be( 3 )
  }
}