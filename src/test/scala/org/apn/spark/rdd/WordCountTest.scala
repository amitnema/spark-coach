package org.apn.spark.rdd

import org.apn.spark.TestBase
import org.testng.annotations.Test

/**
  * @author amit.nema
  */
class WordCountTest extends TestBase[ WordCountTest ] {

  @Test( dataProvider = "dpWordCount" ) def testCountWord(pathIn: String, word: String, count: Long) {
    val rdd = WordCount.countWord( spark.sparkContext, pathIn )
    rdd.filter( _._1.equals( word ) ).collect( )( 0 )._2 should be( count )
  }

  @Test( dataProvider = "dpWordCount" ) def testCountWordSave(pathIn: String, word: String, count: Long) {
    val pathOut = getPathOut( pathIn )
    WordCount.countWordSave( spark.sparkContext, pathIn, pathOut )

    spark.sparkContext
      .textFile( filterFiles( pathOut, "-00000" )( 0 ) )
      .map( _.stripPrefix( "(" ).stripSuffix( ")" ) )
      .map( _.split( ',' ) )
      .filter( _ ( 0 ).equals( word ) )
      .map( _ ( 1 ).toInt ).collect( )( 0 ) should be( count )
  }
}