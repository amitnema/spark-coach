package org.apn.spark.rdd

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
  * @author amit.nema
  */
object WordCount {

  def countWordSave(sc: SparkContext, pathIn: String, pathOut: String, delim: String = StringUtils.SPACE): Unit = {
    countWord( sc, pathIn, delim ).saveAsTextFile( pathOut )
  }

  def countWord(sc: SparkContext, pathIn: String, delim: String = StringUtils.SPACE): RDD[(String, Int)] = {
    val textFile = sc.textFile( pathIn )
    textFile.flatMap( _.split( delim ) )
      .map( (_, 1) )
      .reduceByKey( _ + _ )
      .sortByKey( true, 1 )
  }
}