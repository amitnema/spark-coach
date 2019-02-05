package org.apn.spark.sql

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

/**
  * @author amit.nema
  */
object WordCount {

  def countWordCsv(spark: SparkSession, pathInTxt: String, pathOutCsv: String, delim: String = StringUtils.SPACE) = {
    countWord( spark, pathInTxt, delim ).coalesce( 1 ).write.option( "header", "false" ).csv( pathOutCsv )
  }

  def countWord(spark: SparkSession, pathInTxt: String, delim: String = StringUtils.SPACE) = {
    spark.read.textFile( pathInTxt ).createOrReplaceTempView( "file_dataset" )
    spark.sql( s"SELECT word, count(1) AS wc FROM (select explode(split(value, '$delim')) as word from file_dataset) group by word order by word" )
  }
}
