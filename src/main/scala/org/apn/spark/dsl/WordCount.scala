package org.apn.spark.dsl

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{RowFactory, SparkSession}

object WordCount {

  private final val ColName: String = "word"

  def countWordCsv(spark: SparkSession, pathInTxt: String, pathOutCsv: String, delim: String = StringUtils.SPACE) = {
    countWord( spark, pathInTxt, delim ).coalesce( 1 ).write.csv( pathOutCsv )
  }

  def countWord(spark: SparkSession, pathIn: String, delim: String = StringUtils.SPACE) = {

    // Generate the schema based on the string of schema
    val fields = Array( DataTypes.createStructField( ColName, DataTypes.StringType, true ) )
    val schema = DataTypes.createStructType( fields )

    import spark.implicits._
    spark.read
      .textFile( pathIn )
      .flatMap( _.split( StringUtils.SPACE ) )
      .map( x => RowFactory.create( x.trim( ) ) )( RowEncoder.apply( schema ) )
      .groupBy( ColName )
      .count( )
      .orderBy( ColName )
  }
}