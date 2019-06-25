package org.apn.spark.dsl

import java.sql.Date

import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}

/**
  *
  * Analyze the students result data and aggregate the marks filtered by threshold to pass the exam.
  * <table cellpadding="5" cellspacing="0">
  * <caption style="border: solid thin;">Result</caption>
  * <tr>
  * <th style="border: solid thin;">ID</th>
  * <th style="border: solid thin;">Student name</th>
  * <th style="border: solid thin;">Exam name</th>
  * <th style="border: solid thin;">Exam date</th>
  * <th style="border: solid thin;">Exam points</th>
  * </tr>
  * <tr>
  * <td style="border: solid thin;">1</td>
  * <td style="border: solid thin;">John</td>
  * <td style="border: solid thin;">Mathematics</td>
  * <td style="border: solid thin;">15/06/2016</td>
  * <td style="border: solid thin;">67</td>
  * </tr>
  * </table>
  *
  * Threshold = 40 [Marks required to pass the exam in a individual subject]
  *
  * @author Amit Nema
  */
object ResultsAnalysis {

  def aggregateResult(spark: SparkSession, resultPath: String, threshold: Int): Dataset[Row] = {
    import org.apache.spark.sql.expressions.scalalang._
    import spark.implicits._
    readCSV ( spark, resultPath ).
    groupByKey ( _.id ).
    mapGroups { case (id, itr) =>
      val list = itr.toList
      (id, list, list.forall ( _.examPoints > threshold ))
    }.
    filter ( _._3 ).
    flatMap ( _._2 ).
    groupByKey ( _.id ).
    agg ( typed.sum [ Result ]( _.examPoints ) ).
    toDF ( "id", "examPoints" ).
    orderBy ( 'id )
  }

  private def readCSV(spark: SparkSession, csvPath: String) = {
    import spark.implicits._
    spark.read
    .option ( "header", "true" )
    .option ( "inferSchema", "true" )
    .option ( "ignoreLeadingWhiteSpace", true )
    .option ( "ignoreTrailingWhiteSpace", true )
    .option ( "nullValue", "null" )
    .option ( "dateFormat", "dd/MM/yyyy" )
    .schema ( Encoders.product [ Result ].schema )
    .csv ( csvPath )
    .as [ Result ]
  }
}

case class Result(id: Int, studentName: String, examName: String, examDate: Date, examPoints: Int)