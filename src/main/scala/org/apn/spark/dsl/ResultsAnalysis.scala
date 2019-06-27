package org.apn.spark.dsl

import java.sql.Date

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.scalalang.typed

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

  def aggregateResult(spark: SparkSession, resultPath: String, coursePath: String) = {
    import spark.implicits._

    val resultDS = readCSV ( resultPath )( spark, Encoders.product [ Result ] )
    val courseDS = readCSV ( coursePath )( spark, Encoders.product [ Course ] )

    resultDS.joinWith ( courseDS, resultDS ( "courseId" ).equalTo ( courseDS ( "id" ) ) ).
    groupByKey ( _._1.id ).
    mapGroups { case (id, itr) =>
      val list = itr.toList
      (id, list, list.forall ( x => x._1.examPoints > x._2.markMin ))
    }.
    filter ( _._3 ).
    flatMap ( _._2 ).
    groupByKey ( _._1.id ).
    agg ( typed.sum ( _._1.examPoints ), typed.sum ( _._2.markMax ) ).
    toDF ( "id", "examPoints", "maxMarks" ).
    select ( $"id", $"examPoints", $"maxMarks", $"examPoints" / $"maxMarks" * 100 ).
    withColumnRenamed ( "((examPoints / maxMarks) * 100)", "percentage" ).
    orderBy ( 'id )
  }

  private def readCSV[ T ](path: String)(implicit spark: SparkSession, encoder: Encoder[ T ]): Dataset[ T ] = {
    spark.read
    .option ( "header", true )
    .option ( "inferSchema", false )
    .option ( "ignoreLeadingWhiteSpace", true )
    .option ( "ignoreTrailingWhiteSpace", true )
    .option ( "nullValue", null )
    // .option ( "mode", "DROPMALFORMED" )
    .option ( "dateFormat", "dd/MM/yyyy" )
    .schema ( encoder.schema )
    .csv ( path )
    .as [ T ]
  }
}

case class Result(id: Int, studentName: String, courseId: Int, examDate: Date, examPoints: Int)

case class Course(id: Int, courseName: String, markMax: Int, markMin: Int)