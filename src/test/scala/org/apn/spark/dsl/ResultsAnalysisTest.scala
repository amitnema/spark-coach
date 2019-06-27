package org.apn.spark.dsl

import org.apn.spark.SparkTestNGBase
import org.testng.annotations.{DataProvider, Test}

class ResultsAnalysisTest extends SparkTestNGBase {

  @DataProvider def dpResultAnalysis() = {
    Array ( Array [ Object ](
      getClass.getClassLoader.getResource ( "exam_result.csv" ).getPath,
      getClass.getClassLoader.getResource ( "course.csv" ).getPath
    )
    )
  }

  @Test ( dataProvider = "dpResultAnalysis" ) def testAggregateResult(resultPathIn: String, coursePathIn: String) {
    val ds = ResultsAnalysis.aggregateResult ( spark, resultPathIn, coursePathIn )
    import org.apache.spark.sql.functions._
    ds.
    filter ( col ( "id" ) === 2 ).
    select ( "percentage" ).
    collect ( )( 0 ).
    get ( 0 ) should be ( 60.57142857142858 )
  }
}
