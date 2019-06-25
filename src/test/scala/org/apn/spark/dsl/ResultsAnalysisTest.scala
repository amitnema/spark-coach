package org.apn.spark.dsl

import org.apn.spark.SparkTestNGBase
import org.testng.annotations.{DataProvider, Test}

class ResultsAnalysisTest extends SparkTestNGBase {

  @DataProvider def dpResultAnalysis() = {
    Array ( Array [ Object ]( getClass.getClassLoader.getResource ( "exam_result.csv" ).getPath, Int.box ( 40 ) ) )
  }

  @Test ( dataProvider = "dpResultAnalysis" ) def testAggregateResult(pathIn: String, threshold: Int) {
    val ds = ResultsAnalysis.aggregateResult ( spark, pathIn, threshold )
    ds.show ( 5, true )
    //    ds.filter ( col ( "id" ) === 3 )
    //    .select ( "examPoints" )
    //    .collect ( )( 0 )
    //    .get ( 0 ) should be ( 3 )
  }
}
