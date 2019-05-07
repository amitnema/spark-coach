package org.apn.spark

import org.apache.spark.sql.SparkSession
import org.apn.spark.util.FileUtils
import org.scalatest.Matchers
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.{AfterClass, BeforeClass, DataProvider}

/**
  * @author amit.nema
  */
trait SparkTestNGBase extends TestNGSuite with Matchers with FileUtils {
  thisSuite =>

  private final var _spark: SparkSession = _

  @BeforeClass def beforeClass() {
    _spark = SparkSession.builder( ).appName( getClass.getSimpleName ).master( "local[*]" ).getOrCreate( )
    sparkConfig.foreach { case (k, v) => _spark.sparkContext.getConf.setIfMissing( k, v ) }
  }

  @AfterClass def afterClass() {
    if (_spark != null) {}
    _spark.stop
    _spark.close
    _spark = null
  }

  @DataProvider def dpWordCount() = {
    Array( Array[Object]( getClass.getClassLoader.getResource( "wordcount.txt" ).getPath, "Spark", Long.box( 3 ) ) )
  }

  def sparkConfig: Map[String, String] = Map.empty

  def spark = _spark
}
