package org.apn.spark

import org.apache.spark.sql.SparkSession
import org.apn.spark.util.FileUtils
import org.scalatest.Matchers
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.{AfterClass, BeforeClass, DataProvider}

import scala.reflect.runtime.universe.TypeTag

/**
  * @author amit.nema
  */
class TestBase[T](implicit tag: TypeTag[T]) extends TestNGSuite with Matchers with FileUtils {

  protected final var spark: SparkSession = _

  @BeforeClass def beforeClass() {
    spark = SparkSession.builder( ).appName( tag.tpe.toString ).master( "local[*]" ).getOrCreate( )
  }

  @AfterClass def afterClass() {
    spark.stop
    spark.close
  }

  @DataProvider def dpWordCount() = {
    Array( Array[Object]( getClass.getClassLoader.getResource( "wordcount.txt" ).getPath, "Spark", Long.box( 3 ) ) )
  }

}
