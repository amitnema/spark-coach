package org.apn.spark

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.Matchers
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.{AfterClass, BeforeClass, DataProvider}

import scala.reflect.runtime.universe.TypeTag

/**
  * @author amit.nema
  */
class TestBase[T](implicit tag: TypeTag[T]) extends TestNGSuite with Matchers {

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

  /**
    * Returns the path for the output files.
    * e.g. input arg provided as in-path then
    * it returns {@code $base-path\test-output\$calling-class-name\$calling-method-name\in-path-file-name_20190101000000}
    *
    * @param fileIn : path to refer
    * @return
    */
  protected def getPathOut(fileIn: String) = {
    String.join( "/",
      getClass.getClassLoader.getResource( "." ).getPath,
      "test-output",
      this.getClass.getName,
      Thread.currentThread.getStackTrace( )( 2 ).getMethodName,
      FilenameUtils.removeExtension( FilenameUtils.getBaseName( fileIn ) ) ) + new SimpleDateFormat( "_yMMddhhmmss" ).format( Calendar.getInstance.getTime )
  }

  protected def filterFileExtension(directoryName: String, ext: String) = {
    (new File( directoryName )).listFiles.
      filter { f => f.isFile && (f.getName.toLowerCase.endsWith( ext )) }.
      map( _.getAbsolutePath )
  }

}
