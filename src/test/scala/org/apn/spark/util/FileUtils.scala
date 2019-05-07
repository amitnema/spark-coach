package org.apn.spark.util

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.io.FilenameUtils

trait FileUtils {

  /**
    * Returns the path for the output files.
    * e.g. input arg provided as in-path then
    * it returns {@code $base-path\test-output\$calling-class-name\$calling-method-name\in-path-file-name_20190101000000}
    *
    * @param fileIn : path to refer
    * @return
    */
  def getPathOut(fileIn: String) = {
    String.join( "/",
      getClass.getClassLoader.getResource( "." ).getPath,
      "test-output",
      this.getClass.getName,
      Thread.currentThread.getStackTrace( )( 3 ).getMethodName,
      FilenameUtils.removeExtension( FilenameUtils.getBaseName( fileIn ) ) ) + new SimpleDateFormat( "_yMMddhhmmss" ).format( Calendar.getInstance.getTime )
  }

  /**
    * Returns the filenames with the extension specified, exists inside the given directory.
    * @param directory path of the directory to search inside and filter.
    * @param extension extension of the file to filter out
    * @return Array[String] : filenames with the extension
    */
  def filterFiles(directory: String, extension: String) = {

    (new File( directory )).listFiles.
      filter { f => f.isFile && (f.getName.toLowerCase.endsWith( extension )) }.
      map( _.getAbsolutePath )
  }

}
