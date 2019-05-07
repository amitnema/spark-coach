package org.apn.spark {

  import org.apache.spark.streaming.{ClockWrapper, Duration, Seconds, StreamingContext}
  import org.scalatest.Suite

  trait SparkStreamingSpec extends SparkSpec {
    this: Suite =>

    import java.nio.file.Files

    private var _ssc: StreamingContext = _

    override def beforeAll(): Unit = {
      super.beforeAll( )

      _ssc = new StreamingContext( sc, batchDuration )
      _ssc.checkpoint( checkpointDir )
    }

    def batchDuration: Duration = Seconds( 1 )

    def checkpointDir: String = Files.createTempDirectory( this.getClass.getSimpleName ).toUri.toString

    override def afterAll(): Unit = {
      if (_ssc != null) {
        _ssc.stop( stopSparkContext = false, stopGracefully = false )
        _ssc = null
      }

      super.afterAll( )
    }

    override def sparkConfig: Map[String, String] = {
      super.sparkConfig + ("spark.streaming.clock" -> "org.apache.spark.streaming.util.ManualClock")
    }

    def ssc: StreamingContext = _ssc

    def advanceClock(timeToAdd: Duration): Unit = {
      ClockWrapper.advance( _ssc, timeToAdd )
    }

    def advanceClockOneBatch(): Unit = {
      advanceClock( Duration( batchDuration.milliseconds ) )
    }

  }

}