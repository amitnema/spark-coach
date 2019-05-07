package org.apache.spark.streaming

import org.apache.spark.util.ManualClock

/** Ugly hack to access Spark private ManualClock class. */
object ClockWrapper {
  def advance(ssc: StreamingContext, timeToAdd: Duration): Unit = {
    val manualClock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    manualClock.advance( timeToAdd.milliseconds )
  }
}
