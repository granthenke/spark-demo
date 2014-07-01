/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sa.websocket

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.WebSocketInputDStream._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

// scalastyle:off
/**
 * Usage: NetworkWordCount <master>
 *   <master> is the Spark master URL. In local mode, <master> should be 'local[n]' with n > 1.
 *
 * See http://www.meetup.com/meetup_api/docs/stream/2/rsvps/ for more guidance.
 */
// scalastyle:on
object MeetupRSVP {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: MeetupRSVP <master>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    // Process Args
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName(this.getClass.getCanonicalName)
      .setJars(Seq(SparkContext.jarOfClass(this.getClass).get))

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(conf, Seconds(5))

    // Create a MeetupRSVPStream and print out the lines
    val lines = ssc.webSocketTextStream("ws://stream.meetup.com/2/rsvps", StorageLevel.MEMORY_ONLY_SER)
    lines.print()
    ssc.start()
    ssc.awaitTermination()
  }
}