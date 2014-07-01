package com.cloudera.sa

import org.apache.spark._
import org.apache.spark.SparkContext._
import java.util.regex.Pattern
import com.google.common.io.Files
import java.io.File
import parquet.hadoop.ParquetOutputFormat
import parquet.avro.{AvroWriteSupport, AvroParquetOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.streaming.{Duration, Minutes}
import com.cloudera.sa.sessionize.avro.LogLine
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}
import java.text.SimpleDateFormat
import org.slf4j.LoggerFactory
import org.apache.avro.generic.GenericRecord

/**
 *
 */
object Sessionize {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  val apacheLogRegex = Pattern.compile("(\\d+.\\d+.\\d+.\\d+).*\\[(.*)\\].*GET (\\S+) \\S+ (\\d+) (\\d+) (\\S+) (.*)")
  val testLines = List(
    "12.34.56.78 - - [09/Jan/2014:04:02:26 -0800] \"GET / HTTP/1.1\" 200 43977 \"-\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)\"",
    "123.4.56.7 - - [09/Jan/2014:04:02:26 -0800] \"GET /financing/incentives HTTP/1.1\" 200 20540 \"https://www.google.com/\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.41 Safari/537.36\"",
    "123.45.67.89 - - [09/Jan/2014:04:02:28 -0800] \"GET /static/VEAMUNaPbswx4l9JeZqItoN6YKiVmYY84EJKnPKSPPM.css HTTP/1.1\" 200 6497 \"http://www.example.com/financing/incentives\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.41 Safari/537.36\"",
    "123.45.67.89 - - [09/Jan/2014:04:42:28 -0800] \"GET /static/u5Uj9e2Cc98JPkk2CIHl4SGWcqeQ0YU9O7Ua61z9Qdi.js HTTP/1.1\" 200 6192 \"http://www.example.com/financing/incentives\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.41 Safari/537.36\"",
    "123.45.67.89 - - [09/Jan/2014:04:56:28 -0800] \"GET /static/ZiQCg9sERShna8pay7mOZZdbUwqH6n6s9bbmpJhOzpo.css HTTP/1.1\" 200 626 \"http://www.example.com/financing/incentives\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.41 Safari/537.36\""
  )

  /**
   * Quick and dirty function to parse a line into a LogLine
   */
  def parseLine(line: String) = {
    val matcher = apacheLogRegex.matcher(line)
    if(matcher.find()) {
      val logLine = LogLine.newBuilder()
        .setIp(matcher.group(1))
        .setTimestamp(new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss").parse(matcher.group(2)).getTime)
        .setUrl(matcher.group(3))
        .setReferrer(matcher.group(6))
        .setUseragent(matcher.group(7))
        .setSessionid(0) // Default session to 0
        .build()
      Some(logLine)
    } else {
      // No Match
      LOGGER.warn("Could not parse line: {}", line)
      None
    }
  }

  /**
   * Sessionize a sequence of LogLines based on a passed session timeout
   */
  def sessionize(lines: Seq[LogLine], timeout: Duration) = {
    if(lines.size < 2) {
      lines
    } else {
      val sorted = lines.sortBy(_.getTimestamp)
      sorted.tail.scanLeft(sorted.head) { case (prev, cur) =>
        // Gets the correct session session
        def calculateSession = if (sessionTimedOut) prev.getSessionid + 1 else prev.getSessionid.toInt
        // Returns true if the session has timed out between the prev and cur LogLine
        def sessionTimedOut = cur.getTimestamp - prev.getTimestamp > timeout.milliseconds

        LogLine.newBuilder(cur.asInstanceOf[LogLine]).setSessionid(calculateSession).build()
      }
    }
  }

  /**
   * Class to register all classes that will be serialized with Kryo.
   * Kryo would will still work without using it.
   * However, it would have to store the full class name with each object, which is wasteful.
   */
  class SessionizeRegistrar extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[LogLine])
    }
  }

  def main(args: Array[String]) {
    // Validate args
    if (args.length == 0) {
      System.err.println("Usage: Sessionize <master> input")
      System.exit(1)
    }

    // Process Args and create spark context
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName(this.getClass.getCanonicalName)
      .setJars(Seq(SparkContext.jarOfClass(this.getClass).get))
      .set("spark.serializer", classOf[KryoSerializer].getName) // Set Kryo as the serializer
      .set("spark.kryo.registrator", classOf[SessionizeRegistrar].getName)  // Don't need to register classes but its a performance gain.


    val spark = new SparkContext(conf)
    val job = new Job()

    // Set up output directories
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    LOGGER.info("outputDir: {}", outputDir)

    // Set up output serialization
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    AvroParquetOutputFormat.setSchema(job, LogLine.SCHEMA$)

    // Sessionize the data
    val dataSet = if (args.length == 2) spark.textFile(args(1)) else spark.parallelize(testLines)
    val parsed = dataSet.flatMap(parseLine) // flat map to drop None options. See parseLine function.
    val sessionized = parsed.groupBy(_.getIp).flatMapValues(l => sessionize(l.toSeq, Minutes(30)))

    // Create a rdd with all keys set to null since we don't want to write them out
    val writable = sessionized.map { case (key, line) => (null, line)}

    // Write output to command line
    writable.foreach{ case (key, line) => println(line) }

    // Write output to parquet file
    writable.saveAsNewAPIHadoopFile(outputDir, classOf[Void], classOf[LogLine], classOf[ParquetOutputFormat[LogLine]], job.getConfiguration)
  }
}
