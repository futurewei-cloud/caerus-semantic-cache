package org.openinfralabs.caerus.cache.examples.spark

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, explode, sum}
import org.apache.spark.sql.{SparkSession, functions}

import java.io.{BufferedWriter, FileWriter}
import scala.collection.mutable

/**
 * Class that gets metrics for execution time, CPU usage, memory usage, and I/O size.
 */
case class Metrics(
  queryNum: Long,
  execTime: Long,
  cpuTime: Long,
  readSize: Long,
  writeSize: Long
) {
  override def toString: String = "%d,%d,%d,%d,%d".format(queryNum, execTime, cpuTime, readSize, writeSize)
}

object Metrics {
  /**
   * Function to get all metrics from history folder.
   * @param spark Spark session to use for running metrics jobs.
   * @param appId Application ID for which to get metrics.
   * @param historyPath Input path to find historic archive for the applications.
   * @param outputPath Output path to write metrics results.
   */
  def getMetrics(spark: SparkSession, historyPath: String, appId: String, outputPath: String): Unit = {
    // Create metrics DataFrame.
    val metricFile = historyPath + Path.SEPARATOR + appId
    val metricDF = spark.read.json(metricFile)

    // Get metrics per stage.
    val cpuTimes: mutable.HashMap[Long,Double] = mutable.HashMap.empty[Long,Double]
    metricDF
      .filter(col("Event")==="SparkListenerStageCompleted")
      .withColumn("accumulables", explode(col("Stage Info.Accumulables")))
      .select(
        col("Stage Info.Stage ID").as("id"),
        col("accumulables.Name").as("name"),
        col("accumulables.Value").as("value"))
      .filter(col("name") === "internal.metrics.executorCpuTime" or
        col("name") === "internal.metrics.executorDeserializeCpuTime")
      .groupBy(col("id"))
      .agg(sum(col("value")).as("cpuTime"))
      .orderBy(col("id"))
      .collect()
      .foreach(row => cpuTimes(row(0).asInstanceOf[Long]) = row(1).asInstanceOf[Double])

    val readSizes: mutable.HashMap[Long,Double] = mutable.HashMap.empty[Long,Double]
    metricDF
      .filter(col("Event")==="SparkListenerStageCompleted")
      .withColumn("accumulables", explode(col("Stage Info.Accumulables")))
      .select(
        col("Stage Info.Stage ID").as("id"),
        col("accumulables.Name").as("name"),
        col("accumulables.Value").as("value"))
      .filter(col("name") === "internal.metrics.input.bytesRead")
      .groupBy(col("id"))
      .agg(sum(col("value")).as("bytesRead"))
      .orderBy(col("id"))
      .collect()
      .foreach(row => readSizes(row(0).asInstanceOf[Long]) = row(1).asInstanceOf[Double])

    val writeSizes: mutable.HashMap[Long,Double] = mutable.HashMap.empty[Long,Double]
    metricDF
      .filter(col("Event")==="SparkListenerStageCompleted")
      .withColumn("accumulables", explode(col("Stage Info.Accumulables")))
      .select(
        col("Stage Info.Stage ID").as("id"),
        col("accumulables.Name").as("name"),
        col("accumulables.Value").as("value"))
      .filter(col("name") === "internal.metrics.output.bytesWritten")
      .groupBy(col("id"))
      .agg(sum(col("value")).as("bytesWritten"))
      .orderBy(col("id"))
      .collect()
      .foreach(row => writeSizes(row(0).asInstanceOf[Long]) = row(1).asInstanceOf[Double])

    // Get stages for each job and execution times.
    val jobs: mutable.HashMap[Long,Seq[Long]] = mutable.HashMap.empty[Long,Seq[Long]]
    metricDF
      .filter(col("Event") === "SparkListenerJobStart")
      .select(col("Job ID"), col("Stage IDs"))
      .collect()
      .foreach(row => jobs(row(0).asInstanceOf[Long]) = row(1).asInstanceOf[Seq[Long]])

    val execTimes: mutable.HashMap[Long,Long] = mutable.HashMap.empty[Long,Long]
    metricDF
      .filter(col("Event") === "SparkListenerJobStart" or
        col("Event") === "SparkListenerJobEnd")
      .groupBy("Job ID")
      .agg(functions.max(col("Completion Time")).as("endTime"), functions.max(col("Submission Time")).as("startTime"))
      .withColumn("execTime", col("endTime")-col("startTime"))
      .select(col("Job ID"), col("execTime"))
      .collect()
      .foreach(row => execTimes(row(0).asInstanceOf[Long]) = row(1).asInstanceOf[Long])

    // Calculate queries metrics. Assume that a query is a collection of consecutive jobs finished when a job writes to
    // the disk (last job).
    var execTime: Long = 0
    var cpuTime: Double = 0
    var readSize: Double = 0
    var writeSize: Double = 0
    var curQuery: Long = 0
    val queryMetrics: mutable.HashMap[Long,Metrics] = mutable.HashMap.empty[Long,Metrics]
    for (jobId <- 0 until jobs.size) {
      execTime += execTimes(jobId)
      for (stageId <- jobs(jobId)) {
        if (cpuTimes.contains(stageId))
          cpuTime += cpuTimes(stageId)
        if (readSizes.contains(stageId))
          readSize += readSizes(stageId)
        if (writeSizes.contains(stageId))
          writeSize += writeSizes(stageId)
      }
      if (writeSize > 0) {
        queryMetrics(curQuery) = Metrics(
          curQuery+1,
          execTime,
          (cpuTime/1000000).toLong,
          readSize.toLong,
          writeSize.toLong
        )
        curQuery += 1
        execTime = 0
        cpuTime = 0
        readSize = 0
        writeSize = 0
      }
    }

    // Write result in output path.
    val out = new BufferedWriter(new FileWriter(outputPath))
    for (queryId <- 0 until queryMetrics.size) {
      val elem: String = queryMetrics(queryId).toString
      Console.out.println("%s".format(elem))
      out.write("%s\n".format(elem))
    }
    out.close()
  }

  private def printUsage(): Unit = {
    Console.out.println("Usage: scala <jar file> <spark URI> <history path> <app-id>")
  }

  def main(args: Array[ String ]): Unit = {
    // Take arguments.
    if (args.length != 4) {
      printUsage()
      return
    }
    val sparkURI: String = args(0)
    val historyPath: String = args(1)
    val appId: String = args(2)
    val outputPath: String = args(3)

    // Initiate spark session.
    val spark: SparkSession =
      SparkSession.builder()
        .master(sparkURI)
        .appName(name = "Metrics")
        .getOrCreate()

    // Get metrics
    Metrics.getMetrics(spark, historyPath, appId, outputPath)

    // Close spark session and print results.
    spark.stop()
  }
}
