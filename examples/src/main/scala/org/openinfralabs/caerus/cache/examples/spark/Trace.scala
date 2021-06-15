package org.openinfralabs.caerus.cache.examples.spark

import org.apache.spark.sql.DataFrame

/**
 * Trace class for Spark, responsible for maintaining and running a sequence of jobs. Construction of the trace is
 * dataset-specific and cannot be performed in this level.
 */
class Trace(jobs: Seq[(String,DataFrame)]) {
  private def runJob(inputDF: DataFrame, outputPath: String): Unit = {
    if (outputPath == null || outputPath == "")
      inputDF.collect().foreach(println)
    else
      inputDF.write
        .mode(saveMode="overwrite")
        .format(source="csv")
        .option("header", "true")
        .save(outputPath)
  }

  /**
   * Execute trace and store all the job results in outputPath.
   * @param outputPath  Path to store the results of the jobs.
   * @return name and execution time for every query (queries might consist of one or multiple jobs)
   */
  def execute(outputPath: String): Seq[(String,Long)] = {
    jobs.map(job => {
      val name = job._1
      val df = job._2
      val startTime: Long = System.nanoTime()
      runJob(df, outputPath + "/" + name)
      val endTime: Long = System.nanoTime()
      (name, endTime-startTime)
    })
  }
}