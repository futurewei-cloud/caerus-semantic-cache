package org.openinfralabs.caerus.cache.examples.spark.gridpocket

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openinfralabs.caerus.cache.client.spark.{SemanticCache, Support}
import org.openinfralabs.caerus.cache.examples.spark.{Query, Trace}

import java.io.{BufferedWriter, FileWriter}

/**
 * Class responsible for running GridPocket trace in Spark. The trace is separated in 12 parts (one for each month of
 * the year) and runs for a year worth of data from France. The total number of queries executed is 84.
 */
object GridPocketTrace {
  def createTrace(
    spark: SparkSession,
    schemaProvider: GridPocketSchemaProvider,
    year: Int,
    inputPath: String
  ): Seq[(String,DataFrame)] = {
    val queries: Seq[Query] = Seq(
      Q01(spark,schemaProvider),
      Q02(spark,schemaProvider),
      Q03(spark,schemaProvider),
      Q04(spark,schemaProvider),
      Q05(spark,schemaProvider),
      Q06(spark,schemaProvider),
      Q07(spark,schemaProvider)
    )
    val months: Seq[Int] = Seq(1,2,3,4,5,6,7,8,9,10,11,12)
    months.flatMap(
      month => queries.map(query =>
        ("%04d-%02d-%s".format(year, month, query.getName), query.execute(year.toString, month.toString, inputPath))
      )
    )
  }

  private def printUsage(): Unit = {
    Console.out.println("Usage: scala <jar file> <spark URI> <semantic cache used> <year> <input path> <output path>" +
      "<results path> <print path>")
  }

  private def getSupportedPlans(plan: LogicalPlan, supportTree: Support[Boolean]): Seq[LogicalPlan] = {
    if (supportTree.support)
      Seq(plan)
    else
      plan.children.indices.flatMap(i => getSupportedPlans(plan.children(i), supportTree.children(i)))
  }

  private def getSupportedPlans(plan: LogicalPlan): Seq[LogicalPlan] = {
    val supportTree: Support[Boolean] = SemanticCache.checkSupport(plan)
    getSupportedPlans(plan, supportTree)
  }

  def main(args: Array[ String ]): Unit = {
    // Take arguments.
    if (args.length != 7) {
      printUsage()
      System.exit(0)
    }
    val sparkURI: String = args(0)
    val semanticCacheURI: String = args(1)
    val year: Int = args(2).toInt
    val inputPath: String = args(3)
    val outputPath: String = args(4)
    val resultsPath: String = args(5)
    val printPath: String = args(6)

    // Initiate spark session.
    val spark: SparkSession =
        SparkSession.builder()
          .master(sparkURI)
          .appName(name = "GridPocketTrace")
          .getOrCreate()

    // Activate semantic cache if it is defined.
    if (semanticCacheURI != "none")
        SemanticCache.activate(spark, semanticCacheURI)

    // Create trace.
    val jobs: Seq[(String,DataFrame)] = createTrace(spark, new GridPocketSchemaProvider, year, inputPath)
    val trace: Trace = new Trace(jobs)

    // Print trace in print path if it is defined.
    if (printPath != "none") {
      val logicalPlans: Seq[LogicalPlan] = jobs.map(job => job._2.queryExecution.logical)
      val supportedPlans: Seq[LogicalPlan] = logicalPlans.flatMap(plan => getSupportedPlans(plan))
      val serializedPlans: Seq[String] = supportedPlans.map(plan => SemanticCache.transform(plan).toJSON)
      val out: BufferedWriter = new BufferedWriter(new FileWriter(printPath))
      serializedPlans.foreach(serializedPlan => out.write("%s\n".format(serializedPlan)))
      out.close()
    }

    // Run trace.
    val results: Seq[(String,Long)] = trace.execute(outputPath)

    // Close spark session.
    spark.stop()

    // Write result in output path.
    val out: BufferedWriter = new BufferedWriter(new FileWriter(resultsPath))
    results.foreach(result => out.write("%s,%s\n".format(result._1, result._2)))
    out.close()
  }
}
