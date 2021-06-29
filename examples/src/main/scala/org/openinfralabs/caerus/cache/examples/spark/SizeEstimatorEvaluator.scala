package org.openinfralabs.caerus.cache.examples.spark


import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openinfralabs.caerus.cache.client.spark.{SemanticCache, Support}
import org.openinfralabs.caerus.cache.client.{BasicCandidateSelector, BasicSizeEstimator, CandidateSelector, SizeEstimator}
import org.openinfralabs.caerus.cache.common.{Caching, Candidate, FileSkippingIndexing, Repartitioning, Tier}
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
import org.openinfralabs.caerus.cache.examples.spark.gridpocket.{GridPocketSchemaProvider, GridPocketTrace}

import java.io.{BufferedWriter, FileWriter}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source._

object SizeEstimatorEvaluator {
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

  def main(args: Array[String]) {
    // Take arguments.
    if (args.length != 7) {
      Console.out.println("arg length should be 7")
      //System.exit(0)
    }
    val sparkURI: String = args(0)
    val semanticCacheURI: String = args(1)
    val year: Int = args(2).toInt
    val inputPath: String = args(3)
    val outputPath: String = args(4)
    val historyPath: String = args(5)
    val resultsPath: String = args(6)

    // Initiate spark session.
    val spark: SparkSession =
      SparkSession.builder()
        .master(sparkURI)
        .appName(name = "SizeEstimatorEvaluator")
        .getOrCreate()

    // Create trace.
    val jobs: Seq[(String, DataFrame)] = GridPocketTrace.createTrace(spark, new GridPocketSchemaProvider, year,
      inputPath)

    // Find all the candidates from these plans.
    val candidateSelector: CandidateSelector = BasicCandidateSelector()
    val logicalPlans: Seq[LogicalPlan] = jobs.map(job => job._2.queryExecution.optimizedPlan)
    val supportedPlans: Seq[LogicalPlan] = logicalPlans.flatMap(plan => getSupportedPlans(plan))
    val plans: Seq[CaerusPlan] = supportedPlans.map(plan => SemanticCache.transform(plan))
    val candidates: Seq[Candidate] =
      plans.flatMap(candidateSelector.getCandidates).distinct.filter(candidate => !candidate.isInstanceOf[Caching])
    Console.out.println("Number of Candidates:\n%s".format(candidates.length))

    // Calculate the estimates for all the candidates.
    val sizeEstimator: SizeEstimator = BasicSizeEstimator()
    for (candidate <- candidates) {
      sizeEstimator.estimateSize(candidate)
    }
    // Run the candidates by using semantic cache api.
    if (semanticCacheURI == "none") {
      Console.err.println("Semantic Cache URI should be defined.")
      System.exit(-1)
    }
    val semanticCache: SemanticCache = new SemanticCache(spark, semanticCacheURI)


    // Output: candidate No, write size, estimated write size, read size, estimated read size (min "2019-01-01 00:00:00, max "2019-02-01 00:00:00)
    val schema: StructType = new GridPocketSchemaProvider().getSchema
    val loadDF: DataFrame = spark.read.option("header", "true").schema(schema).csv(inputPath)
    for (candidate <- candidates) {
      Console.out.println("Running candidate: %s".format(candidate))
      candidate match {
        case Repartitioning(_, index, _) =>
          semanticCache.repartitioning(loadDF, loadDF.columns(index), Tier.STORAGE_DISK, "temp")
        case FileSkippingIndexing(_, index, _) =>
          semanticCache.fileSkippingIndexing(loadDF, loadDF.columns(index), Tier.STORAGE_DISK, "temp")
        case Caching(plan, _) =>
      }
      loadDF
        .filter(col("date") < "%04d-02-01 00:00:00".format(year) &&
          col("date") >= "%04d-01-01 00:00:00".format(year))
        .write.mode("overwrite").option("header", "true").csv(outputPath)
      semanticCache.delete("temp")
    }

    // Write results
    val applicationId = spark.sparkContext.applicationId
    spark.stop
    // Initiate spark session for getting results.
    val sparkMetrics: SparkSession =
      SparkSession.builder()
        .master(sparkURI)
        .appName(name = "SizeEstimatorEvaluatorMetrics")
        .getOrCreate()
    // Write results and stop spark session.
    Metrics.getMetrics(sparkMetrics, historyPath, applicationId, resultsPath)
    sparkMetrics.stop

    val result = ArrayBuffer[Array[String]]()
    val bufferedSource = fromFile(resultsPath)
    for (line <- bufferedSource.getLines()) {
      result += line.split(",").map(_.trim)
    }
    bufferedSource.close()

    for (candidate <- candidates) {
      val Array(estimatedWrite,estimatedReadInfo) = candidate.sizeInfo.toString.split(",")
      Console.out.println(estimatedWrite,result(0)(4),estimatedReadInfo,result(1)(3))
    }






  }
}

