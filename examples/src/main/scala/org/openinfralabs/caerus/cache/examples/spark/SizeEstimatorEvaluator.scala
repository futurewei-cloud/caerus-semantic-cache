package org.openinfralabs.caerus.cache.examples.spark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openinfralabs.caerus.cache.client.spark.{SemanticCache, Support}
import org.openinfralabs.caerus.cache.client.{BasicCandidateSelector, BasicSizeEstimator, CandidateSelector, SamplingSizeEstimator, SizeEstimator}
import org.openinfralabs.caerus.cache.common._
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
import org.openinfralabs.caerus.cache.examples.spark.gridpocket.{GridPocketSchemaProvider, GridPocketTrace}

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
    val jobs: Seq[(String, DataFrame)] =
      GridPocketTrace.createTrace(spark, new GridPocketSchemaProvider, year, inputPath)
    val job: (String, DataFrame) = jobs.head

    // Find all the candidates from these plans.
    val candidateSelector: CandidateSelector = BasicCandidateSelector()
    val logicalPlan: LogicalPlan = job._2.queryExecution.optimizedPlan
    val supportedPlans: Seq[LogicalPlan] = getSupportedPlans(logicalPlan)
    val supportedPlan: LogicalPlan = supportedPlans.head
    val plan: CaerusPlan = SemanticCache.transform(supportedPlan)
    val candidatePairs: Seq[(LogicalPlan,Candidate)] = candidateSelector.getCandidates(supportedPlan, plan)
    Console.out.println("Number of Candidates:\n%s".format(candidatePairs.length))

    // Calculate the estimates for all the candidates.
    val basicSizeEstimator: SizeEstimator = BasicSizeEstimator()
    val samplingSizeEstimator: SizeEstimator = SamplingSizeEstimator(spark, 100)
    candidatePairs.foreach(candidatePair => {
      basicSizeEstimator.estimateSize(candidatePair._1, candidatePair._2)
      samplingSizeEstimator.estimateSize(candidatePair._1, candidatePair._2)
    })

    // Run the candidates by using semantic cache api.
    if (semanticCacheURI == "none") {
      Console.err.println("Semantic Cache URI should be defined.")
      System.exit(-1)
    }
    val semanticCache: SemanticCache = new SemanticCache(spark, semanticCacheURI)

    // Output: candidate No, write size, estimated write size, read size, estimated read size (min "2019-01-01 00:00:00, max "2019-02-01 00:00:00)
    val schema: StructType = new GridPocketSchemaProvider().getSchema
    val loadDF: DataFrame = spark.read.option("header", "true").schema(schema).csv(inputPath)
    candidatePairs.zipWithIndex.foreach(elem => {
      val logicalPlan: LogicalPlan = elem._1._1
      val candidate: Candidate = elem._1._2
      val tempName: String = "temp-%d".format(elem._2)
      Console.out.println("Running candidate: %s".format(candidate))
      candidate match {
        case Repartitioning(_, index, _) =>
          semanticCache.startRepartitioning(loadDF, loadDF.columns(index), Tier.STORAGE_DISK, tempName)
        case FileSkippingIndexing(_, index, _) =>
          semanticCache.startFileSkippingIndexing(loadDF, loadDF.columns(index), Tier.STORAGE_DISK, tempName)
        case Caching(_, _) =>
          semanticCache.startCacheIntermediateData(logicalPlan, Tier.STORAGE_DISK, tempName)
      }
      Console.out.println(semanticCache.status)
      loadDF
        .filter(col("date") < "%04d-02-01 00:00:00".format(year) && col("date") >= "%04d-01-01 00:00:00".format(year))
        .write.mode("overwrite").option("header", "true").csv(outputPath)
      semanticCache.delete(tempName)
    })

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

    for ((_,candidate) <- candidatePairs) {
      val Array(estimatedWrite,estimatedReadInfo) = candidate.sizeInfo.toString.split(",")
      val Array(samplingWrite, samplingReadInfo) = candidate.sizeInfo.toString.split(",")
      candidate match {
        case Repartitioning(_,_,_) =>
          Console.out.println("EstimatedWrite: %s%s, ActualWrite: %s, EstimatedRead: %s%s, ActualRead: %s".format(
            estimatedWrite,samplingWrite,result(0)(4),estimatedReadInfo,samplingReadInfo,result(1)(3)))
        case FileSkippingIndexing(_,_,_) =>
          Console.out.println("EstimatedWrite: %s%s, ActualWrite: %s, EstimatedRead: %s%s, ActualRead: %s".format(
            estimatedWrite,samplingWrite,result(2)(4),estimatedReadInfo,samplingReadInfo,result(3)(3)))
        case Caching(_, _) =>
      }
    }
  }
}

