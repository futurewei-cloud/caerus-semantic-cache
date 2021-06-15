package org.openinfralabs.caerus.cache.examples.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openinfralabs.caerus.cache.client.spark.SemanticCache
import org.openinfralabs.caerus.cache.client.{BasicCandidateSelector, BasicSizeEstimator, CandidateSelector, SizeEstimator}
import org.openinfralabs.caerus.cache.common.Candidate
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
import org.openinfralabs.caerus.cache.examples.spark.gridpocket.{GridPocketSchemaProvider, GridPocketTrace}

object SizeEstimatorEvaluator {
  def main(args: Array[ String ]) {
    // Take arguments.
    if (args.length != 6) {
      System.exit(0)
    }
    val sparkURI: String = args(0)
    val semanticCacheURI: String = args(1)
    val year: Int = args(2).toInt
    val inputPath: String = args(3)
    val outputPath: String = args(4)
    val resultsPath: String = args(5)

    // Initiate spark session.
    val spark: SparkSession =
      SparkSession.builder()
        .master(sparkURI)
        .appName(name = "GridPocketTrace")
        .getOrCreate()

    // Create trace.
    val jobs: Seq[(String, DataFrame)] = GridPocketTrace.createTrace(spark, new GridPocketSchemaProvider, year,
      inputPath)

    // Find all the candidates from these plans.
    val candidateSelector: CandidateSelector = BasicCandidateSelector()
    val plans: Seq[CaerusPlan] = jobs.map(job => SemanticCache.transform(job._2.queryExecution.logical))
    val candidates: Seq[Candidate] = plans.flatMap(candidateSelector.getCandidates)

    // Calculate the estimates for all the candidates.
    val sizeEstimator: SizeEstimator = BasicSizeEstimator()

    // Run the candidates by using semantic cache api.
    // Use metrics from History server to collect actual results (see Metrics).
    // Output: candidate No, write size, estimated write size, read size, estimated read size (min "2019-01-01 00:00:00, max "2019-02-01 00:00:00)


  }
}
