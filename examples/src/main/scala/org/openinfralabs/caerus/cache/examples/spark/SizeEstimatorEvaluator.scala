package org.openinfralabs.caerus.cache.examples.spark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openinfralabs.caerus.cache.client.spark.{SemanticCache, Support}
import org.openinfralabs.caerus.cache.client.{BasicCandidateSelector, BasicSizeEstimator, CandidateSelector, SizeEstimator}
import org.openinfralabs.caerus.cache.common.Candidate
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
import org.openinfralabs.caerus.cache.examples.spark.gridpocket.{GridPocketSchemaProvider, GridPocketTrace}

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
    val logicalPlans: Seq[LogicalPlan] = jobs.map(job => job._2.queryExecution.logical)
    val supportedPlans: Seq[LogicalPlan] = logicalPlans.flatMap(plan => getSupportedPlans(plan))
    val plans: Seq[CaerusPlan] = supportedPlans.map(plan => SemanticCache.transform(plan))
    val candidates: Seq[Candidate] = plans.flatMap(candidateSelector.getCandidates)
    Console.out.println("Candidates:\n%s".format(candidates.mkString("\n")))

    // Calculate the estimates for all the candidates.
    val sizeEstimator: SizeEstimator = BasicSizeEstimator()

    // Run the candidates by using semantic cache api.
    // Use metrics from History server to collect actual results (see Metrics).
    // Output: candidate No, write size, estimated write size, read size, estimated read size (min "2019-01-01 00:00:00, max "2019-02-01 00:00:00)


  }
}
