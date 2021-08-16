package org.openinfralabs.caerus.cache.examples.spark

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
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

  def main(args: Array[String]): Unit = {
    // Take arguments.
    if (args.length < 8) {
      Console.err.println("Wrong number of args. Usage: <spark URI> <semantic cache URI> <year> <input path> " +
        "<output path> <history path> <results path> <size estimator type> <size estimator args")
      System.exit(-1)
    }
    val sparkURI: String = args(0)
    val semanticCacheURI: String = args(1)
    val year: Int = args(2).toInt
    val inputPath: String = args(3)
    val outputPath: String = args(4)
    val historyPath: String = args(5)
    val resultsPath: String = args(6)
    val sizeEstimatorType: String = args(7)

    // Initiate spark session.
    val spark: SparkSession =
      SparkSession.builder()
        .master(sparkURI)
        .appName(name = "SizeEstimatorEvaluator")
        .getOrCreate()

    // Create size estimator object.
    val sizeEstimator: SizeEstimator = sizeEstimatorType match {
      case "basic" => BasicSizeEstimator()
      case "sampling" =>
        val sampleSize: Int = args(8).toInt
        SamplingSizeEstimator(spark, sampleSize)
      case _ =>
        Console.err.println("The %s is not recognised as a size estimator type. Supported types are basic, sampling.")
        System.exit(-1)
        return
    }

    // Create trace.
    var id: Int = -1
    val jobs: Seq[(Int, String, DataFrame)] =
      GridPocketTrace.createTrace(spark, new GridPocketSchemaProvider, year, inputPath).map(job => {
        id += 1
        (id, job._1, job._2)
      })

    // Find all the candidates from these plans.
    val candidateSelector: CandidateSelector = BasicCandidateSelector()
    val logicalPlans: Seq[(Int, LogicalPlan)] = jobs.map(job => (job._1, job._3.queryExecution.optimizedPlan))
    val supportedPlans: Seq[(Int, LogicalPlan)] = logicalPlans.flatMap(logicalPlan => {
      getSupportedPlans(logicalPlan._2).map(supportedPlan => (logicalPlan._1, supportedPlan))
    })
    val caerusPlans: Seq[(Int, CaerusPlan)] = supportedPlans.map(supportedPlan => {
      (supportedPlan._1, SemanticCache.transform(supportedPlan._2))
    })
    val candidateInfos: Seq[(Int, Int, LogicalPlan, Candidate)] =
      caerusPlans.indices.flatMap(i => {
        candidateSelector.getCandidates(supportedPlans(i)._2, caerusPlans(i)._2).distinct.map(candidate => {
          assert(caerusPlans(i)._1 == supportedPlans(i)._1)
          (caerusPlans(i)._1, i, candidate._1, candidate._2)
        })
      })

    // Calculate the estimates for all the candidates.
    candidateInfos.foreach(candidate => {
      sizeEstimator.estimateSize(candidate._3, candidate._4)
    })

    // Run the candidates by using semantic cache api.
    if (semanticCacheURI == "none") {
      Console.err.println("Semantic Cache URI should be defined.")
      System.exit(-1)
    }
    val semanticCache: SemanticCache = new SemanticCache(spark, semanticCacheURI)

    // Output:
    // candidate No, write size, estimated write size, read size, estimated read size (min "2019-01-01 00:00:00,
    // max "2019-02-01 00:00:00)
    val schema: StructType = new GridPocketSchemaProvider().getSchema
    val loadDF: DataFrame = spark.read.option("header", "true").schema(schema).csv(inputPath)
    candidateInfos.zipWithIndex.foreach(candidateInfo => {
      val jobID: Int = candidateInfo._1._1
      val planID: Int = candidateInfo._1._2
      val logicalPlan: LogicalPlan = candidateInfo._1._3
      val candidate: Candidate = candidateInfo._1._4
      val candidateID: Int = candidateInfo._2
      val tempName: String = "temp-%d".format(candidateID)
      Console.out.println("Running candidate: %s".format(candidate))
      candidate match {
        case Repartitioning(_, index, _) =>
          semanticCache.repartitioning(loadDF, loadDF.columns(index), Tier.STORAGE_DISK, tempName)
        case FileSkippingIndexing(_, index, _) =>
          semanticCache.fileSkippingIndexing(loadDF, loadDF.columns(index), Tier.STORAGE_DISK, tempName)
        case Caching(_, _) =>
          semanticCache.cacheIntermediateData(loadDF, Tier.STORAGE_DISK, tempName)
      }
      Console.out.println(semanticCache.status)
      jobs(planID)._3.write.option("header","true").csv(outputPath + Path.SEPARATOR + tempName)
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

    for ((_,_,_,candidate) <- candidateInfos) {
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

