package org.openinfralabs.caerus.cache.client

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.openinfralabs.caerus.cache.common.plans.{CaerusPlan, CaerusSourceLoad}
import org.openinfralabs.caerus.cache.common.{Caching, Candidate, FileSkippingIndexing, Repartitioning}

/**
 * Size estimator for Spark's Semantic Cache Client. SamplingSizeEstimator should create relatively small samples based
 * on a customized value (@ratio). Then it should instantiate the candidate for this sample.
 *
 * The writeSizeInfo should be derived from the size of the specific candidate instantiation
 * ((sample candidate size/sample source input size) * real source input size).
 *
 * The readSizeInfo should be derived differently for each technique.
 *
 * For repartitioning, the SamplingSizeEstimator should observe how many partitions are used for a specific query and
 * return:
 * (nrPartitionsUsed/nrPartitions) * real source input size
 * For example if you have repartitions for temperature [0-9], [10,19], [20-29] and a query asks for an estimation for
 * temperatures in [5,10]. Then there should be only two partitions selected. So, the answer should be:
 * 2/3*real source input size
 *
 * For file-skipping, the SamplingSizeEstimator should observe how many partitions are used for a specific query and
 * return:
 * (nrPartitionsUsed/nrPartitions) * real source input size
 * For example if you have files for temperature [0-15], [8,20], [12-30] and a query asks for an estimation for
 * temperatures in [5,10]. Then there should be only two files selected. So, the answer should be:
 * 2/3*real source input size
 *
 * For caching, the SamplingSizeEstimator should return the same value as the writeSizeInfo no matter what is the query.
 */
class SamplingSizeEstimator(spark: SparkSession, sampleSize: Int) extends SizeEstimator {
  private def detectSources(inputPlan: LogicalPlan, plan: CaerusPlan): Seq[RDD[InternalRow]] = {
    plan match {
      case caerusSourceLoad: CaerusSourceLoad =>
        assert(inputPlan.isInstanceOf[LogicalRelation])
        val logicalRelation: LogicalRelation = inputPlan.asInstanceOf[LogicalRelation]
        assert(logicalRelation.relation.isInstanceOf[HadoopFsRelation])
        val hadoopFsRelation = logicalRelation.relation.asInstanceOf[HadoopFsRelation]
        val loadDF: DataFrame = spark.read
          .format(caerusSourceLoad.format)
          .options(hadoopFsRelation.options)
          .schema(hadoopFsRelation.dataSchema)
          .load(caerusSourceLoad.sources.map(source => source.path):_*)
        Seq(loadDF.queryExecution.toRdd)
      case _ =>
        plan.children.indices.flatMap(i => detectSources(inputPlan.children(i), plan.children(i)))
    }
  }

  /**
   * Estimate and update read and write sizes for specific candidate.
   *
   * @param candidate Candidate to estimate and updates sizes for.
   */
  override def estimateSize(inputPlan: LogicalPlan, candidate: Candidate): Unit = {
    candidate match {
      case Repartitioning(caerusSourceLoad, _, _) =>
        assert(inputPlan.isInstanceOf[LogicalRelation])
        val logicalRelation: LogicalRelation = inputPlan.asInstanceOf[LogicalRelation]
        assert(logicalRelation.relation.isInstanceOf[HadoopFsRelation])
        val hadoopFsRelation = logicalRelation.relation.asInstanceOf[HadoopFsRelation]
        val loadDF: DataFrame = spark.read
          .format(caerusSourceLoad.format)
          .options(hadoopFsRelation.options)
          .schema(hadoopFsRelation.dataSchema)
          .load(caerusSourceLoad.sources.map(source => source.path):_*)
        val rdd: RDD[InternalRow] = loadDF.queryExecution.toRdd
      case FileSkippingIndexing(caerusSourceLoad, _, _) =>
      case Caching(plan, cachingSizeInfo) =>
        detectSources(inputPlan, plan)

    }
  }
}
