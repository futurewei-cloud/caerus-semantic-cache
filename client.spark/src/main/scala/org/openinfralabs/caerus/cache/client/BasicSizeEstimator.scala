package org.openinfralabs.caerus.cache.client

import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan}
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
import org.openinfralabs.caerus.cache.common.{BasicReadSizeInfo, Caching, Candidate, FileSkippingIndexing, ReadSizeInfo, Repartitioning, SizeInfo}

/**
 * Basic size estimator for Semantic Cache client.
 */
case class BasicSizeEstimator() extends SizeEstimator {
  private var hasFilter: Boolean = false
  private var hasAggregate: Boolean = false
  private var sourceSize: Long = 0L

  private def detectNodes(plan: CaerusPlan): Unit = {
    plan match {
      case _: Aggregate =>
        hasAggregate = true
      case _: Filter =>
        hasFilter = true
      case _ =>
    }
    plan.children.foreach(detectNodes)
  }

  /**
   * Estimate and update read and write sizes for specific candidate. The prediction is based on spark estimation
   * predictions and some simple hypothesis made for the read sizes.
   * @param inputPlan Plan from which candidate is derived.
   * @param candidate Candidates to estimate and update sizes for.
   */
  override def estimateSize(inputPlan: LogicalPlan, candidate: Candidate): Unit = {
    candidate match {
      case repartitioning: Repartitioning =>
        sourceSize = repartitioning.source.size
        val writeSize: Long = sourceSize
        val readSizeInfo: ReadSizeInfo = BasicReadSizeInfo(sourceSize / 10)
        val sizeInfo: SizeInfo = SizeInfo(writeSize, readSizeInfo)
        repartitioning.sizeInfo = Some(sizeInfo)
      case fileSkippingIndexing: FileSkippingIndexing =>
        sourceSize = fileSkippingIndexing.source.size
        val writeSize: Long = fileSkippingIndexing.source.sources.length * 128L + 512L
        val readSizeInfo: ReadSizeInfo = BasicReadSizeInfo(sourceSize / 2)
        val sizeInfo: SizeInfo = SizeInfo(writeSize, readSizeInfo)
        fileSkippingIndexing.sizeInfo = Some(sizeInfo)
      case caching: Caching =>
        hasAggregate = false
        hasFilter = false
        detectNodes(caching.plan)
        val writeSize: Long = caching.plan.stats.sizeInBytes.toLong
        val readSizeInfo: ReadSizeInfo = {
          if (hasAggregate && hasFilter)
            BasicReadSizeInfo(writeSize / 50)
          else if (hasAggregate)
            BasicReadSizeInfo(writeSize / 10)
          else if (hasFilter)
            BasicReadSizeInfo(writeSize / 5)
          else
            BasicReadSizeInfo(writeSize)
        }
        val sizeInfo = SizeInfo(writeSize, readSizeInfo)
        caching.sizeInfo = Some(sizeInfo)
      case _ =>
        throw new RuntimeException("Candidate %s is not supported.".format(candidate))
    }
  }
}
