package org.openinfralabs.caerus.cache.client

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.openinfralabs.caerus.cache.common.Candidate

/**
 * Size estimator for Caerus Plans.
 */
abstract class SizeEstimator {
  /**
   * Estimate and update read and write sizes for specific candidate.
   * @param inputPlan Plan from which candidate is derived.
   * @param candidate Candidate to estimate and updates sizes for.
   */
  def estimateSize(inputPlan: LogicalPlan, candidate: Candidate): Unit
}
