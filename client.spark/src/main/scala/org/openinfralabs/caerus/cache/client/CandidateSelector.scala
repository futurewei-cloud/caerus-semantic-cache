package org.openinfralabs.caerus.cache.client

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.openinfralabs.caerus.cache.common.Candidate
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

/**
 * Candidate selector for Semantic Cache client.
 */
abstract class CandidateSelector {
  /**
   * Estimate all possible candidates for specific plan.
   * @param inputPlan Initial plan for which plan was transformed from.
   * @param plan Plan to estimate candidates for.
   * @return a sequence of all the possible candidates for the provided plan.
   */
  def getCandidates(inputPlan: LogicalPlan, plan: CaerusPlan): Seq[(LogicalPlan,Candidate)]
}
