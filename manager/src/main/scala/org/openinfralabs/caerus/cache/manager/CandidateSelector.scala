package org.openinfralabs.caerus.cache.manager

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.openinfralabs.caerus.cache.common.Candidate

private[manager] abstract class CandidateSelector {
  def getCandidates(plan: LogicalPlan): Seq[Candidate]
}
