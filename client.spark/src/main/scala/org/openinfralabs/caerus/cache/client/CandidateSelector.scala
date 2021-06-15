package org.openinfralabs.caerus.cache.client

import org.openinfralabs.caerus.cache.common.Candidate
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

/**
 * Candidate selector for Semantic Cache client.
 */
abstract class CandidateSelector {
  /**
   * Estimate all possible candidates for specific plan.
   * @param plan Plan to estimate candidates for.
   * @return a sequence of all the possible candidates for the provided plan.
   */
  def getCandidates(plan: CaerusPlan): Seq[Candidate]
}
