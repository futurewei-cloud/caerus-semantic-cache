package org.openinfralabs.caerus.cache.client

import org.openinfralabs.caerus.cache.common.Candidate

/**
 * Size estimator for Caerus Plans.
 */
abstract class SizeEstimator {
  /**
   * Estimate and update read and write sizes for specific candidate.
   * @param candidate Candidate to estimate and updates sizes for.
   */
  def estimateSize(candidate: Candidate): Unit
}
