package org.openinfralabs.caerus.cache.manager

import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
import org.openinfralabs.caerus.cache.common.Candidate

/**
 * Creates LRC policy for Candidate selection and eviction from Semantic Cache.
 */
class LRCPlanner extends Planner {
  /**
   *
   * @param plan Current plan to optimize.
   * @param contents Current contents of Semantic Cache.
   * @param candidates Write candidates for current plan.
   * @param capacity Capacity of the Semantic Cache in bytes.
   * @return optimized plan
   */
  override def optimize(
    plan: CaerusPlan,
    contents: Map[Candidate,String],
    candidates: Seq[Candidate],
    capacity: Long
  ): CaerusPlan = {
    // 1. Find top candidate.
    // Create a reference map (Candidate -> Long).
    // For each candidate in candidates:
    // Add candidate in contents with name "temp".
    // Optimize plans in the future prediction (add the current one) adding one reference
    // to temp when necessary.
    // Pick the maximum reference count candidate as top candidate.
    // 2. Find eviction candidate (if capacity is smaller than size used).
    // Add top candidate to contents with proper name (use getName function on Planner).
    // Create a reference map (String -> Long).
    // Optimize plans in the future prediction adding one reference in all contents when
    // necessary.
    // Create a reference map (Candidate -> Long)
    // Go through contents and update the new reference map.
    // Evict candidates with minimum reference count until capacity constraints are satisfied.
    plan
  }
}
