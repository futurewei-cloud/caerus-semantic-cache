package org.openinfralabs.caerus.cache.manager

import org.openinfralabs.caerus.cache.common.Candidate
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

private[manager] abstract class Planner {
  def optimize(plan: CaerusPlan, contents: Map[Candidate,String], candidates: Seq[Candidate]): CaerusPlan
}
