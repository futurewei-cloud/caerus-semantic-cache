package org.openinfralabs.caerus.cache.manager

import org.openinfralabs.caerus.cache.common.Tier.Tier
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
import org.openinfralabs.caerus.cache.common.{Caching, Candidate, FileSkippingIndexing, Repartitioning}

private[manager] abstract class Planner {
  private var id = 0

  protected def getId: Long = {
    val res: Long = id
    id += 1
    res
  }

  protected def getName(candidate: Candidate): String = {
    val candidateName = candidate match {
      case _: Repartitioning => "R"
      case _: FileSkippingIndexing => "FSI"
      case _: Caching => "C"
      case _ => throw new RuntimeException("Candidate %s not recognized.".format(candidate))
    }
    candidateName + getId.toString
  }

  def optimize(
    plan: CaerusPlan,
    all_contents: Map[Tier, Map[Candidate,String]],
    candidates: Seq[Candidate],
    capacity: Map[Tier, Long]
  ): Map[Tier, Map[Candidate,String]]
}
