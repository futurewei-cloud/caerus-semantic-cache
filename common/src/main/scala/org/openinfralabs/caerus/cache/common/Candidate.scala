package org.openinfralabs.caerus.cache.common

import org.openinfralabs.caerus.cache.common.plans.{CaerusPlan, CaerusSourceLoad}

abstract class Candidate {
  def toString: String

  def toJSON: String = {
    val caerusPlanSerDe: CaerusPlanSerDe = new CaerusPlanSerDe
    caerusPlanSerDe.serializeCandidate(this)
  }
}

object Candidate {
  def fromJSON(json: String): Candidate = {
    val caerusPlanSerDe = new CaerusPlanSerDe
    caerusPlanSerDe.deserializeCandidate(json)
  }
}

case class Repartitioning(source: CaerusSourceLoad, index: Int) extends Candidate {
  override def toString: String = "Repartitioning(%s,%s)".format(source, index)
}

case class FileSkippingIndexing(source: CaerusSourceLoad, index: Int) extends Candidate {
  override def toString: String = "FileSkippingIndexing(%s,%s)".format(source, index)
}

case class Caching(plan: CaerusPlan) extends Candidate {
  override def toString: String = "Caching(%s)".format(plan)
}
