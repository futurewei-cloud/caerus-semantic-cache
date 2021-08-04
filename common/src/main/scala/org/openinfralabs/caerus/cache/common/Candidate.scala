package org.openinfralabs.caerus.cache.common

import org.openinfralabs.caerus.cache.common.plans.{CaerusPlan, CaerusSourceLoad}

abstract class Candidate(var sizeInfo: Option[SizeInfo]) {
  def toString: String

  def toJSON: String = {
    val caerusPlanSerDe: CaerusPlanSerDe = new CaerusPlanSerDe
    caerusPlanSerDe.serializeCandidate(this)
  }

  def withNewSizeInfo(newSizeInfo: SizeInfo): Candidate
}

object Candidate {
  def fromJSON(json: String): Candidate = {
    val caerusPlanSerDe = new CaerusPlanSerDe
    caerusPlanSerDe.deserializeCandidate(json)
  }
}

case class Repartitioning(source: CaerusSourceLoad, index: Int, repSizeInfo: Option[SizeInfo] = None)
  extends Candidate(repSizeInfo) {
  override def toString: String = "%s(%s,%s,%s)".format(getClass.getSimpleName, source, index, sizeInfo)

  def canEqual(other: Any): Boolean = other.isInstanceOf[Repartitioning]

  override def equals(other: Any): Boolean = {
    other match {
      case Repartitioning(otherSource, otherIndex, _) => source == otherSource && otherIndex == index
      case _ => false
    }
  }

  override def withNewSizeInfo(newSizeInfo: SizeInfo): Candidate = {
    Repartitioning(source, index, Some(newSizeInfo))
  }
}

case class FileSkippingIndexing(source: CaerusSourceLoad, index: Int, fsSizeInfo: Option[SizeInfo] = None)
  extends Candidate(fsSizeInfo) {
  override def toString: String = "%s(%s,%s,%s)".format(getClass.getSimpleName, source, index, sizeInfo)

  def canEqual(other: Any): Boolean = other.isInstanceOf[FileSkippingIndexing]

  override def equals(other: Any): Boolean = other match {
    case FileSkippingIndexing(otherSource, otherIndex, _) => source == otherSource && otherIndex == index
    case _ => false
  }

  override def withNewSizeInfo(newSizeInfo: SizeInfo): Candidate = {
    FileSkippingIndexing(source, index, Some(newSizeInfo))
  }
}

case class Caching(plan: CaerusPlan, cachingSizeInfo: Option[SizeInfo] = None) extends Candidate(cachingSizeInfo) {
  override def toString: String = "%s(%s,%s)".format(getClass.getSimpleName, plan, sizeInfo)

  def canEqual(other: Any): Boolean = other.isInstanceOf[Caching]

  override def equals(other: Any): Boolean = other match {
    case Caching(otherPlan, _) => plan.sameResult(otherPlan)
    case _ => false
  }

  override def withNewSizeInfo(newSizeInfo: SizeInfo): Candidate = {
    Caching(plan, Some(newSizeInfo))
  }
}
