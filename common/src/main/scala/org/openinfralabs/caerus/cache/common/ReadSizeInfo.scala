package org.openinfralabs.caerus.cache.common

abstract class ReadSizeInfo {
  // TODO: Replace min/max estimation with distinct ranges.
  def getSize[T](minValue: T, maxValue: T): Long

  def toJSON: String = {
    val caerusPlanSerDe = new CaerusPlanSerDe
    caerusPlanSerDe.serializeReadSizeInfo(this)
  }
}

object ReadSizeInfo {
  def fromJSON(json: String): ReadSizeInfo = {
    val caerusPlanSerDe = new CaerusPlanSerDe
    caerusPlanSerDe.deserializeReadSizeInfo(json)
  }
}
