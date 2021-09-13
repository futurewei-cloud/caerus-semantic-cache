package org.openinfralabs.caerus.cache.common

abstract class ReadSizeInfo {
  def getSize(orderingInterval: Seq[OrderingInterval[Any]]): Long

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
