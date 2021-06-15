package org.openinfralabs.caerus.cache.common

case class SizeInfo(var writeSize: Long, readSizeInfo: ReadSizeInfo) {
  override def toString: String = "%s,%s".format(writeSize, readSizeInfo)

  def toJSON: String = {
    val caerusPlanSerDe = new CaerusPlanSerDe
    caerusPlanSerDe.serializeSizeInfo(this)
  }
}

object SizeInfo {
  def fromJSON(json: String): SizeInfo = {
    val caerusPlanSerDe = new CaerusPlanSerDe
    caerusPlanSerDe.deserializeSizeInfo(json)
  }
}