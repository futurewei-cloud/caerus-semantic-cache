package org.openinfralabs.caerus.cache.common

case class BasicReadSizeInfo(readSize: Long) extends ReadSizeInfo {
  override def getSize(orderingInterval: Seq[OrderingInterval[Any]]): Long = readSize
  override def toString: String = "%s(%s)".format(getClass.getSimpleName, readSize)
}
