package org.openinfralabs.caerus.cache.common

case class BasicReadSizeInfo(readSize: Long) extends ReadSizeInfo {
  override def getSize[T](minValue: T, maxValue: T): Long = readSize
  override def toString: String = "%s(%s)".format(getClass.getSimpleName, readSize)
}
