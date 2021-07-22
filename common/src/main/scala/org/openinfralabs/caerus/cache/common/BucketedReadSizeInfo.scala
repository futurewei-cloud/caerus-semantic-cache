package org.openinfralabs.caerus.cache.common

import scala.language.higherKinds

case class BucketedReadSizeInfo(buckets: Array[(Any,Any)], initialSize: Long) extends ReadSizeInfo {
  override def getSize[T](minValue: T, maxValue: T): Long = {
    /*
    var nrBucketsUtilized: Int = 0
    buckets.foreach(bucket => {
      val minV = bucket._1.asInstanceOf[T]
      val maxV = bucket._2.asInstanceOf[T]
      if ((minV <= maxValue) && (maxV >= minValue)) {
        nrBucketsUtilized += 1
      }
    })
    initialSize*nrBucketsUtilized/buckets.length
     */
    0L
  }

  override def toString: String = "%s(%s) -> %s ".format(getClass.getSimpleName, initialSize, buckets.mkString("[", ",", "]"))
}
