package org.openinfralabs.caerus.cache.common

import scala.language.higherKinds

case class BucketedReadSizeInfo(buckets: Array[(AnyRef,AnyRef)], initialSize: Long) extends ReadSizeInfo {
  override def getSize(orderingIntervals: Seq[OrderingInterval[Any]]): Long = {
    var nrBucketsUtilized: Int = 0
    buckets.foreach(bucket => {
      val minV = bucket._1
      val maxV = bucket._2
      if (orderingIntervals.flatMap(_.intersect(minV, maxV)).nonEmpty)
        nrBucketsUtilized += 1
    })
    initialSize*nrBucketsUtilized/buckets.length
  }

  override def toString: String = "%s(%s) -> %s ".format(getClass.getSimpleName, initialSize, buckets.mkString("[", ",", "]"))
}
