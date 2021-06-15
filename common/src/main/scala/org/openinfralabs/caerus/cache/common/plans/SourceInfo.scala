package org.openinfralabs.caerus.cache.common.plans

import org.apache.spark.sql.catalyst.trees.TreeNode

case class SourceInfo(path: String, creationTime: Long, start: Long, length: Long) extends TreeNode[SourceInfo] {
  override def children: Seq[SourceInfo] = Seq.empty[SourceInfo]

  override def simpleStringWithNodeId(): String = "(%s,%s,%s,%s)".format(path, creationTime, start, length)

  override def verboseString(maxFields: Int): String = simpleStringWithNodeId()

  override def toString: String = simpleStringWithNodeId()

  override def canEqual(other: Any): Boolean = other.isInstanceOf[SourceInfo]

  override def equals(other: Any): Boolean = {
    other match {
      case otherSourceInfo: SourceInfo =>
        otherSourceInfo.path == path &&
          otherSourceInfo.creationTime == creationTime &&
          otherSourceInfo.start == start &&
          otherSourceInfo.length == length
      case _ =>
        false
    }
  }

  /**
   * Find common records between this and other source.
   *
   * @param other : Source to compare with.
   * @return (commonPart, this records only, other records only)
   */
  def commonRecords(other: SourceInfo): (Option[SourceInfo], Option[SourceInfo], Option[SourceInfo]) = {
    if (path == other.path && creationTime == other.creationTime) {
      val diffLength: Long = length - other.length
      if (diffLength == 0)
        (Some(this), None, None)
      else if (diffLength > 0)
        (Some(other), Some(SourceInfo(path, creationTime, other.length, diffLength)), None)
      else
        (Some(this), None, Some(SourceInfo(path, creationTime, this.length, -diffLength)))
    } else {
      (None, Some(this), Some(other))
    }
  }
}

object SourceInfo {
  def subsetOf(first: Set[SourceInfo], second: Set[SourceInfo]): Option[Set[SourceInfo]] = {
    if (first.subsetOf(second))
      Some(second -- first)
    else
      None
  }
}
