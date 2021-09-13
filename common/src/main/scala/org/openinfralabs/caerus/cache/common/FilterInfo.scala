package org.openinfralabs.caerus.cache.common

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Not, Or}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Filter, UnaryNode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.openinfralabs.caerus.cache.common.plans.{CaerusAttribute, CaerusLoad, CaerusPlan}

import java.sql.{Date, Timestamp}
import javax.ws.rs.NotSupportedException
import scala.annotation.tailrec
import scala.collection.mutable

object FilterInfo {
  private def indexOfAttribute(attrib: Attribute): Int = attrib.exprId.id.toInt

  private def indexTransformationForChild(indices: Set[Int], startIndex: Int, child: CaerusPlan): Map[Int,Int] = {
    val res: mutable.HashMap[Int,Int] = mutable.HashMap.empty[Int,Int]
    indices.foreach(index => {
      val childIndex = index - startIndex
      if (childIndex >= 0 && childIndex < child.output.length)
        res(index) = indexOfAttribute(child.output(childIndex))
    })
    res.toMap[Int,Int]
  }

  private def transformIndexInCondition(condition: Expression, previousIndex: Int, newIndex: Int): Expression = {
    condition match {
      case attrib: CaerusAttribute if attrib.index == previousIndex =>
        attrib.withIndex(newIndex)
      case _ =>
        condition.withNewChildren(condition.children.map(transformIndexInCondition(_, previousIndex, newIndex)))
    }
  }

  private def propagateFilterInfoToChildren(
    filterInfo: Map[Int,Expression],
    children: Seq[CaerusPlan]
  ): Seq[Map[Int,Expression]] = {
    var currentAttribute = 0
    children.map(child => {
      val transformationMap: Map[Int,Int] = indexTransformationForChild(filterInfo.keys.toSet, currentAttribute, child)
      currentAttribute += child.output.length
      val newFilterInfo: mutable.HashMap[Int,Expression] = mutable.HashMap.empty[Int,Expression]
      filterInfo.keys.foreach(attribute =>
        if (transformationMap.contains(attribute)) {
          val newAttribute: Int = transformationMap(attribute)
          newFilterInfo(newAttribute) = transformIndexInCondition(filterInfo(attribute), attribute, newAttribute)
        }
      )
      newFilterInfo.toMap[Int,Expression]
    })
  }

  private def union(filterInfo1: Map[Int,Expression], filterInfo2: Map[Int,Expression]): Map[Int,Expression] = {
    val indices: Seq[Int] = filterInfo1.keys.toSet.union(filterInfo2.keys.toSet).toSeq
    val res: mutable.HashMap[Int,Expression] = mutable.HashMap.empty[Int,Expression]
    indices.foreach(index =>
      if (filterInfo1.contains(index) && filterInfo2.contains(index))
        res(index) = And(filterInfo1(index), filterInfo2(index))
      else if (filterInfo1.contains(index))
        res(index) = filterInfo1(index)
      else
        res(index) = filterInfo2(index)
    )
    res.toMap[Int,Expression]
  }

  private def intersect(filterInfo1: Map[Int,Expression], filterInfo2: Map[Int,Expression]): Map[Int,Expression] = {
    val indices: Seq[Int] = filterInfo1.keys.toSet.intersect(filterInfo2.keys.toSet).toSeq
    val res: mutable.HashMap[Int,Expression] = mutable.HashMap.empty[Int,Expression]
    indices.foreach(index => res(index) = Or(filterInfo1(index), filterInfo2(index)))
    res.toMap[Int,Expression]
  }

  private def getBounds(
    dataType: DataType,
    start: Option[Any],
    end: Option[Any],
    startClosed: Boolean,
    endClosed: Boolean
  ): OrderingInterval[Any] = {
    dataType match {
      case _: ByteType =>
        OrderingInterval[Byte](
          start.asInstanceOf[Option[Byte]],
          end.asInstanceOf[Option[Byte]],
          startClosed,
          endClosed
        ).asInstanceOf[OrderingInterval[Any]]
      case _: ShortType =>
        OrderingInterval[Short](
          start.asInstanceOf[Option[Short]],
          end.asInstanceOf[Option[Short]],
          startClosed,
          endClosed
        ).asInstanceOf[OrderingInterval[Any]]
      case _: IntegerType =>
        OrderingInterval[Int](
          start.asInstanceOf[Option[Int]],
          end.asInstanceOf[Option[Int]],
          startClosed,
          endClosed
        ).asInstanceOf[OrderingInterval[Any]]
      case _: LongType =>
        OrderingInterval[Long](
          start.asInstanceOf[Option[Long]],
          end.asInstanceOf[Option[Long]],
          startClosed,
          endClosed
        ).asInstanceOf[OrderingInterval[Any]]
      case _: FloatType =>
        OrderingInterval[Float](
          start.asInstanceOf[Option[Float]],
          end.asInstanceOf[Option[Float]],
          startClosed,
          endClosed
        ).asInstanceOf[OrderingInterval[Any]]
      case _: DoubleType =>
        OrderingInterval[Double](
          start.asInstanceOf[Option[Double]],
          end.asInstanceOf[Option[Double]],
          startClosed,
          endClosed
        ).asInstanceOf[OrderingInterval[Any]]
      case _: DecimalType =>
        OrderingInterval[BigDecimal](
          start.asInstanceOf[Option[BigDecimal]],
          end.asInstanceOf[Option[BigDecimal]],
          startClosed,
          endClosed
        ).asInstanceOf[OrderingInterval[Any]]
      case _: TimestampType =>
        val newStart: Option[Long] =
          if (start.isDefined)
            Some(DateTimeUtils.fromJavaTimestamp(start.get.asInstanceOf[Timestamp]))
          else
            None
        val newEnd: Option[Long] =
          if (end.isDefined)
            Some(DateTimeUtils.fromJavaTimestamp(end.get.asInstanceOf[Timestamp]))
          else
            None
        OrderingInterval[Long](newStart, newEnd, startClosed, endClosed).asInstanceOf[OrderingInterval[Any]]
      case _: DateType =>
        val newStart: Option[Int] =
          if (start.isDefined)
            Some(DateTimeUtils.fromJavaDate(start.get.asInstanceOf[Date]))
          else
            None
        val newEnd: Option[Int] =
          if (end.isDefined)
            Some(DateTimeUtils.fromJavaDate(end.get.asInstanceOf[Date]))
          else
            None
        OrderingInterval[Int](newStart, newEnd, startClosed, endClosed).asInstanceOf[OrderingInterval[Any]]
      case _ =>
        throw new UnsupportedOperationException("Cannot handle type %s".format(dataType))
    }
  }

  private def getBounds(expression: Expression, dataType: DataType): Seq[OrderingInterval[Any]] = {
    val converter: Any=>Any = CatalystTypeConverters.createToScalaConverter(dataType)
    expression match {
      case EqualTo(attrib: CaerusAttribute, lit: Literal) =>
        assert(attrib.dataType == dataType)
        val value: Any = converter(lit.value)
        Seq(getBounds(dataType, Some(value), Some(value), startClosed=true, endClosed=true))
      case EqualNullSafe(attrib: CaerusAttribute, lit: Literal) =>
        assert(attrib.dataType == dataType)
        val value: Any = converter(lit.value)
        Seq(getBounds(dataType, Some(value), Some(value), startClosed=true, endClosed=true))
      case GreaterThan(attrib: CaerusAttribute, lit: Literal) =>
        assert(attrib.dataType == dataType)
        val value: Any = converter(lit.value)
        Seq(getBounds(dataType, Some(value), None, startClosed=false, endClosed=false))
      case LessThan(attrib: CaerusAttribute, lit: Literal) =>
        assert(attrib.dataType == dataType)
        val value: Any = converter(lit.value)
        Seq(getBounds(dataType, None, Some(value), startClosed=false, endClosed=false))
      case GreaterThanOrEqual(attrib: CaerusAttribute, lit: Literal) =>
        assert(attrib.dataType == dataType)
        val value: Any = converter(lit.value)
        Seq(getBounds(dataType, Some(value), None, startClosed=true, endClosed=false))
      case LessThanOrEqual(attrib: CaerusAttribute, lit: Literal) =>
        assert(attrib.dataType == dataType)
        val value: Any = converter(lit.value)
        Seq(getBounds(dataType, None, Some(value), startClosed=false, endClosed=true))
      case And(left: Expression, right: Expression) =>
        val boundsLeft: Seq[OrderingInterval[Any]] = getBounds(left, dataType)
        val boundsRight: Seq[OrderingInterval[Any]] = getBounds(right, dataType)
        boundsLeft.flatMap(orderingIntervalLeft => boundsRight.flatMap(orderingIntervalLeft.intersect(_)))
      case Or(left: Expression, right: Expression) =>
        val boundsLeft: Seq[OrderingInterval[Any]] = getBounds(left, dataType)
        val boundsRight: Seq[OrderingInterval[Any]] = getBounds(right, dataType)
        var res: Seq[OrderingInterval[Any]] = boundsRight
        boundsLeft.foreach(interval => res = interval.merge(res))
        res
      case Not(child: Expression) =>
        child match {
          case and: And =>
            val newExpression: Expression = Or(Not(and.left), Not(and.right))
            getBounds(newExpression, dataType)
          case or: Or =>
            val newExpression: Expression = And(Not(or.left), Not(or.right))
            getBounds(newExpression, dataType)
          case _ =>
            val childBounds = getBounds(child, dataType)
            assert(childBounds.length == 1)
            childBounds.head.complement
        }
        val boundsChild: Seq[OrderingInterval[Any]] = getBounds(child, dataType)
        boundsChild.flatMap(interval => interval.complement)
      case _ => Seq.empty[OrderingInterval[Any]]
    }
  }

  @tailrec
  private def get(
    caerusPlan: CaerusPlan,
    candidate: Candidate,
    filterInfo: Map[Int,Expression]
  ): Seq[OrderingInterval[Any]] = {
    val (targetPlan, targetAttrib): (CaerusPlan, Option[(Int,DataType)]) = candidate match {
      case Repartitioning(source, index, _) => (source, Some((index,source.output(index).dataType)))
      case FileSkippingIndexing(source, index, _) => (source, Some((index,source.output(index).dataType)))
      case Caching(cachedPlan, _) => (cachedPlan, None)
    }
    caerusPlan match {
      case _ if caerusPlan.equals(targetPlan) && targetAttrib.isDefined =>
        val targetIndex: Int = targetAttrib.get._1
        val targetDataType: DataType = targetAttrib.get._2
        getBounds(filterInfo(targetIndex), targetDataType)
      case _ if caerusPlan.equals(targetPlan) =>
        Seq.empty[OrderingInterval[Any]]
      case _: CaerusLoad =>
        Seq.empty[OrderingInterval[Any]]
      case filter: Filter =>
        val additionalFilterInfo: Map[Int,Expression] = normalizeCondition(filter.condition)
        val newFilterInfo: Map[Int,Expression] = union(filterInfo, additionalFilterInfo)
        val childFilterInfo: Map[Int,Expression] = propagateFilterInfoToChildren(newFilterInfo, filter.children).head
        get(filter.child, candidate, childFilterInfo)
      case unary: UnaryNode =>
        val childFilterInfo: Map[Int,Expression] = propagateFilterInfoToChildren(filterInfo, unary.children).head
        get(unary.child, candidate, childFilterInfo)
      case binary: BinaryNode =>
        throw new NotSupportedException("Binary node %s is not supported yet.".format(binary))
    }
  }

  /**
   * Find all filter expressions in a filter condition for each different attribute that can be utilized in Semantic
   * Cache (size estimation, file-skipping, etc.).
   * @param condition Filter condition that is utilized.
   * @return a map from the index of an attribute to its corresponding expression.
   */
  def normalizeCondition(condition: Expression): Map[Int,Expression] = {
    condition match {
      case EqualTo(attrib: CaerusAttribute, lit: Literal) => Map(attrib.index -> EqualTo(attrib,lit))
      case EqualTo(lit: Literal, attrib: CaerusAttribute) => Map(attrib.index -> EqualTo(attrib,lit))
      case EqualNullSafe(attrib: CaerusAttribute, lit: Literal) => Map(attrib.index -> EqualNullSafe(attrib,lit))
      case EqualNullSafe(lit: Literal, attrib: CaerusAttribute) => Map(attrib.index -> EqualNullSafe(attrib,lit))
      case GreaterThan(attrib: CaerusAttribute, lit: Literal) => Map(attrib.index -> GreaterThan(attrib,lit))
      case GreaterThan(lit: Literal, attrib: CaerusAttribute) => Map(attrib.index -> LessThan(attrib,lit))
      case LessThan(attrib: CaerusAttribute, lit: Literal) => Map(attrib.index -> LessThan(attrib,lit))
      case LessThan(lit: Literal, attrib: CaerusAttribute) => Map(attrib.index -> GreaterThan(attrib,lit))
      case GreaterThanOrEqual(attrib: CaerusAttribute, lit: Literal) =>
        Map(attrib.index -> GreaterThanOrEqual(attrib,lit))
      case GreaterThanOrEqual(lit: Literal, attrib: CaerusAttribute) =>
        Map(attrib.index -> LessThanOrEqual(attrib,lit))
      case LessThanOrEqual(attrib: CaerusAttribute, lit: Literal) => Map(attrib.index -> LessThanOrEqual(attrib,lit))
      case LessThanOrEqual(lit: Literal, attrib: CaerusAttribute) => Map(attrib.index -> GreaterThanOrEqual(attrib,lit))
      case And(left: Expression, right: Expression) =>
        val normalizedLeft: Map[Int,Expression] = normalizeCondition(left)
        val normalizedRight: Map[Int,Expression] = normalizeCondition(right)
        union(normalizedLeft, normalizedRight)
      case Or(left: Expression, right: Expression) =>
        val normalizedLeft: Map[Int,Expression] = normalizeCondition(left)
        val normalizedRight: Map[Int,Expression] = normalizeCondition(right)
        intersect(normalizedLeft, normalizedRight)
      case Not(child: Expression) =>
        val normalizedChild: Map[Int,Expression] = normalizeCondition(child)
        val indices: Seq[Int] = normalizedChild.keys.toSeq
        val res: mutable.HashMap[Int,Expression] = mutable.HashMap.empty[Int,Expression]
        indices.foreach(index => res(index) = Not(normalizedChild(index)))
        res.toMap[Int,Expression]
      case _ => Map.empty[Int,Expression]
    }
  }

  /**
   * Get all the filter attributes for the specified candidate.
   */
  def get(caerusPlan: CaerusPlan, candidate: Candidate): Seq[OrderingInterval[Any]] = {
    get(caerusPlan, candidate, Map.empty[Int, Expression])
  }
}
