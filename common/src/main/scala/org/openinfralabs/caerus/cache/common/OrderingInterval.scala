package org.openinfralabs.caerus.cache.common

import scala.math.Ordered.orderingToOrdered

private[common] case class OrderingInterval[T: Ordering](
  start: Option[T],
  end: Option[T],
  isStartClosed: Boolean,
  isEndClosed: Boolean
) {
  assert(start.isEmpty || end.isEmpty || start.get < end.get || (start.get == end.get && isStartClosed && isEndClosed))

  def complement: Seq[OrderingInterval[T]] = {
    if (start.isDefined && end.isDefined)
      Seq(
        OrderingInterval[T](None, start, isStartClosed=false, isEndClosed = !isStartClosed),
        OrderingInterval[T](end, None, !isEndClosed, isEndClosed=false)
      )
    else if (start.isDefined)
      Seq(OrderingInterval[T](None, start, isStartClosed=false, isEndClosed = !isStartClosed))
    else if (end.isDefined)
      Seq(OrderingInterval[T](end, None, !isEndClosed, isEndClosed=false))
    else
      Seq.empty[OrderingInterval[T]]
  }

  def intersect(otherMin: Any, otherMax: Any): Option[OrderingInterval[T]] = {
    val otherInterval: OrderingInterval[T] = OrderingInterval(
      Some(otherMin.asInstanceOf[T]),
      Some(otherMax.asInstanceOf[T]),
      isStartClosed=true,
      isEndClosed=true
    )
    intersect(otherInterval.asInstanceOf[OrderingInterval[Any]])
  }

  def intersect(otherInterval: OrderingInterval[Any]): Option[OrderingInterval[T]] = {
    val interval: OrderingInterval[T] = otherInterval.asInstanceOf[OrderingInterval[T]]
    OrderingInterval.intersect[T](this, interval)
  }

  // The sequence should be ordered in order for this method to work.
  def merge(otherIntervals: Seq[OrderingInterval[Any]]): Seq[OrderingInterval[T]] = {
    if (otherIntervals.isEmpty) {
      Seq(this)
    } else {
      val interval: OrderingInterval[T] = otherIntervals.head.asInstanceOf[OrderingInterval[T]]
      if (start.isDefined && interval.end.isDefined &&
        (start.get > interval.end.get || (start.get == interval.end.get && !(isStartClosed && interval.isEndClosed)))) {
        interval +: merge(otherIntervals.tail)
      } else if (end.isDefined && interval.start.isDefined &&
        (interval.start.get > end.get || (interval.start.get == end.get && !(interval.isStartClosed && isEndClosed)))) {
        this +: otherIntervals.asInstanceOf[Seq[OrderingInterval[T]]]
      } else {
        val (newStart, isNewStartClosed): (Option[T], Boolean) = {
          if (start.isEmpty || interval.start.isEmpty)
            (None, false)
          else if (start.get < interval.start.get)
            (start, isStartClosed)
          else if (start.get > interval.start.get)
            (interval.start, interval.isStartClosed)
          else
            (start, isStartClosed || interval.isStartClosed)
        }
        val (newEnd, isNewEndClosed): (Option[T], Boolean) = {
          if (end.isEmpty || interval.end.isEmpty)
            (None, false)
          else if (end.get < interval.end.get)
            (interval.end, interval.isEndClosed)
          else if (end.get > interval.end.get)
            (end, isEndClosed)
          else
            (end, isEndClosed || interval.isEndClosed)
        }
        val newOrderingInterval: OrderingInterval[T] =
          OrderingInterval[T](newStart, newEnd, isNewStartClosed, isNewEndClosed)
        newOrderingInterval.merge(otherIntervals.tail)
      }
    }
  }
}

object OrderingInterval {
  def intersect[T: Ordering](
    interval1: OrderingInterval[T],
    interval2: OrderingInterval[T]
  ): Option[OrderingInterval[T]] = {
    val (isStartClosed, start): (Boolean, Option[T]) = {
      if (interval1.start.isDefined && interval2.start.isDefined) {
        if (interval1.start.get > interval2.start.get)
          (interval1.isStartClosed, interval1.start)
        else if (interval1.start.get < interval2.start.get)
          (interval2.isStartClosed, interval2.start)
        else
          (interval1.isStartClosed && interval2.isStartClosed, interval1.start)
      } else if (interval1.start.isDefined) {
        (interval1.isStartClosed, interval1.start)
      } else if (interval2.start.isDefined) {
        (interval2.isStartClosed, interval2.start)
      } else {
        (false, None)
      }
    }
    val (isEndClosed, end): (Boolean, Option[T]) = {
      if (interval1.end.isDefined && interval2.end.isDefined) {
        if (interval1.end.get < interval2.end.get)
          (interval1.isEndClosed, interval1.end)
        else if (interval1.end.get > interval2.end.get)
          (interval2.isEndClosed, interval2.end)
        else
          (interval1.isEndClosed && interval2.isEndClosed, interval1.end)
      } else if (interval1.end.isDefined) {
        (interval1.isEndClosed, interval1.end)
      } else if (interval2.end.isDefined) {
        (interval2.isEndClosed, interval2.end)
      } else {
        (false, None)
      }
    }
    if (
      start.isDefined &&
        end.isDefined &&
        (start.get > end.get || (start.get == end.get && !(isStartClosed && isEndClosed)))
    )
      None
    else
      Some(OrderingInterval[T](start, end, isStartClosed, isEndClosed))
  }
}
