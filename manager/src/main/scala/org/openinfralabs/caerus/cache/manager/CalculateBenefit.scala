package org.openinfralabs.caerus.cache.manager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.optimizer.OrderedJoin
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

import org.openinfralabs.caerus.cache.common._

private[manager] class CalculateBenefit {
  def benefit(candidate: Candidate, queries: Seq[LogicalPlan]): Long = {
    var res: Long = 0

    queries.foreach(res += benefit(candidate,_))
    res
  }

  private def benefit(candidate: Candidate, query: LogicalPlan, attributes: Seq[Attribute] = Seq.empty[Attribute]): Long = {
    candidate match {
      case Repartitioning(source, attribute) =>
        if (query.sameOutput(source) && attributes.contains(attribute)) {
          repartitioning(source.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation])
        } else {
          var res: Long = 0L
          val newAttributes: Seq[Attribute] = query match {
            case Filter(condition, _) =>
              attributes ++ condition.references.toSeq
            case _:Join | _:Aggregate | _:OrderedJoin =>
              Seq.empty[Attribute]
            case _ =>
              attributes
          }
          query.children.foreach(res += benefit(candidate, _, newAttributes))
          res
        }
      case FileSkippingIndexing(source, attribute) =>
        if (query.sameOutput(source) && attributes.contains(attribute)) {
          indexing(source.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation])
        } else {
          var res: Long = 0L
          val newAttributes: Seq[Attribute] = query match {
            case Filter(condition, _) =>
              attributes ++ condition.references.toSeq
            case _:Join | _:Aggregate | _:OrderedJoin =>
              Seq.empty[Attribute]
            case _ =>
              attributes
          }
          query.children.foreach(res += benefit(candidate, _, newAttributes))
          res
        }
      case Caching(plan) =>
        if (plan.sameOutput(query)) {
          caching(plan).toLong
        } else {
          var res: Long = 0L

          query.children.foreach(res += benefit(candidate, _, attributes))
          res
        }
    }
  }

  private def repartitioning(relation: HadoopFsRelation): Long = {
    val fileSize = relation.location.sizeInBytes
    (fileSize * 0.9).toLong
  }

  private def indexing(relation: HadoopFsRelation): Long = {
    val fileSize = relation.location.sizeInBytes
    (fileSize * 0.3).toLong
  }

  def caching(lp:LogicalPlan): BigInt = {
    lp.stats.sizeInBytes
  }
}
