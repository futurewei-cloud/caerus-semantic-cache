package org.openinfralabs.caerus.cache.manager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

import org.openinfralabs.caerus.cache.common._

private[manager] class CalculateCost {
  def cost(candidate: Candidate): Long = {
    candidate match {
      case Repartitioning(source, _) =>
        repartitioning(source.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation])
      case Caching(plan) =>
        caching(plan).toLong
      case _ =>
        0L
    }
  }

  private def repartitioning(relation:HadoopFsRelation): Long ={
    val fileSize = relation.location.sizeInBytes
    fileSize*2
  }

  private def caching(lp:LogicalPlan): BigInt = {
    lp.stats.sizeInBytes*2
  }

}
