package org.openinfralabs.caerus.cache.manager

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.openinfralabs.caerus.cache.common.Candidate

private[manager] abstract class Optimizer {
  def optimize(logicalPlan: LogicalPlan, contents: Map[Candidate,String], addReference: String => Unit): LogicalPlan
}
