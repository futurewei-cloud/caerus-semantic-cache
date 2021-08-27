package org.openinfralabs.caerus.cache.client

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
abstract class RuntimePredictor {


  def predictRuntime(inputPlan: LogicalPlan, caerusPlan: CaerusPlan): Long /* = {
    val queryExecution: QueryExecution = new QueryExecution(sparkSession, inputPlan)
    queryExecution.toRdd.getNumPartitions
  }*/
}
