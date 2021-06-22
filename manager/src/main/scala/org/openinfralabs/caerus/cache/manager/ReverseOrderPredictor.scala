package org.openinfralabs.caerus.cache.manager

import com.typesafe.scalalogging.LazyLogging
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

case class ReverseOrderPredictor(var futurePlans: Seq[CaerusPlan], maxLimit: Int) extends Predictor with LazyLogging {
  override def getPredictions(caerusPlan: CaerusPlan): Seq[CaerusPlan] = {
    assert(futurePlans.length<=maxLimit)
    if(futurePlans.contains(caerusPlan))
      futurePlans = futurePlans.filterNot(CaurusPlan => CaurusPlan == caerusPlan)
    if(futurePlans.nonEmpty && futurePlans.length==maxLimit)
    {
      futurePlans = futurePlans.dropRight(1)
    }
    logger.info("Caerus plans from planner: %s".format(caerusPlan.toString()))
    futurePlans = caerusPlan +: futurePlans
    if(futurePlans.nonEmpty)
    {
      logger.info("Future plans: %s".format(futurePlans.toString()))
      logger.info("Future plan tail: %s".format(futurePlans.last.toString()))
    }
    futurePlans.tail
  }
}