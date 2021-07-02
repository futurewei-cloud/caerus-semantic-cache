package org.openinfralabs.caerus.cache.manager

import com.typesafe.scalalogging.LazyLogging
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

case class ReverseOrderPredictor(windowSize: Int) extends Predictor(windowSize) with LazyLogging {
  private var futurePlans: Seq[CaerusPlan] = Seq.empty[CaerusPlan]

  override def getPredictions(caerusPlan: CaerusPlan): Seq[CaerusPlan] = {
    assert(futurePlans.length <= windowSize)
    if (futurePlans.contains(caerusPlan))
      futurePlans = futurePlans.filterNot(plan => plan == caerusPlan)
    if (futurePlans.nonEmpty && futurePlans.length == windowSize)
      futurePlans = futurePlans.dropRight(1)
    logger.info("Caerus plans from planner: %s".format(caerusPlan.toString()))
    futurePlans = caerusPlan +: futurePlans
    if (futurePlans.nonEmpty) {
      logger.info("Future plans: %s".format(futurePlans.toString()))
      logger.info("Future plan tail: %s".format(futurePlans.last.toString()))
    }
    futurePlans.tail
  }
}