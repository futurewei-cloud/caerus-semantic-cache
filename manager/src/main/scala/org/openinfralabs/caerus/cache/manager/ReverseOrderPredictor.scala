package org.openinfralabs.caerus.cache.manager

import com.typesafe.scalalogging.LazyLogging
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

case class ReverseOrderPredictor(windowSize: Int) extends Predictor(windowSize) with LazyLogging {
  private var futurePlans: Seq[CaerusPlan] = Seq.empty[CaerusPlan]

  override def getPredictions(caerusPlan: CaerusPlan): Seq[CaerusPlan] = {
    assert(futurePlans.length <= windowSize + 1)
    // TODO: Discuss with Jit why do we need this?
    //if (futurePlans.contains(caerusPlan))
    //  futurePlans = futurePlans.filterNot(plan => plan == caerusPlan)
    if (futurePlans.length > windowSize)
      futurePlans = futurePlans.dropRight(1)
    logger.info("Caerus plans from planner: %s".format(caerusPlan))
    futurePlans = caerusPlan +: futurePlans
    logger.info("Future plans:\n%s".format(futurePlans.mkString("\n")))
    futurePlans.tail
  }
}