package org.openinfralabs.caerus.cache.manager

import com.typesafe.scalalogging.LazyLogging
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

case class OraclePredictor(var futurePlans: Seq[CaerusPlan]) extends Predictor with LazyLogging {
  override def getPredictions(caerusPlan: CaerusPlan): Seq[CaerusPlan] = {
    if (futurePlans.isEmpty) {
      logger.warn("Oracle predictions are empty.")
      futurePlans
    } else {
      if (!futurePlans.head.equals(caerusPlan)) {
        logger.warn(
          "Caerus plan does not match the oracle prediction. Plans:\n%s\n%s".format(caerusPlan, futurePlans.head)
        )
      }
      futurePlans = futurePlans.tail
      futurePlans
    }
  }
}
