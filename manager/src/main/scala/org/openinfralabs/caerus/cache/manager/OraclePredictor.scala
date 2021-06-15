package org.openinfralabs.caerus.cache.manager

import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

case class OraclePredictor(var futurePlans: Seq[CaerusPlan]) extends Predictor {
  override def getPredictions(caerusPlan: CaerusPlan): Seq[CaerusPlan] = {
    assert(futurePlans.nonEmpty && caerusPlan == futurePlans.head)
    futurePlans = futurePlans.tail
    futurePlans
  }
}
