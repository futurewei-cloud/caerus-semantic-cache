package org.openinfralabs.caerus.cache.manager

import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

case class ReverseOrderPredictor(maxLimit: Int) extends Predictor {
  override def getPredictions(caerusPlan: CaerusPlan): Seq[CaerusPlan] = ???
}
