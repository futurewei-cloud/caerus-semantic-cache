package org.openinfralabs.caerus.cache.manager

import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

abstract class Predictor(windowSize: Int) {
  def getPredictions(caerusPlan: CaerusPlan): Seq[CaerusPlan]
}
