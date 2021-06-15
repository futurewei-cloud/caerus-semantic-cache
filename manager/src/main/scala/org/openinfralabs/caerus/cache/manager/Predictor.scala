package org.openinfralabs.caerus.cache.manager

import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

abstract class Predictor {
  def getPredictions(caerusPlan: CaerusPlan): Seq[CaerusPlan]
}
