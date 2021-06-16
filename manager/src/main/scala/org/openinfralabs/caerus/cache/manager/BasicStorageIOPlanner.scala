package org.openinfralabs.caerus.cache.manager

import org.openinfralabs.caerus.cache.common.Candidate
import org.openinfralabs.caerus.cache.common.plans.{CaerusPlan, CaerusSourceLoad}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

case class BasicStorageIOPlanner(optimizer: Optimizer, predictor: Predictor) extends Planner {

  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def addReference(references: mutable.HashMap[String,Long]): String => Unit = {
    path => {
      if (references.contains(path))
        references(path) += 1
      else
        references(path) = 1
    }
  }

  private def calculateSourceStorageIOCost(plan: CaerusPlan): Long = {
    var cost: Long = 0L
    plan match {
      case caerusSourceLoad: CaerusSourceLoad =>
        cost += caerusSourceLoad.sources.map(source => source.length).sum
      case _ =>
        plan.children.foreach(child => cost += calculateSourceStorageIOCost(child))
    }
    cost
  }

  private def calculateCacheStorageIOCost(
    contents: Map[Candidate,String],
    references: Map[String,Long]
  ): Long = {
    var cost: Long = 0L
    contents.keys.foreach(candidate => {
      val path: String = contents(candidate)
      if (references.contains(path)) {
        val numRefs: Long = references(path)
        // TODO: This needs to change according to what filters are issued after this load.
        cost += numRefs * candidate.sizeInfo.get.readSizeInfo.getSize(0,0)
      }
    })
    cost
  }

  override def optimize(plan: CaerusPlan, contents: Map[Candidate,String], candidates: Seq[Candidate]): CaerusPlan = {
    logger.info("Initial candidates:\n%s\n".format(candidates.mkString("\n")))

    // Eliminate candidates that are not optimizing the current plan.
    val remainingCandidates: Seq[Candidate] = candidates.filter(candidate => {
      val tempReferences: mutable.HashMap[String,Long] = mutable.HashMap.empty[String,Long]
      optimizer.optimize(plan, contents ++ Map((candidate, "temp")), addReference(tempReferences))
      tempReferences.contains("temp")
    })
    logger.info("Remaining candidates:\n%s".format(remainingCandidates.mkString("\n")))

    // Get future plans.
    val plans: Seq[CaerusPlan] = plan +: predictor.getPredictions(plan)
    logger.info("Predictions:\n%s".format(plans.mkString("\n")))

    // Find initial cost for current contents.
    /*
    val curReferences: mutable.HashMap[String,Long] = mutable.HashMap.empty[String,Long]
    var cost: Long = 0L
    plans.foreach(curPlan => {
      val optPlan = optimizer.optimize(curPlan, contents, addReference(curReferences))
      cost += calculateSourceStorageIOCost(optPlan)
    })
    cost += calculateCacheStorageIOCost(contents, curReferences.toMap[String,Long])
    */
    plan
  }
}
