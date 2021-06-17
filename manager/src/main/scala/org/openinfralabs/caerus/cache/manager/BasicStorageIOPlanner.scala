package org.openinfralabs.caerus.cache.manager

import org.openinfralabs.caerus.cache.common.{Caching, Candidate, FileSkippingIndexing, Repartitioning}
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

  private def findCost(contents: Map[Candidate,String], plans: Seq[CaerusPlan]): Long = {
    val tempReferences: mutable.HashMap[String,Long] = mutable.HashMap.empty[String,Long]
    val storageSourceIOCost: Long = plans.map(plan => {
      val optimizedPlan: CaerusPlan = optimizer.optimize(plan, contents, addReference(tempReferences))
      calculateSourceStorageIOCost(optimizedPlan)
    }).sum
    storageSourceIOCost + calculateCacheStorageIOCost(contents, tempReferences.toMap)
  }

  private def findSourceReadSize(contents: Map[Candidate,String], candidate: Candidate): Long = {
    candidate match {
      case Repartitioning(source, _, _) => source.size
      case FileSkippingIndexing(source, _, _) => source.size
      case Caching(plan, _) => findCost(contents, Seq(plan))
    }
  }

  override def optimize(
    plan: CaerusPlan,
    contents: Map[Candidate,String],
    candidates: Seq[Candidate],
    capacity: Long
  ): CaerusPlan = {
    logger.info("Initial candidates:\n%s\n".format(candidates.mkString("\n")))

    // Eliminate candidates that are not optimizing the current plan.
    val remainingCandidates: Seq[Candidate] = candidates.filter(!contents.contains(_)).filter(candidate => {
      val tempReferences: mutable.HashMap[String,Long] = mutable.HashMap.empty[String,Long]
      optimizer.optimize(plan, contents + ((candidate, "temp")), addReference(tempReferences))
      tempReferences.contains("temp")
    })
    logger.info("Remaining candidates:\n%s".format(remainingCandidates.mkString("\n")))

    // Get future plans.
    val plans: Seq[CaerusPlan] = plan +: predictor.getPredictions(plan)
    logger.info("Predictions:\n%s".format(plans.mkString("\n")))

    // Find initial cost for current contents.
    val initialCost: Long = findCost(contents, plans)
    logger.info("Initial I/O: %s MiB".format(initialCost/(1024*1024)))

    // Find candidate per byte benefit.
    val costs: Seq[Long] = remainingCandidates.map(candidate => {
      findCost(contents + ((candidate,"temp")), plans) +
        findSourceReadSize(contents, candidate) +
        candidate.sizeInfo.get.writeSize
    })
    val benefits: Seq[Long] = costs.map(cost => initialCost - cost)
    val benefitsPerByte: Seq[Long] = benefits.indices.map(i => {
      benefits(i) / remainingCandidates(i).sizeInfo.get.writeSize
    })
    logger.info("Benefits/Byte: %s".format(benefitsPerByte.mkString(", ")))

    val topCandidate: Candidate = remainingCandidates(benefitsPerByte.zipWithIndex.maxBy(_._1)._2)
    logger.info("Top candidate: %s".format(topCandidate))

    val newContents = contents + ((topCandidate,"temp"))
    val newSize: Long = newContents.keys.map(candidate => candidate.sizeInfo.get.writeSize).sum
    if (newSize > capacity) {
      logger.warn("Capacity is not big enough to hold an extra candidate. Do not change anything.")
    }

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