package org.openinfralabs.caerus.cache.manager

import org.apache.hadoop.fs.Path
import org.openinfralabs.caerus.cache.common.Candidate
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
import org.openinfralabs.caerus.cache.manager.LRCPlanner.{refCandidate, refPath}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * Creates LRC policy for Candidate selection and eviction from Semantic Cache.
 */

case class LRCPlanner(optimizer: Optimizer, predictor: Predictor, path: String) extends Planner {
  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def emptyAddReference(path: String): Unit = {}
  private def emptyCandidate(candidate: Candidate): Unit = {}



  private def addReference(references: mutable.Map[Candidate,Long], candidate: Candidate) = {

      if (references.contains(candidate))
        references(candidate) += 1
      else
        references(candidate) = 1
    }

  /**
   *
   * @param plan Current plan to optimize.
   * @param contents Current contents of Semantic Cache.
   * @param candidates Write candidates for current plan.
   * @param capacity Capacity of the Semantic Cache in bytes.
   * @return optimized plan
   */
  override def optimize(
    plan: CaerusPlan,
    contents: Map[Candidate,String],
    candidates: Seq[Candidate],
    capacity: Long
  ): CaerusPlan = {
    // 1. Find top candidate.
    // Create a reference map (Candidate -> Long).
    logger.info("Initial candidates:\n%s\n".format(candidates.mkString("\n")))
    LRCPlanner.refCandidate = mutable.Map.empty[Candidate, Long]
    LRCPlanner.refPath = mutable.HashMap.empty[String, Long]
    // For each candidate in candidates:
    // Add candidate in contents with name "temp".
    var newContents = contents
   candidates.foreach(candidate => {
      LRCPlanner.refCandidate(candidate) = 0
      newContents = newContents + ((candidate, "temp"))
    }
    )
    // Optimize plans in the future prediction (add the current one) adding one reference
    // to temp when necessary.

    val plans: Seq[CaerusPlan] = plan +: predictor.getPredictions(plan)
    val remainingCandidates: Seq[Candidate] = candidates.filter(!contents.contains(_)).filter(candidate => {

      plans.foreach(tempPlan => {
        LRCPlanner.refPath = mutable.HashMap.empty[String, Long]
        optimizer.optimize(tempPlan, newContents, LRCPlanner.addReference(candidate))
      })
      LRCPlanner.refPath.contains("temp")
    }
    )
    logger.info("Candidates with reference:\n%s".format(LRCPlanner.refCandidate.mkString("\n")))
    logger.info("Remaining candidates:\n%s".format(remainingCandidates.mkString("\n")))
    // Pick the maximum reference count candidate as top candidate.
    val topCandidate = LRCPlanner.refCandidate.maxBy(_._2)._1
    logger.info("Top candidate: %s".format(topCandidate))

    // 2. Find eviction candidate (if capacity is smaller than size used).
    // Add top candidate to contents with proper name (use getName function on Planner).
    val topCandidateName: String = getName(topCandidate)
    val topCandidatePath: String = path + Path.SEPARATOR + topCandidateName
    newContents = contents + ((topCandidate, topCandidatePath))
    var newSize: Long = newContents.keys.map(candidate => candidate.sizeInfo.get.writeSize).sum
    // Create a reference map (String -> Long).
   // refPath = mutable.HashMap.empty[String, Long]

    // Optimize plans in the future prediction adding one reference in all contents when
    // necessary.
    // Create a reference map (Candidate -> Long)
    // Go through contents and update the new reference map.
   // refCandidate = mutable.HashMap.empty[Candidate, Long]

    plans.foreach(tempPlan => {
      optimizer.optimize(tempPlan, newContents, LRCPlanner.addReference(topCandidate))
    })
    contents.foreach(content=>{
      refCandidate(content._1) = refPath(content._2)
    })
    // Evict candidates with minimum reference count until capacity constraints are satisfied.
    while (newSize > capacity) {
      logger.warn("Capacity is not big enough to hold an extra candidate. Do not change anything.")
      //val bottomCandidateInfo: (Candidate, Long) = findBottomCandidate(newContents, plans, newCost)
      val bottomCandidate: Candidate = refCandidate.maxBy(_._2)._1
      //newCost = bottomCandidateInfo._2
      newContents -= bottomCandidate
      newSize -= bottomCandidate.sizeInfo.get.writeSize
    }
    logger.info("New contents:\n%s".format(newContents.mkString("\n")))
    plan
  }
}

object LRCPlanner{
  var refCandidate: mutable.Map[Candidate, Long] = mutable.Map.empty[Candidate, Long]
  var refPath: mutable.HashMap[String, Long] = mutable.HashMap.empty[String, Long]
  def addReference(candidate: Candidate): String => Unit = {
    if(refCandidate.contains(candidate))
      refCandidate(candidate) += 1
    path => {
       if (refPath.contains(path))
         refPath(path) += 1
       else
         refPath(path) = 1
       //if(path == "temp")


      //  else
      // refCandidate(candidate) = 1
    }
  }
}
