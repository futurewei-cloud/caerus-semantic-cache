package org.openinfralabs.caerus.cache.manager

import org.apache.hadoop.fs.Path
import org.apache.spark.api.java
import org.openinfralabs.caerus.cache.common.Candidate
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
import org.openinfralabs.caerus.cache.manager.LRCPlanner.{refCandidate, refPath}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.reflect.internal.util.NoSourceFile.content

/**
 * Creates LRC policy for Candidate selection and eviction from Semantic Cache.
 */

case class MRDPlanner(optimizer: Optimizer, predictor: Predictor, path: String) extends Planner {
  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def emptyAddReference(path: String): Unit = {}

  private def addReference(references: mutable.HashMap[String,Long]): String => Unit = {
    path => {
      if (references.contains(path))
        references(path) += 1
      else
        references(path) = 1
    }
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
    var distance: mutable.Map[Candidate,Array[Int]] = mutable.Map.empty[Candidate, Array[Int]]
    var refCandidate: mutable.Map[Candidate,Array[Int]] = mutable.Map.empty[Candidate, Array[Int]]
    var refPath: mutable.HashMap[String, Long] = mutable.HashMap.empty[String, Long]
    logger.info("Initial candidates:\n%s\n".format(candidates.mkString("\n")))
    //MRDPlanner.refCandidate = mutable.Map.empty[Candidate, Long]

    // Eliminate candidates that are not optimizing the current plan.


    // Get optimized plan without new additions.
    val optimizedPlan: CaerusPlan = optimizer.optimize(plan, contents, emptyAddReference)

    // Get future plans.
    val plans: Seq[CaerusPlan] = plan +: predictor.getPredictions(plan)
    var newContents = contents
    candidates.foreach(candidate => {
      refCandidate(candidate) = Array(Int.MaxValue)
      newContents = newContents + ((candidate, "temp"))
    }
    )
    var contentKeys = newContents.keys.toSeq
    logger.info("Content Keys:\n%s".format(contentKeys.mkString("\n")))
    val remainingCandidates: Seq[Candidate] = contentKeys.filter(candidate => {
      var tempReferences = mutable.HashMap.empty[String, Long]
      //updateCandidateDistance(candidate)
      MRDPlanner.futureRef(candidate) = false
      plans.zipWithIndex.foreach(tempPlan => {
        //var refPath = mutable.HashMap.empty[String, Long]
        optimizer.optimize(tempPlan._1, newContents, addReference(tempReferences))
        MRDPlanner.updateDistance(tempPlan._2, candidate, distance)
      })
      tempReferences.contains("temp")
    }
    )
   // logger.info("Candidates with reference distance:\n%s".format(refCandidate.mkString("\n")))
    logger.info("Remaining candidates:\n%s".format(remainingCandidates.mkString("\n")))
    for ((k,v) <- refCandidate) {
      if (distance.contains(k))
        refCandidate(k) = distance(k)
    }
    logger.info("Contents with reference distance:\n%s".format(distance.mkString("\n")))
    logger.info("Candidates with reference distance:\n%s".format(refCandidate.mkString("\n")))

    // Pick the maximum reference distance candidate as top candidate.
    //resolving conflict by checking next distance causes exception as not all candidates has same frequency of access
 /*   val topCandidate = {
      var minCount = 0
      var loopCount = 0
      while(loopCount!=2 || minCount!=1)
        {
          val occurences = refCandidate groupBy(_._2(loopCount)) mapValues(_.size)
          minCount = occurences(refCandidate.minBy(_._2(loopCount))._2(loopCount))
          loopCount+=1
        }
        refCandidate.minBy(_._2(loopCount-1))._1
    }*/

    val topCandidate ={
      val occurences = refCandidate groupBy(_._2(0)) mapValues(_.size)
      if(occurences(refCandidate.minBy(_._2(0))._2(0))==1)
        refCandidate.minBy(_._2(0))._1
      else
        {
          refCandidate.maxBy(_._2.size)._1
        }
    }
    //distance = distance + ((topCandidate,0))
    logger.info("Top candidate: %s".format(topCandidate))

    // 2. Find eviction candidate (if capacity is smaller than size used).
    // Add top candidate to contents with proper name (use getName function on Planner).
    val topCandidateName: String = getName(topCandidate)
    val topCandidatePath: String = path + Path.SEPARATOR + topCandidateName
    newContents = contents + ((topCandidate, topCandidatePath))
    //updateContentDistance()
    var newSize: Long = newContents.keys.map(candidate => candidate.sizeInfo.get.writeSize).sum
    // Create a reference map (String -> Long).
    refPath = mutable.HashMap.empty[String, Long]

    // Optimize plans in the future prediction adding one reference in all contents when
    // necessary.
    // Create a reference map (Candidate -> Long)
    // Go through contents and update the new reference map.
   // refCandidate = mutable.HashMap.empty[Candidate, Long]

    plans.foreach(tempPlan => {
      optimizer.optimize(tempPlan, newContents, emptyAddReference)
    })

    // Evict candidates with minimum reference count until capacity constraints are satisfied.
    while (newSize > capacity) {
      logger.warn("Capacity is not big enough to hold an extra candidate. Do not change anything.")
      //val bottomCandidateInfo: (Candidate, Long) = findBottomCandidate(newContents, plans, newCost)
      val bottomCandidate: Candidate = distance.maxBy(_._2(0))._1
      //newCost = bottomCandidateInfo._2
      newContents -= bottomCandidate
      newSize -= bottomCandidate.sizeInfo.get.writeSize
    }
    logger.info("New contents:\n%s".format(newContents.mkString("\n")))
    plan
  }
}

object MRDPlanner{
  var futureRef: mutable.Map[Candidate,Boolean] = mutable.Map.empty[Candidate, Boolean]
  def trackRef(candidate: Candidate): Unit ={
    if(futureRef.contains(candidate) && !futureRef(candidate))
      futureRef(candidate) = true
  }
  def updateDistance(index:Int, candidate: Candidate, distance:mutable.Map[Candidate, Array[Int]]): Unit ={
    if(futureRef.contains(candidate) && futureRef(candidate)) {
      if(distance.contains(candidate))
        {
          val size = distance.get(candidate).size
          distance(candidate) = distance(candidate) :+ (distance(candidate)(size-1) - index)
        }
      else if(!distance.contains(candidate)) {
        distance(candidate) = Array(index)
      }
      //futureRef.remove(candidate)
    }
  }
}


