package org.openinfralabs.caerus.cache.manager

import org.apache.hadoop.fs.Path
import org.openinfralabs.caerus.cache.common.plans._
import org.openinfralabs.caerus.cache.common.{Caching, Candidate, FileSkippingIndexing, Repartitioning, Tier}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

case class BasicStorageIOMultiTierPlanner(optimizer: Optimizer, predictor: Predictor, tiers: Map[Tier.Tier, String]) extends Planner {
  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)

  //private def emptyAddReference(path: String): Unit = {}

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

  private def getReadSizeFactor(
    tier: Tier.Tier): Long = {
    tier match {
      case Tier.COMPUTE_MEMORY => 6
      case _ => 1
    }
  }

  private def calculateCacheStorageIOCost(
    contents: Map[Candidate,String],
    references: Map[String,Long], tier: Tier.Tier
  ): Long = {
    var cost: Long = 0L
    val readSizeFactor: Long = getReadSizeFactor(tier)
    logger.info("in calculateCacheStorageIOCost, readSizeFactor is %s".format(readSizeFactor))
    contents.keys.foreach(candidate => {
      val path: String = contents(candidate)
      if (references.contains(path)) {
        val numRefs: Long = references(path)
        // TODO: This needs to change according to what filters are issued after this load.
        cost += numRefs * candidate.sizeInfo.get.readSizeInfo.getSize(0,0)
      }
    })
    cost = cost/readSizeFactor
    cost
  }

  private def findCost(contents: Map[Candidate,String], plans: Seq[CaerusPlan], tier: Tier.Tier): Long = {
    val tempReferences: mutable.HashMap[String,Long] = mutable.HashMap.empty[String,Long]
    val storageSourceIOCost: Long = plans.map(plan => {
      val optimizedPlan: CaerusPlan = optimizer.optimize(plan, contents, addReference(tempReferences))
      calculateSourceStorageIOCost(optimizedPlan)
    }).sum
    storageSourceIOCost + calculateCacheStorageIOCost(contents, tempReferences.toMap, tier)
  }

  private def findSourceReadSize(contents: Map[Candidate,String], candidate: Candidate, tier: Tier.Tier): Long = {
    val readSizeFactor: Long = getReadSizeFactor(tier)
    logger.info("in findSourceReadSize, readSizeFactor is %s".format(readSizeFactor))
    candidate match {
      case Repartitioning(source, _, _) => source.size
      case FileSkippingIndexing(source, _, _) => source.size/readSizeFactor
      case Caching(plan, _) => findCost(contents, Seq(plan), tier)
    }
  }

  private def findBottomCandidate(
    contents: Map[Candidate,String],
    plans: Seq[CaerusPlan],
    cost: Long,
    tier: Tier.Tier
  ): (Candidate,Long) = {
    val candidates: Seq[Candidate] = contents.keys.toSeq
    val costs: Seq[Long] = candidates.map(candidate => findCost(contents-candidate, plans, tier))
    val benefits: Seq[Long] = costs.map(newCost => newCost - cost)
    val benefitsPerByte: Seq[Long] = benefits.indices.map(i => benefits(i)/candidates(i).sizeInfo.get.writeSize)
    val bottomCandidateIndex: Int = benefitsPerByte.zipWithIndex.minBy(_._1)._2
    val bottomCandidate: Candidate = candidates(bottomCandidateIndex)
    val newCost: Long = costs(bottomCandidateIndex)
    (bottomCandidate, newCost)
  }

  /*private def insertCaerusWrite(
    plan: CaerusPlan,
    backupPlan: CaerusPlan,
    caerusWrite: CaerusWrite,
    caerusDelete: Option[CaerusDelete],
  ): CaerusPlan = {
    logger.info("Insert Caerus Write for plan:\n%s".format(plan))
    plan match {
      case caerusCacheLoad: CaerusCacheLoad if caerusCacheLoad.sources == Seq(caerusWrite.name) =>
        var caerusIf: CaerusIf = caerusWrite match {
          case _: CaerusRepartitioning | _: CaerusCaching =>
            CaerusIf(Seq(caerusWrite, plan, backupPlan))
          case _ =>
            logger.warn("Found caerus write %s, when we should only have Repartitioning and Caching"
              .format(caerusWrite))
            return plan
        }
        if (caerusDelete.isDefined)
          caerusIf = CaerusIf(Seq(caerusDelete.get, caerusIf, caerusIf.children(2)))
        caerusIf
      case CaerusLoadWithIndices(_, loadChild, _, loadIndex) =>
        caerusWrite match {
          case CaerusFileSkippingIndexing(name, _, index) if index == loadIndex =>
            val caerusCacheLoad: CaerusCacheLoad =
              loadChild.asInstanceOf[Project].child.asInstanceOf[Filter].child.asInstanceOf[CaerusCacheLoad]
            if (caerusCacheLoad.sources == Seq(name)) {
              var caerusIf = CaerusIf(Seq(caerusWrite, plan, backupPlan))
              if (caerusDelete.isDefined)
                caerusIf = CaerusIf(Seq(caerusDelete.get, caerusIf, caerusIf.children(2)))
              caerusIf
            } else {
              plan
            }
          case _ =>
            plan
        }
      case _ =>
        assert(plan.children.size == backupPlan.children.size)
        val newChildren: Seq[CaerusPlan] = plan.children.indices.map(i =>
          insertCaerusWrite(plan.children(i), backupPlan.children(i), caerusWrite, caerusDelete))
        plan.withNewChildren(newChildren)
    }
  }*/

  private def selectCandidates(candidates: Seq[Candidate], tier: Option[Tier.Value] = None): Seq[Candidate]={
    var newCandidates: Seq[Candidate] = Seq[Candidate]()
    tier match {
      case None=>{
        logger.info("selectCandidates in None")
        for(candidate <- candidates){
          candidate match {
            case Caching(plan, cachingSizeInfo) => newCandidates = newCandidates :+ candidate
            case _ => None
          }
        }
      }
      case Tier.COMPUTE_MEMORY =>{
        logger.info("selectCandidates in COMPUTE_MEMORY")
        for(candidate <- candidates){
          candidate match {
            case Caching(_,_) => newCandidates = newCandidates :+ candidate
            case FileSkippingIndexing(source, index, fsSizeInfo) => newCandidates = newCandidates :+ candidate
            case _ => None
          }
        }
      }
      case Tier.STORAGE_DISK =>{
        logger.info("selectCandidates in STORAGE_DISK")
        for(candidate <- candidates){
          candidate match {
            case Repartitioning(source, index, repSizeInfo) => newCandidates :+ candidate
            case _ =>  None
          }
        }
      }
      case _ =>{
        logger.info("selectCandidates in Else")
        for(candidate <- candidates){
          newCandidates = newCandidates :+ candidate
        }
      }
    }
    newCandidates
  }
  override def optimize(
    plan: CaerusPlan,
    allContents: Map[Tier.Tier, Map[Candidate,String]],
    candidates: Seq[Candidate],
    capacity: Map[Tier.Tier, Long]
  ): Map[Tier.Tier, Map[Candidate,String]] = {
    val newMultitierContents: mutable.HashMap[Tier.Tier, Map[Candidate,String]] = mutable.HashMap.empty[Tier.Tier, Map[Candidate,String]]
    logger.info("Initial %s candidates:\n%s\n".format(candidates.size, candidates.mkString("\n")))

    // Get future plans.
    val plans: Seq[CaerusPlan] = plan +: predictor.getPredictions(plan)
    logger.info("Predictions:\n%s".format(plans.mkString("\n")))
    var allCandidates: Seq[Candidate] = candidates
    // var allCandidates: Seq[Candidate] = selectCandidates(candidates)
    logger.info("All candidates after update:\n%s\n".format(allCandidates.mkString("\n")))
    for(tier <- Tier.values){
      if(allContents.contains(tier)){
        val contents = allContents(tier)
        // contents.foreach( co => {allCandidates = allCandidates.filterNot(candidate => candidate.equals(co._1))} )
        for((ca, p) <- contents) {
          allCandidates = allCandidates.filterNot(candidate => candidate.equals(ca))
        }
        allCandidates = selectCandidates(allCandidates, tier = Some(tier))
        logger.info("existing contents for Tier %s: %s\n".format(tier, contents.mkString("\n")))
        logger.info("existing candidates for Tier %s: %s\n".format(tier, allCandidates.mkString("\n")))
        val remainingCandidates: Seq[Candidate] = allCandidates.filter(!contents.contains(_)).filter(candidate => {
          val tempReferences: mutable.HashMap[String,Long] = mutable.HashMap.empty[String,Long]
          optimizer.optimize(plan, contents + ((candidate, "temp")), addReference(tempReferences))
          tempReferences.contains("temp")
        })
        logger.info("Remaining candidates:\n%s".format(remainingCandidates.mkString("\n")))
        if (remainingCandidates.nonEmpty) {
          val initialCost: Long = findCost(contents, plans, tier)
          logger.info("Initial I/O: %s MiB".format(initialCost / (1024 * 1024)))

          // Find candidate per byte benefit.
          val costs: Seq[Long] = remainingCandidates.map(candidate => {
            findCost(contents + ((candidate, "temp")), plans, tier) +
              findSourceReadSize(contents, candidate, tier) +
              candidate.sizeInfo.get.writeSize
          })
          val benefits: Seq[Long] = costs.map(cost => initialCost - cost)
          val benefitsPerByte: Seq[Long] = benefits.indices.map(i => {
            benefits(i) / remainingCandidates(i).sizeInfo.get.writeSize
          })
          logger.info("Benefits/Byte: %s".format(benefitsPerByte.mkString(", ")))

          val topCandidateIndex: Int = benefitsPerByte.zipWithIndex.maxBy(_._1)._2
          val topCandidate = remainingCandidates(topCandidateIndex)
          var newCost: Long = costs(topCandidateIndex)
          logger.info("New I/O: %s MiB".format(newCost / (1024 * 1024)))
          if (newCost - initialCost >= 0) {
            logger.info("No positive candidate found. No content change")
            logger.info("More info about remain candidates, Costs: %s, benefits: %s".format(costs.mkString(", "), benefits.mkString(", ")))
            newMultitierContents(tier) = contents

          } else {
            logger.info("Top candidate: %s".format(topCandidate))
            logger.info(" more info about the top candidate, Costs: %s, benefits: %s".format(costs.mkString(", "), benefits.mkString(", ")))
            val topCandidateName: String = getName(topCandidate)
            val topCandidatePath: String = tiers(tier) + Path.SEPARATOR + topCandidateName
            var newContents = contents + ((topCandidate, topCandidatePath))
            var newSize: Long = newContents.keys.map(candidate => candidate.sizeInfo.get.writeSize).sum
            while (newSize > capacity(tier)) {
              logger.warn("Capacity is not big enough to hold an extra candidate. Do not change anything.")
              val bottomCandidateInfo: (Candidate, Long) = findBottomCandidate(newContents, plans, newCost, tier)
              val bottomCandidate: Candidate = bottomCandidateInfo._1
              newCost = bottomCandidateInfo._2
              newContents -= bottomCandidate
              newSize -= bottomCandidate.sizeInfo.get.writeSize
            }
            if(newContents.contains(topCandidate)) {
              logger.info("New contents:\n%s".format(newContents.mkString("\n")))
              newMultitierContents(tier) = newContents
              // if the topcandidate is been added, remove it from list
              allCandidates = allCandidates.filterNot(candidate => candidate.equals(topCandidate))
            }else{
              // if topcandidate is removed, we will not update contents
              newMultitierContents(tier) = contents
            }
          }
        }
        else{
          logger.info("No remain candidate found. No content change")
          newMultitierContents(tier) = contents
        }
      }
    }
    newMultitierContents.toMap
  }
}
