package org.openinfralabs.caerus.cache.manager

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.openinfralabs.caerus.cache.common.Tier.Tier
import org.openinfralabs.caerus.cache.common.plans._
import org.openinfralabs.caerus.cache.common.{Caching, Candidate, FileSkippingIndexing, Repartitioning}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

case class BasicStorageIOMultiTierPlanner(optimizer: Optimizer, predictor: Predictor, tiers: Map[Tier, String]) extends Planner {
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

  private def findBottomCandidate(
    contents: Map[Candidate,String],
    plans: Seq[CaerusPlan],
    cost: Long
  ): (Candidate,Long) = {
    val candidates: Seq[Candidate] = contents.keys.toSeq
    val costs: Seq[Long] = candidates.map(candidate => findCost(contents-candidate, plans))
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

  override def optimize(
    plan: CaerusPlan,
    all_contents: Map[Tier, Map[Candidate,String]],
    candidates: Seq[Candidate],
    capacity: Map[Tier, Long]
  ): Map[Tier, Map[Candidate,String]] = {
    val new_multitier_contents: mutable.HashMap[Tier, Map[Candidate,String]] = mutable.HashMap.empty[Tier, Map[Candidate,String]]
    logger.info("Initial candidates:\n%s\n".format(candidates.mkString("\n")))

    // Get future plans.
    val plans: Seq[CaerusPlan] = plan +: predictor.getPredictions(plan)
    logger.info("Predictions:\n%s".format(plans.mkString("\n")))

    for ((tier, contents) <- all_contents){
      logger.info("existing contents for Tier %s: %s\n".format(tier, contents.mkString("\n")))
      val remainingCandidates: Seq[Candidate] = candidates.filter(!contents.contains(_)).filter(candidate => {
        val tempReferences: mutable.HashMap[String,Long] = mutable.HashMap.empty[String,Long]
        optimizer.optimize(plan, contents + ((candidate, "temp")), addReference(tempReferences))
        tempReferences.contains("temp")
      })
      logger.info("Remaining candidates:\n%s".format(remainingCandidates.mkString("\n")))
      val initialCost: Long = findCost(contents, plans)
      logger.info("Initial I/O: %s MiB".format(initialCost /(1024*1024)))

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

      val topCandidateIndex: Int = benefitsPerByte.zipWithIndex.maxBy(_._1)._2
      val topCandidate = remainingCandidates(topCandidateIndex)
      var newCost: Long = costs(topCandidateIndex)
      if (newCost - initialCost >= 0) {
        logger.info("No positive candidate found. No content change")
        new_multitier_contents(tier) = contents

      }else {
        logger.info("Top candidate: %s".format(topCandidate))

        val topCandidateName: String = getName(topCandidate)
        val topCandidatePath: String = tiers(tier) + Path.SEPARATOR + topCandidateName
        var newContents = contents + ((topCandidate, topCandidatePath))
        var newSize: Long = newContents.keys.map(candidate => candidate.sizeInfo.get.writeSize).sum
        while (newSize > capacity(tier)) {
          logger.warn("Capacity is not big enough to hold an extra candidate. Do not change anything.")
          val bottomCandidateInfo: (Candidate, Long) = findBottomCandidate(newContents, plans, newCost)
          val bottomCandidate: Candidate = bottomCandidateInfo._1
          newCost = bottomCandidateInfo._2
          newContents -= bottomCandidate
          newSize -= bottomCandidate.sizeInfo.get.writeSize
        }
        logger.info("New contents:\n%s".format(newContents.mkString("\n")))
        new_multitier_contents(tier) = newContents
        // if the topcandidate is been added, remove it from list
        candidates.filterNot(candidate => candidate == topCandidate)
      }
    }
    new_multitier_contents.toMap
  }
}
