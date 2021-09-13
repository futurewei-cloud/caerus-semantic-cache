package org.openinfralabs.caerus.cache.manager

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.openinfralabs.caerus.cache.common.plans._
import org.openinfralabs.caerus.cache.common.{Caching, Candidate, FileSkippingIndexing, FilterInfo, Repartitioning}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

case class BasicStorageIOPlanner(optimizer: Optimizer, predictor: Predictor, path: String) extends Planner {
  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def emptyAddReference(path: String): Unit = {}

  private def addReference(references: mutable.HashMap[ String, Long ]): String => Unit = {
    path => {
      if (references.contains(path))
        references(path) += 1
      else
        references(path) = 1
    }
  }

  private def calculateStorageIOCost(
    contents: Map[Candidate,String],
    plan: CaerusPlan,
    originalPlan: CaerusPlan
  ): Long =
    plan match {
      case caerusSourceLoad: CaerusSourceLoad =>
        caerusSourceLoad.sources.map(source => source.length).sum
      case caerusCacheLoad: CaerusCacheLoad =>
        // TODO: If Tier is COMPUTE_X return 0 else return the following:
        caerusCacheLoad.sources.map(source => {
          val candidate: Candidate = contents.filter(_._2 == source).head._1
          candidate.sizeInfo.get.readSizeInfo.getSize(FilterInfo.get(originalPlan, candidate))
        }).sum
      case _ =>
        plan.children.map(child => calculateStorageIOCost(contents, child, originalPlan)).sum
  }

  private def findCost(contents: Map[Candidate,String], plans: Seq[CaerusPlan]): Long = {
    plans.map(plan => {
      val optimizedPlan: CaerusPlan = optimizer.optimize(plan, contents, emptyAddReference)
      calculateStorageIOCost(contents, optimizedPlan, plan)
    }).sum
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

  private def insertCaerusWrite(
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

    // Get optimized plan without new additions.
    val optimizedPlan: CaerusPlan = optimizer.optimize(plan, contents, emptyAddReference)

    // Get future plans.
    val plans: Seq[CaerusPlan] = plan +: predictor.getPredictions(plan)
    logger.info("Predictions:\n%s".format(plans.mkString("\n")))

    // Find initial cost for current contents.
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
      logger.info("No positive candidate found. Original plan is returned")
      return optimizedPlan
    }
    logger.info("Top candidate: %s".format(topCandidate))

    val topCandidateName: String = getName(topCandidate)
    val topCandidatePath: String = path + Path.SEPARATOR + topCandidateName
    var newContents = contents + ((topCandidate, topCandidatePath))
    var newSize: Long = newContents.keys.map(candidate => candidate.sizeInfo.get.writeSize).sum
    while (newSize > capacity) {
      logger.warn("Capacity is not big enough to hold an extra candidate. Do not change anything.")
      val bottomCandidateInfo: (Candidate, Long) = findBottomCandidate(newContents, plans, newCost)
      val bottomCandidate: Candidate = bottomCandidateInfo._1
      newCost = bottomCandidateInfo._2
      newContents -= bottomCandidate
      newSize -= bottomCandidate.sizeInfo.get.writeSize
    }
    logger.info("New contents:\n%s".format(newContents.mkString("\n")))

    if (newContents.contains(topCandidate)) {
      val caerusWrite: CaerusWrite = topCandidate match {
        case Repartitioning(source, index, _) =>
          CaerusRepartitioning(topCandidatePath, source, index)
        case FileSkippingIndexing(source, index, _) =>
          CaerusFileSkippingIndexing(topCandidatePath, source, index)
        case Caching(cachedPlan, _) =>
          val optimizedCachedPlan = optimizer.optimize(cachedPlan, newContents - topCandidate, emptyAddReference)
          CaerusCaching(topCandidatePath, optimizedCachedPlan)
      }
      val caerusDelete: Option[CaerusDelete] = if (!contents.keySet.subsetOf(newContents.keySet)) {
        val deletedCandidates: Set[Candidate] = contents.keySet -- newContents.keySet
        Some(CaerusDelete(deletedCandidates.toSeq.map(contents)))
      } else {
        None
      }
      val optimizedPlan: CaerusPlan = optimizer.optimize(plan, newContents, emptyAddReference)
      val backupPlan: CaerusPlan = optimizer.optimize(plan, newContents-topCandidate, emptyAddReference)
      insertCaerusWrite(optimizedPlan, backupPlan, caerusWrite, caerusDelete)
    } else {
      optimizedPlan
    }
  }
}