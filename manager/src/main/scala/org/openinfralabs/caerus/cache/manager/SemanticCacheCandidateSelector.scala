package org.openinfralabs.caerus.cache.manager

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.optimizer.OrderedJoin
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, UnaryNode}
import org.openinfralabs.caerus.cache.common._
import org.openinfralabs.caerus.cache.common.plans.{CaerusPlan, CaerusSourceLoad}

class SemanticCacheCandidateSelector extends CandidateSelector {
  override def getCandidates(plan: LogicalPlan): Seq[Candidate] = {
    getCandidates(plan, Seq.empty[Attribute], Seq.empty[Attribute])
  }

  private def attributeToIndex(caerusSourceLoad: CaerusSourceLoad, attribute: Attribute): Seq[Int] = {
    val index = caerusSourceLoad.output.indexOf(attribute)
    if (index == -1)
      Seq.empty[Int]
    else
      Seq(index)
  }

  def getCandidates(
      plan: CaerusPlan,
      filterAttributes: Seq[Attribute],
      shuffleAttributes: Seq[Attribute]): Seq[Candidate] = {
    plan match {
      case caerusSourceLoad: CaerusSourceLoad  =>
        val repartitionCandidates: Seq[Candidate] =
          shuffleAttributes.flatMap(attributeToIndex(caerusSourceLoad,_)).map(Repartitioning(caerusSourceLoad, _))
        val fileSkippingIndexingCandidates: Seq[Candidate] =
          filterAttributes.flatMap(attributeToIndex(caerusSourceLoad,_)).map(FileSkippingIndexing(caerusSourceLoad, _))
        Seq(Caching(caerusSourceLoad)) ++ repartitionCandidates ++ fileSkippingIndexingCandidates

      case aggr: Aggregate =>
        val newShuffleAttributes: Seq[Attribute] = aggr.groupingExpressions.flatMap(_.references.toSeq)

        Seq(Caching(plan)) ++ getCandidates(aggr.child, Seq.empty[Attribute], newShuffleAttributes)

      case join: Join =>
        val newShuffleAttributes: Seq[Attribute] = {
          if (join.condition.isDefined)
            join.condition.get.references.toSeq
          else
            Seq.empty[Attribute]
        }

        Seq(Caching(plan)) ++ getCandidates(join.left, Seq.empty[Attribute], newShuffleAttributes) ++
          getCandidates(join.right, Seq.empty[Attribute], newShuffleAttributes)

      case orderedJoin: OrderedJoin =>
        val newShuffleAttributes: Seq[Attribute] = {
          if (orderedJoin.condition.isDefined) {
            orderedJoin.condition.get.references.toSeq
          } else {
            Seq.empty[Attribute]
          }
        }

        Seq(Caching(plan)) ++ getCandidates(orderedJoin.left, Seq.empty[Attribute], newShuffleAttributes) ++
          getCandidates(orderedJoin.right, Seq.empty[Attribute], newShuffleAttributes)

      case filter: Filter =>
        val newFilterAttributes = filterAttributes ++ filter.expressions.flatMap(_.references.toSeq)

        Seq(Caching(plan)) ++ getCandidates(filter.child, newFilterAttributes, shuffleAttributes)

      case project: Project =>
        Seq(Caching(plan)) ++ getCandidates(project.child, filterAttributes, shuffleAttributes)

      case _: UnaryNode =>
        getCandidates(plan.asInstanceOf[UnaryNode].child, filterAttributes, shuffleAttributes)

      case _ =>
        plan.children.flatMap(getCandidates(_, Seq.empty[Attribute], Seq.empty[Attribute]))
    }
  }

  /*
  override def getRepartitionCandidates(plan: LogicalPlan): List[Candidate] = {
    val repartitionCandidates:Repartition = Repartition(ListBuffer[AnyRef](), ListBuffer[AnyRef]())
    for (d <- data) {
      if(d.operator.contains("Aggregate")){
        repartitionCandidates.attribute += d.attribute
      } else if(d.operator.contains("LogicalRelation")){
        repartitionCandidates.source += d.attribute
      } else {
        repartitionCandidates
      }
    }
    repartitionCandidates
  }

  def cacheCandidate(p: LogicalPlan): Caching = p match {
    case _: LogicalRelation => Caching(ListBuffer(p))
    case _: Filter | _: Project | _: Aggregate =>
      val childCandidates: ListBuffer[LogicalPlan] = cacheCandidate(p.asInstanceOf[UnaryNode].child).getList
      Caching(childCandidates ++ ListBuffer(p))
    case _: OrderedJoin | _: Join =>
      val leftCandidates: ListBuffer[LogicalPlan] = cacheCandidate(p.asInstanceOf[BinaryNode].left).getList
      val rightCandidates: ListBuffer[LogicalPlan] = cacheCandidate(p.asInstanceOf[BinaryNode].right).getList
      Caching(leftCandidates ++ rightCandidates ++ ListBuffer(p))
    case unaryNode: UnaryNode =>
      cacheCandidate(unaryNode.child)
    case binaryNode: BinaryNode =>
      Caching(cacheCandidate(binaryNode.left).getList ++ cacheCandidate(binaryNode.right).getList)
    case _: LeafNode =>
      Caching(ListBuffer())
  }

  lazy val data: Seq[NodeData] = LogicalPlanParser.visit(p)

  def repartitionCandidate: Repartition = {
    val repartitionCandidates:Repartition = Repartition(ListBuffer[AnyRef](), ListBuffer[AnyRef]())
    for (d <- data) {
      if(d.operator.contains("Aggregate")){
        repartitionCandidates.attribute += d.attribute
      } else if(d.operator.contains("LogicalRelation")){
        repartitionCandidates.source += d.attribute
      } else {
        repartitionCandidates
      }
    }
    repartitionCandidates
  }

  def indexCandidate: Indexing = {
    val indexCandidates:Indexing = Indexing(ListBuffer[AnyRef](), ListBuffer[AnyRef]())
    for (d <- data) {
      if(d.operator.contains("Aggregate")){
        indexCandidates.attribute += d.attribute
      } else if(d.operator.contains("LogicalRelation")){
        indexCandidates.source += d.attribute
      } else {
        indexCandidates
      }
    }
    indexCandidates
  }
   */
}
