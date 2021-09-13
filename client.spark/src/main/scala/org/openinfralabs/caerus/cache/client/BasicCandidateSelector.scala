package org.openinfralabs.caerus.cache.client

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._
import org.openinfralabs.caerus.cache.common.plans.{CaerusPlan, CaerusSourceLoad}
import org.openinfralabs.caerus.cache.common.{Caching, Candidate, FileSkippingIndexing, Repartitioning}

case class BasicCandidateSelector() extends CandidateSelector {
  override def getCandidates(inputPlan: LogicalPlan, plan: CaerusPlan): Seq[(LogicalPlan,Candidate)] = {
    getCandidates(inputPlan, plan, Seq.empty[Attribute], Seq.empty[Attribute])
  }

  private def attributeToIndex(caerusSourceLoad: CaerusSourceLoad, attribute: Attribute): Seq[ Int ] = {
    val index = caerusSourceLoad.output.indexOf(attribute)
    if (index == -1)
      Seq.empty[ Int ]
    else
      Seq(index)
  }

  private def isNumeric(attribute: Attribute): Boolean = {
    attribute.dataType match {
      case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType =>
        true
      case _ => false
    }
  }

  private def isDate(attribute: Attribute): Boolean = {
    attribute.dataType match {
      case _: TimestampType | _: DateType => true
      case _ => false
    }
  }

  private def isSupported(attribute: Attribute): Boolean = isNumeric(attribute) || isDate(attribute)

  private def getCandidates(
    inputPlan: LogicalPlan,
    plan: CaerusPlan,
    filterAttributes: Seq[Attribute],
    shuffleAttributes: Seq[Attribute]
  ): Seq[(LogicalPlan,Candidate)] = {
    plan match {
      case caerusSourceLoad: CaerusSourceLoad =>
        val repartitionCandidates: Seq[(LogicalPlan,Candidate)] =
          shuffleAttributes
            .flatMap(attributeToIndex(caerusSourceLoad, _))
            .map(index => Repartitioning(caerusSourceLoad, index))
            .map(candidate => (inputPlan, candidate))
        val fileSkippingIndexingCandidates: Seq[(LogicalPlan,Candidate)] =
          filterAttributes
            .flatMap(attributeToIndex(caerusSourceLoad, _))
            .map(index => FileSkippingIndexing(caerusSourceLoad, index))
            .map(candidate => (inputPlan, candidate))
        assert(inputPlan.isInstanceOf[LogicalRelation])
        val cachingCandidate: (LogicalPlan,Candidate) = (inputPlan, Caching(caerusSourceLoad))
        cachingCandidate +: (repartitionCandidates ++ fileSkippingIndexingCandidates)
      case aggregate: Aggregate =>
        val newShuffleAttributes: Seq[Attribute] =
          aggregate
            .groupingExpressions
            .flatMap(_.references.toSeq)
            .filter(attribute => isSupported(attribute))
        assert(inputPlan.isInstanceOf[Aggregate])
        val newInputPlan: LogicalPlan = inputPlan.asInstanceOf[Aggregate].child
        val cachingCandidate: (LogicalPlan,Candidate) = (inputPlan, Caching(aggregate))
        cachingCandidate +: getCandidates(newInputPlan, aggregate.child, Seq.empty[Attribute], newShuffleAttributes)
      case filter: Filter =>
        val newFilterAttributes: Seq[Attribute] = filterAttributes ++
          filter
            .expressions
            .flatMap(_.references.toSeq)
            .filter(attribute => isSupported(attribute))
        assert(inputPlan.isInstanceOf[Filter])
        val newInputPlan: LogicalPlan = inputPlan.asInstanceOf[Filter].child
        val cachingCandidate: (LogicalPlan,Candidate) = (inputPlan, Caching(filter))
        cachingCandidate +: getCandidates(newInputPlan, filter.child, newFilterAttributes, shuffleAttributes)
      case project: Project =>
        assert(inputPlan.isInstanceOf[Project])
        val newInputPlan: LogicalPlan = inputPlan.asInstanceOf[Project].child
        val cachingCandidate: (LogicalPlan,Candidate) = (inputPlan, Caching(project))
        cachingCandidate +: getCandidates(newInputPlan, project.child, filterAttributes, shuffleAttributes)
      case unary: UnaryNode =>
        assert(inputPlan.isInstanceOf[UnaryNode])
        val newInputPlan: LogicalPlan = inputPlan.asInstanceOf[UnaryNode].child
        getCandidates(newInputPlan, unary.child, filterAttributes, shuffleAttributes)
      case _ =>
        assert(inputPlan.children.length == plan.children.length)
        plan.children.indices.flatMap(i =>
          getCandidates(inputPlan.children(i), plan.children(i), Seq.empty[Attribute], Seq.empty[Attribute]))
    }
  }
}