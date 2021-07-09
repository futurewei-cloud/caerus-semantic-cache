package org.openinfralabs.caerus.cache.client

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project, UnaryNode}
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
        (inputPlan, Caching(caerusSourceLoad)) +: (repartitionCandidates ++ fileSkippingIndexingCandidates)
      case aggr: Aggregate =>
        val newShuffleAttributes: Seq[Attribute] =
          aggr.groupingExpressions.flatMap(_.references.toSeq).filter(attrib => isNumeric(attrib) || isDate(attrib))
        assert(inputPlan.isInstanceOf[Aggregate])
        val newInputPlan: LogicalPlan = inputPlan.asInstanceOf[Aggregate].child
        (inputPlan, Caching(plan)) +:
          getCandidates(newInputPlan, aggr.child, Seq.empty[Attribute], newShuffleAttributes)
      case filter: Filter =>
        val newFilterAttributes: Seq[Attribute] =
          filter.expressions.flatMap(_.references.toSeq).filter(attrib => isNumeric(attrib) || isDate(attrib))
        assert(inputPlan.isInstanceOf[Filter])
        val newInputPlan: LogicalPlan = inputPlan.asInstanceOf[Filter].child
        (inputPlan, Caching(plan)) +:
          getCandidates(newInputPlan, filter.child, filterAttributes ++ newFilterAttributes, shuffleAttributes)
      case project: Project =>
        assert(inputPlan.isInstanceOf[Project])
        val newInputPlan: LogicalPlan = inputPlan.asInstanceOf[Project].child
        (inputPlan, Caching(plan)) +: getCandidates(newInputPlan, project.child, filterAttributes, shuffleAttributes)
      case _: UnaryNode =>
        assert(inputPlan.isInstanceOf[UnaryNode])
        val newInputPlan: LogicalPlan = inputPlan.asInstanceOf[UnaryNode].child
        getCandidates(newInputPlan, plan.asInstanceOf[UnaryNode].child, filterAttributes, shuffleAttributes)
      case _ =>
        assert(inputPlan.children.length == plan.children.length)
        plan.children.indices.flatMap(i =>
          getCandidates(inputPlan.children(i), plan.children(i), Seq.empty[Attribute], Seq.empty[Attribute]))
    }
  }
}
