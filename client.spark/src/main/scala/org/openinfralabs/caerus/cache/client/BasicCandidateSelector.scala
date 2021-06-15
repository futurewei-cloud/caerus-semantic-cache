package org.openinfralabs.caerus.cache.client

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Project, UnaryNode}
import org.apache.spark.sql.types._
import org.openinfralabs.caerus.cache.common.plans.{CaerusPlan, CaerusSourceLoad}
import org.openinfralabs.caerus.cache.common.{Caching, Candidate, FileSkippingIndexing, Repartitioning}

case class BasicCandidateSelector() extends CandidateSelector {
  override def getCandidates(plan: CaerusPlan): Seq[ Candidate ] = {
    getCandidates(plan, Seq.empty[ Attribute ], Seq.empty[ Attribute ])
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
    plan: CaerusPlan,
    filterAttributes: Seq[ Attribute ],
    shuffleAttributes: Seq[ Attribute ]
  ): Seq[ Candidate ] = {
    plan match {
      case caerusSourceLoad: CaerusSourceLoad =>
        val repartitionCandidates: Seq[ Candidate ] =
          shuffleAttributes
            .flatMap(attributeToIndex(caerusSourceLoad, _))
            .map(index => Repartitioning(caerusSourceLoad, index))
        val fileSkippingIndexingCandidates: Seq[ Candidate ] =
          filterAttributes
            .flatMap(attributeToIndex(caerusSourceLoad, _))
            .map(index => FileSkippingIndexing(caerusSourceLoad, index))
        Seq(Caching(caerusSourceLoad)) ++ repartitionCandidates ++ fileSkippingIndexingCandidates
      case aggr: Aggregate =>
        val newShuffleAttributes: Seq[ Attribute ] =
          aggr.groupingExpressions.flatMap(_.references.toSeq).filter(attrib => isNumeric(attrib) || isDate(attrib))
        Seq(Caching(plan)) ++ getCandidates(aggr.child, Seq.empty[ Attribute ], newShuffleAttributes)
      case filter: Filter =>
        val newFilterAttributes: Seq[ Attribute ] =
          filter.expressions.flatMap(_.references.toSeq).filter(attrib => isNumeric(attrib) || isDate(attrib))
        Seq(Caching(plan)) ++ getCandidates(filter.child, filterAttributes ++ newFilterAttributes, shuffleAttributes)
      case project: Project =>
        Seq(Caching(plan)) ++ getCandidates(project.child, filterAttributes, shuffleAttributes)
      case _: UnaryNode =>
        getCandidates(plan.asInstanceOf[ UnaryNode ].child, filterAttributes, shuffleAttributes)
      case _ =>
        plan.children.flatMap(getCandidates(_, Seq.empty[ Attribute ], Seq.empty[ Attribute ]))
    }
  }
}
