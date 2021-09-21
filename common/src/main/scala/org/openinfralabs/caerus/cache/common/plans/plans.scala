package org.openinfralabs.caerus.cache.common

import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics, UnaryNode}

package object plans {
  import org.apache.spark.sql.catalyst.expressions.Attribute
  import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

  type CaerusPlan = LogicalPlan

  object CaerusPlan {
    def fromJSON(json: String): CaerusPlan = {
      val caerusPlanSerDe: CaerusPlanSerDe = new CaerusPlanSerDe
      caerusPlanSerDe.deserializeCaerusPlan(json)
    }

    def preOrder(plan: CaerusPlan): Seq[CaerusPlan] = {
      plan.withNewChildren(plan.children.map(_ => CaerusEmpty())) +: plan.children.flatMap(CaerusPlan.preOrder)
    }

    def postOrder(plan: CaerusPlan): Seq[CaerusPlan] = {
      plan.children.flatMap(CaerusPlan.postOrder) :+ plan.withNewChildren(plan.children.map(_ => CaerusEmpty()))
    }

    def getRoot(plan: CaerusPlan): CaerusPlan = {
      plan.withNewChildren(plan.children.map(_ => CaerusEmpty()))
    }
  }

  abstract class CaerusLoad(override val output: Seq[Attribute]) extends LeafNode

  case class CaerusEmpty() extends CaerusLoad(Seq.empty[Attribute])

  case class CaerusSourceLoad(
    override val output: Seq[Attribute],
    sources: Seq[SourceInfo],
    format: String
  ) extends CaerusLoad(output) {

    private def sameSources(other: CaerusSourceLoad): Boolean = {
      val sourceSet = sources.toSet[SourceInfo]
      val otherSourceSet = other.sources.toSet[SourceInfo]
      sourceSet == otherSourceSet
    }

    def size: Long = sources.map(_.length).sum

    override def computeStats(): Statistics = Statistics(size)

    override def canEqual(other: Any): Boolean = {
      other.isInstanceOf[CaerusSourceLoad] && other.asInstanceOf[CaerusSourceLoad].format == format
    }

    override def equals(other: Any): Boolean = {
      other match {
        case otherCaerusSourceLoad: CaerusSourceLoad =>
          canEqual(otherCaerusSourceLoad) && sameSources(otherCaerusSourceLoad)
        case _ =>
          false
      }
    }

    /**
     * Find the intersection of this source with other source.
     * @param other Another source to intersect with.
     * @return the remaining SourceInfo from this node (this.sources-other.sources) if the intersection is non-empty,
     *         None otherwise.
     */
    def intersection(other: CaerusSourceLoad): Option[Seq[SourceInfo]] = {
      if (!canEqual(other))
        return None
      val rest: Seq[SourceInfo] = (sources.toSet[SourceInfo] -- other.sources.toSet[SourceInfo]).toSeq
      if (rest.size < sources.size)
        Some(rest)
      else
        None
    }

    /**
     * See if the set of source files of this CaerusSourceLoad is a subset of the corresponding set of the other
     * CaerusSourceLoad.
     * @param other : Other source to compare with.
     * @return the source files that only other CaerusSourceLoad has (other.sources - this.sources), if this source is a
     *         subset of other. Otherwise, None.
     */
    def subsetOf(other: CaerusSourceLoad): Option[Seq[SourceInfo]] = {
      if (!canEqual(other)) {
        return None
      }
      val remainder: Option[Set[SourceInfo]] =
        SourceInfo.subsetOf(this.sources.toSet[SourceInfo], other.sources.toSet[SourceInfo])
      if (remainder.isDefined)
        Some(remainder.get.toSeq)
      else
        None
    }
  }

  case class CaerusCacheLoad(override val output: Seq[Attribute], sources: Seq[String], format: String)
    extends CaerusLoad(output) {
    /**
     * Union of two CaerusLoad plans.
     * @param other The second plan to merge.
     * @return new CaerusLoad plan which contains all records from this and other plan.
     */
    def merge(other: CaerusPlan): CaerusPlan = {
      assert(other.output == output)
      other match {
        case cacheLoad: CaerusCacheLoad if cacheLoad.format == format =>
          CaerusCacheLoad(output, sources ++ cacheLoad.sources, format)
        case union: CaerusUnion =>
          var cacheChildren: Seq[CaerusCacheLoad] = Seq.empty[CaerusCacheLoad]
          var otherChildren: Seq[CaerusLoad] = Seq.empty[CaerusLoad]
          union.children.foreach {
            case cacheLoad: CaerusCacheLoad if cacheLoad.format == format => cacheChildren :+= cacheLoad
            case load: CaerusLoad => otherChildren :+= load
            case otherLoad => throw new RuntimeException("The following plan cannot be part of CaerusUnion:\n%s"
              .format(otherLoad))
          }
          assert(cacheChildren.isEmpty || cacheChildren.length == 1)
          val newCaerusPlan: CaerusPlan = if (cacheChildren.nonEmpty) merge(cacheChildren.head) else this
          if (otherChildren.nonEmpty)
            CaerusUnion(output, newCaerusPlan +: otherChildren)
          else
            newCaerusPlan
        case load: CaerusLoad => CaerusUnion(output, Seq(this, load))
        case _ =>
          throw new RuntimeException("This type of Caerus Plan cannot be merged:\n%s".format(other))
      }
    }
  }

  case class CaerusUnion(override val output: Seq[Attribute], override val children: Seq[CaerusPlan]) extends CaerusPlan

  case class CaerusLoadWithIndices(
    override val output: Seq[Attribute],
    @transient child: CaerusPlan,
    sources: Seq[SourceInfo],
    index: Int
  ) extends CaerusLoad(output) {
    override val innerChildren: Seq[CaerusPlan] = Seq(child)
  }

  abstract class CaerusWrite extends UnaryNode {
    val name = "none"
    override def output: Seq[Attribute] = child.output
  }
  case class CaerusRepartitioning(override val name: String, override val child: CaerusSourceLoad, index: Int)
    extends CaerusWrite
  case class CaerusFileSkippingIndexing(override val name: String, override val child: CaerusSourceLoad, index: Int)
    extends CaerusWrite
  case class CaerusCaching(override val name: String, plan: CaerusPlan, override val child: CaerusPlan)
    extends CaerusWrite

  case class CaerusDelete(paths: Seq[String]) extends CaerusPlan {
    override def output: Seq[Attribute] = Seq.empty[Attribute]
    override def children: Seq[CaerusPlan] = Seq.empty[CaerusPlan]
  }


  case class CaerusIf(override val children: Seq[CaerusPlan]) extends CaerusPlan {
    assert(this.children.size == 3)
    assert(this.children(1).output == this.children(2).output)

    override def output: Seq[Attribute] = children(1).output
  }

  case class CaerusTrue() extends LogicalPlan {
    override def output: Seq[Attribute] = Seq.empty[Attribute]
    override def children: Seq[CaerusPlan] = Seq.empty[CaerusPlan]
  }

  case class CaerusFalse() extends LogicalPlan {
    override def output: Seq[Attribute] = Seq.empty[Attribute]
    override def children: Seq[CaerusPlan] = Seq.empty[CaerusPlan]
  }
}
