package org.openinfralabs.caerus.cache.common

import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, Unevaluable}
import org.apache.spark.sql.types.{DataType, Metadata}

package object plans {
  import org.apache.spark.sql.catalyst.expressions.Attribute
  import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

  type CaerusPlan = LogicalPlan

  object CaerusPlan {
    def fromJSON(json: String): CaerusPlan = {
      val caerusPlanSerDe: CaerusPlanSerDe = new CaerusPlanSerDe
      caerusPlanSerDe.deserializeCaerusPlan(json)
    }

    def postOrder(plan: CaerusPlan): Seq[CaerusPlan] = {
      plan.children.flatMap(CaerusPlan.postOrder) :+ plan.withNewChildren(plan.children.map(_ => CaerusEmpty()))
    }
  }

  abstract class CaerusLoad(override val output: Seq[Attribute]) extends CaerusPlan {
    override final def children: Seq[CaerusPlan] = Seq.empty[CaerusPlan]
  }

  case class CaerusEmpty() extends CaerusLoad(Seq.empty[Attribute])

  case class CaerusSourceLoad(override val output: Seq[Attribute], sources: Seq[SourceInfo], format: String)
      extends CaerusLoad(output) {
    override def canEqual(other: Any): Boolean = {
      other.isInstanceOf[CaerusSourceLoad] && other.asInstanceOf[CaerusSourceLoad].format == format
    }

    override def equals(other: Any): Boolean = {
      other match {
        case otherCaerusSourceLoad: CaerusSourceLoad =>
          canEqual(otherCaerusSourceLoad) && otherCaerusSourceLoad.sources == sources
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
      var res1: Boolean = false
      var res2: Seq[SourceInfo] = Seq.empty[SourceInfo]
      for (source <- sources) {
        if (other.sources.contains(source)) {
          res1 = true
        } else {
          res2 :+= source
        }
      }
      if (res1)
        Some(res2)
      else
        None
    }

    /**
     * See if the set of source files of this CaerusSourceLoad is a subset of the corresponding set of the other
     * CaerusSourceLoad.
     * @param other : Other source to compare with.
     * @return the source files that only other CaerusSourceLoad has, if this source is a subset of other. Otherwise,
     *         None.
     */
    def subsetOf(other: CaerusSourceLoad): Option[Seq[SourceInfo]] = {
      Console.out.println("Find if %s is a subset of %s.\n".format(this, other))
      if (!canEqual(other))
        return None
      var curIndex: Int = 0
      var res: Seq[SourceInfo] = Seq.empty[SourceInfo]
      for (source <- sources) {
        val newIndex: Int = other.sources.indexOf(source, curIndex)
        if (newIndex == -1)
          return None
        res ++= other.sources.slice(curIndex, newIndex)
        curIndex = newIndex+1
      }
      res ++= other.sources.slice(curIndex, other.sources.length)
      Some(res)
    }
  }

  case class CaerusCacheLoad(override val output: Seq[Attribute], sources: Seq[String], format: String)
      extends CaerusLoad(output) {
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
      path: Seq[String],
      index: Int) extends CaerusLoad(output) {
    override val innerChildren: Seq[CaerusPlan] = Seq(child)
  }

  case class CaerusAttribute(index: Int, dataType: DataType) extends Attribute with Unevaluable {
    override def withNullability(newNullability: Boolean): Attribute = this
    override def withQualifier(newQualifier: Seq[String]): Attribute = this
    override def withName(newName: String): Attribute = this
    override def withMetadata(newMetadata: Metadata): Attribute = this
    override def withExprId(newExprId: ExprId): Attribute = this
    override def newInstance(): Attribute = CaerusAttribute(index, dataType)
    override def name: String = "none"
    override def exprId: ExprId = ExprId(index.toLong)
    override def qualifier: Seq[String] = Seq.empty[String]
    override def nullable: Boolean = true

    def sameRef(other: CaerusAttribute): Boolean = index == other.index

    override def equals(other: Any): Boolean = other match {
      case otherCaerusAttribute: CaerusAttribute =>
        index == otherCaerusAttribute.index && dataType == otherCaerusAttribute.dataType
      case _ => false
    }

    override def semanticEquals(other: Expression): Boolean = other match {
      case otherCaerusAttribute: CaerusAttribute => sameRef(otherCaerusAttribute)
      case _ => false
    }

    override def toString: String = "%s#%s: %s".format(name, index, dataType)

    override def simpleString(maxFields: Int): String =
        "%s#%s: %s".format(name, index, dataType.simpleString(maxFields))

    override def sql: String = "%s#%s".format(name, index)
  }
}
