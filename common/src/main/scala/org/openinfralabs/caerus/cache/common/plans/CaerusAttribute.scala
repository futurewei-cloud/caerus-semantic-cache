package org.openinfralabs.caerus.cache.common.plans

import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, Expression, Unevaluable}
import org.apache.spark.sql.types.{DataType, Metadata}

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
