package org.openinfralabs.caerus.cache.manager

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{BooleanType, DataType, StringType}
import org.openinfralabs.caerus.cache.common._
import org.openinfralabs.caerus.cache.common.plans._
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.mutable

case class UnifiedOptimizer() extends Optimizer {
  type DataTransform = Option[(Set[Set[Expression]], Seq[Int], Option[CaerusPlan], Seq[CaerusPlan])]

  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def indexOfAttribute(attrib: Attribute): Int = attrib.exprId.id.toInt

  private def attributeTransformationForChild(attributes: Set[Int], start: Int, child: CaerusPlan): Map[Int,Int] = {
    val res: mutable.HashMap[Int,Int] = mutable.HashMap.empty[Int,Int]
    attributes.foreach(attribute => {
      val childAttribute = attribute - start
      if (childAttribute >= 0 && childAttribute < child.output.length)
        res(attribute) = indexOfAttribute(child.output(childAttribute))
    })
    res.toMap[Int,Int]
  }

  private def propagateAttributesToChildren(attributes: Set[Int], children: Seq[CaerusPlan]): Seq[Set[Int]] = {
    var currentAttribute = 0
    children.map{child =>
      val transformationMap: Map[Int,Int] = attributeTransformationForChild(attributes, currentAttribute, child)
      currentAttribute += child.output.length
      attributes.flatMap{attribute =>
        if (transformationMap.contains(attribute))
          Seq(transformationMap(attribute))
        else
          None
      }
    }
  }

  private def transformAttributeInCondition(condition: Expression, previousAttrib: Int, newAttrib: Int): Expression = {
    condition match {
      case ref: AttributeReference if ref.exprId.id.toInt == previousAttrib =>
        ref.withExprId(ExprId(newAttrib.toLong, ref.exprId.jvmId))
      case _ =>
        condition.withNewChildren(condition.children.map(transformAttributeInCondition(_, previousAttrib, newAttrib)))
    }
  }

  private def propagateFilterStatusToChildren(
      filterStatus: Map[Int,Expression],
      children: Seq[CaerusPlan]): Seq[Map[Int,Expression]] = {
    var currentAttribute = 0
    children.map(child => {
      val transformationMap: Map[Int,Int] =
        attributeTransformationForChild(filterStatus.keys.toSet, currentAttribute, child)
      currentAttribute += child.output.length
      val newFilterStatus: mutable.HashMap[Int,Expression] = mutable.HashMap.empty[Int,Expression]
      filterStatus.keys.foreach(attribute =>
        if (transformationMap.contains(attribute)) {
          val newAttribute: Int = transformationMap(attribute)
          newFilterStatus(newAttribute) =
            transformAttributeInCondition(filterStatus(attribute), attribute, newAttribute)
        }
      )
      newFilterStatus.toMap[Int,Expression]
    })
  }

  private def normalizeCondition(condition: Expression): Map[Int,Expression] = {
    condition match {
      case EqualTo(attribute: CaerusAttribute, lit: Literal) => Map(attribute.index -> EqualTo(attribute,lit))
      case EqualTo(lit: Literal, attribute: CaerusAttribute) => Map(attribute.index -> EqualTo(attribute,lit))
      case EqualNullSafe(attrib: Attribute, lit: Literal) => Map(indexOfAttribute(attrib) -> EqualNullSafe(attrib,lit))
      case EqualNullSafe(lit: Literal, attrib: Attribute) => Map(indexOfAttribute(attrib) -> EqualNullSafe(attrib,lit))
      case GreaterThan(attrib: Attribute, lit: Literal) => Map(indexOfAttribute(attrib) -> GreaterThan(attrib,lit))
      case GreaterThan(lit: Literal, attrib: Attribute) => Map(indexOfAttribute(attrib) -> LessThan(attrib,lit))
      case LessThan(attrib: Attribute, lit: Literal) => Map(indexOfAttribute(attrib) -> LessThan(attrib,lit))
      case LessThan(lit: Literal, attrib: Attribute) => Map(indexOfAttribute(attrib) -> GreaterThan(attrib,lit))
      case GreaterThanOrEqual(attrib: Attribute, lit: Literal) =>
        Map(indexOfAttribute(attrib) -> GreaterThanOrEqual(attrib,lit))
      case GreaterThanOrEqual(lit: Literal, attrib: Attribute) =>
        Map(indexOfAttribute(attrib) -> LessThanOrEqual(attrib,lit))
      case LessThanOrEqual(attrib: Attribute, lit: Literal) =>
        Map(indexOfAttribute(attrib) -> LessThanOrEqual(attrib,lit))
      case LessThanOrEqual(lit: Literal, attrib: Attribute) =>
        Map(indexOfAttribute(attrib) -> GreaterThanOrEqual(attrib,lit))
      case And(left: Expression, right: Expression) =>
        val normalizedLeft: Map[Int,Expression] = normalizeCondition(left)
        val normalizedRight: Map[Int,Expression] = normalizeCondition(right)
        val indices: Seq[Int] = normalizedLeft.keys.toSet.union(normalizedRight.keys.toSet).toSeq
        val res: mutable.HashMap[Int,Expression] = mutable.HashMap.empty[Int,Expression]
        indices.foreach(index =>
          if (normalizedLeft.contains(index) && normalizedRight.contains(index))
            res(index) = And(normalizedLeft(index), normalizedRight(index))
          else if (normalizedLeft.contains(index))
            res(index) = normalizedLeft(index)
          else
            res(index) = normalizedRight(index)
        )
        res.toMap[Int,Expression]
      case Or(left: Expression, right: Expression) =>
        val normalizedLeft: Map[Int,Expression] = normalizeCondition(left)
        val normalizedRight: Map[Int,Expression] = normalizeCondition(right)
        val indices: Seq[Int] = normalizedLeft.keys.toSet.intersect(normalizedRight.keys.toSet).toSeq
        val res: mutable.HashMap[Int,Expression] = mutable.HashMap.empty[Int,Expression]
        indices.foreach(index => res(index) = Or(normalizedLeft(index), normalizedRight(index)))
        res.toMap[Int,Expression]
      case Not(child: Expression) =>
        val normalizedChild: Map[Int,Expression] = normalizeCondition(child)
        val indices: Seq[Int] = normalizedChild.keys.toSeq
        val res: mutable.HashMap[Int,Expression] = mutable.HashMap.empty[Int,Expression]
        indices.foreach(index => res(index) = Not(normalizedChild(index)))
        res.toMap[Int,Expression]
      case _ => Map.empty[Int,Expression]
    }
  }

  private def conjuctiveNormalForm(condition: Expression): Set[Set[Expression]] = {
    condition match {
      case EqualTo(attrib: Attribute, lit: Literal) => Set(Set(EqualTo(attrib,lit)))
      case EqualTo(lit: Literal, attrib: Attribute) => Set(Set(EqualTo(attrib,lit)))
      case EqualNullSafe(attrib: Attribute, lit: Literal) => Set(Set(EqualNullSafe(attrib,lit)))
      case EqualNullSafe(lit: Literal, attrib: Attribute) => Set(Set(EqualNullSafe(attrib,lit)))
      case GreaterThan(attrib: Attribute, lit: Literal) => Set(Set(GreaterThan(attrib,lit)))
      case GreaterThan(lit: Literal, attrib: Attribute) => Set(Set(LessThan(attrib,lit)))
      case LessThan(attrib: Attribute, lit: Literal) => Set(Set(LessThan(attrib,lit)))
      case LessThan(lit: Literal, attrib: Attribute) => Set(Set(GreaterThan(attrib,lit)))
      case GreaterThanOrEqual(attrib: Attribute, lit: Literal) => Set(Set(GreaterThanOrEqual(attrib,lit)))
      case GreaterThanOrEqual(lit: Literal, attrib: Attribute) => Set(Set(LessThanOrEqual(attrib,lit)))
      case LessThanOrEqual(attrib: Attribute, lit: Literal) => Set(Set(LessThanOrEqual(attrib,lit)))
      case LessThanOrEqual(lit: Literal, attrib: Attribute) => Set(Set(GreaterThanOrEqual(attrib,lit)))
      case And(left: Expression, right: Expression) => conjuctiveNormalForm(left).union(conjuctiveNormalForm(right))
      case Or(left: Expression, right: Expression) =>
        val cnfLeft: Set[Set[Expression]] = conjuctiveNormalForm(left)
        val cnfRight: Set[Set[Expression]] = conjuctiveNormalForm(right)
        var res: Set[Set[Expression]] = Set.empty[Set[Expression]]
        cnfLeft.foreach(leftSet => cnfRight.foreach(rightSet => res += leftSet.union(rightSet)))
        res
      case Not(child: Expression) =>
        child match {
          case _: GreaterThan => conjuctiveNormalForm(LessThan(child.children(1), child.children(0)))
          case _: LessThan => conjuctiveNormalForm(GreaterThan(child.children(1), child.children(0)))
          case _: GreaterThanOrEqual => conjuctiveNormalForm(LessThanOrEqual(child.children(1), child.children(0)))
          case _: LessThanOrEqual => conjuctiveNormalForm(GreaterThanOrEqual(child.children(1), child.children(0)))
          case And(left: Expression, right: Expression) => conjuctiveNormalForm(Or(Not(left),Not(right)))
          case Or(left: Expression, right: Expression) => conjuctiveNormalForm(And(Not(left), Not(right)))
          case Not(grandchild: Expression) => conjuctiveNormalForm(grandchild)
          case _ =>
            val cnfChild: Set[Set[Expression]] = conjuctiveNormalForm(child)
            assert(cnfChild.size == 1)
            assert(cnfChild.head.size == 1)
            Set(Set(Not(cnfChild.head.head)))
        }
      case _ => Set(Set(condition))
    }
  }

  private def lookupRepartitioning(
      source: CaerusSourceLoad,
      indices: Set[Int],
      contents: Map[Candidate, String]): Seq[(String, Seq[SourceInfo])] = {
    // Search for sources that are equal first.
    logger.info("Lookup for repartitions for indices %s with the same records as:\n%s"
      .format(indices.mkString(","), source))
    val equalSourcePaths: Seq[(String,Seq[SourceInfo])] = contents.keys.flatMap(candidate => {
      candidate match {
        case Repartitioning(repartitionedSource, partitionIndex, _)
          if indices.contains(partitionIndex) && repartitionedSource == source =>
            Some((contents(candidate), Seq.empty[SourceInfo]))
        case _ => None
      }
    }).toSeq
    if (equalSourcePaths.nonEmpty)
      return equalSourcePaths

    // Search for plans that partially fill the output.
    logger.info("Lookup for repartitions for indices %s with a subset of records as:\n%s"
      .format(indices.mkString(","), source))
    contents.keys.flatMap(candidate => {
      candidate match {
        case Repartitioning(repartitionedSource, partitionIndex, _) if indices.contains(partitionIndex) =>
          val diff: Option[Seq[SourceInfo]] = repartitionedSource.subsetOf(source)
          if (diff.isDefined)
            Some((contents(candidate), diff.get))
          else
            None
        case _ => None
      }
    }).toSeq
  }

  private def prepareMinMaxCondition(
    condition: Expression,
    minAttribute: Attribute,
    maxAttribute: Attribute
  ): Expression = {
    condition match {
      case EqualTo(_, lit: Literal) =>
        And(LessThanOrEqual(minAttribute, lit), GreaterThanOrEqual(maxAttribute,lit))
      case EqualNullSafe(_, lit: Literal) =>
        And(LessThanOrEqual(minAttribute, lit), GreaterThanOrEqual(maxAttribute,lit))
      case GreaterThan(_, lit: Literal) =>
        GreaterThan(maxAttribute, lit)
      case LessThan(_, lit: Literal) =>
        LessThan(minAttribute, lit)
      case GreaterThanOrEqual(_, lit: Literal) =>
        GreaterThanOrEqual(maxAttribute, lit)
      case LessThanOrEqual(_, lit: Literal) =>
        LessThanOrEqual(minAttribute,lit)
      case Not(child) =>
        child match {
          case EqualTo(_, lit: Literal) =>
            Or(Not(EqualTo(minAttribute,lit)),Not(EqualTo(maxAttribute,lit)))
          case EqualNullSafe(_, lit: Literal) =>
            Or(Not(EqualTo(minAttribute,lit)),Not(EqualTo(maxAttribute,lit)))
          case GreaterThan(attribute: Attribute, lit: Literal) =>
            prepareMinMaxCondition(LessThanOrEqual(attribute,lit), minAttribute, maxAttribute)
          case LessThan(attribute: Attribute, lit: Literal) =>
            prepareMinMaxCondition(GreaterThanOrEqual(attribute,lit), minAttribute, maxAttribute)
          case GreaterThanOrEqual(attribute: Attribute, lit: Literal) =>
            prepareMinMaxCondition(LessThan(attribute,lit), minAttribute, maxAttribute)
          case LessThanOrEqual(attribute: Attribute, lit: Literal) =>
            prepareMinMaxCondition(GreaterThan(attribute,lit), minAttribute, maxAttribute)
          case Not(grandchild) =>
            prepareMinMaxCondition(grandchild, minAttribute, maxAttribute)
        }
      case _: Or | _: And =>
        condition.withNewChildren(condition.children.map(prepareMinMaxCondition(_, minAttribute, maxAttribute)))
      case _ =>
        throw new RuntimeException("Condition %s cannot be evaluated.".format(condition))
    }
  }

  private def prepareFileSkippingIndexingLoad(path: Seq[String], condition: Expression, dtype: DataType): CaerusPlan = {
    val output: Seq[AttributeReference] = Seq(
      AttributeReference("none", StringType)(ExprId(0L)),
      AttributeReference("none", dtype)(ExprId(1L)),
      AttributeReference("none", dtype)(ExprId(2L))
    )
    val cacheLoad: CaerusCacheLoad = CaerusCacheLoad(output, path, "json")
    val filter: Filter = Filter(prepareMinMaxCondition(condition, output(1), output(2)), cacheLoad)
    Project(Seq(output.head), filter)
  }

  private def lookupFileSkippingIndexing(
    source: CaerusSourceLoad,
    indices: Set[Int],
    contents: Map[Candidate, String]): Seq[(String, Int, Seq[SourceInfo])] = {
    // Search for file-skipping indices that contain same files as the source.
    logger.info("Lookup for file-skipping indexing for indices %s with similar records as:\n%s"
      .format(indices.mkString(","), source))
    contents.keys.flatMap(candidate => {
      candidate match {
        case FileSkippingIndexing(indexedSource, index, _)
          if indices.contains(index) && indexedSource.canEqual(source) =>
          val intersection: Option[Seq[SourceInfo]] = source.intersection(indexedSource)
          if (intersection.isDefined)
            Some(contents(candidate), index, intersection.get)
          else
            None
        case _ => None
      }
    }).toSeq
  }

  private def orConditionEquals(set1: Set[Expression], set2: Set[Expression]): Boolean = {
    val condition1: Expression = makeOrCondition(set1)
    val condition2: Expression = makeOrCondition(set2)

    logger.info("Compare condition %s with condition %s.".format(condition1, condition2))
    (condition1.canonicalized, condition2.canonicalized) match {
      case (IsNotNull(attrib1: AttributeReference), IsNotNull(attrib2: AttributeReference)) =>
        logger.info("Attribute1: %s-%s-%s-%s-%s-%s\nAttribute2: %s-%s-%s-%s-%s-%s".format(
          attrib1.name, attrib1.dataType, attrib1.nullable, attrib1.metadata, attrib1.exprId, attrib1.qualifier,
          attrib2.name, attrib2.dataType, attrib2.nullable, attrib2.metadata, attrib2.exprId, attrib2.qualifier
        ))
      case _ => logger.info("No IsNotNull nodes.")
    }
    makeOrCondition(set1).semanticEquals(makeOrCondition(set2))
  }

  @tailrec
  private def conditionReduce(
      cachedConditions: Set[Set[Expression]],
      conditions: Set[Set[Expression]]
  ): Option[Set[Set[Expression]]] = {
    if (cachedConditions.isEmpty) {
      Some(conditions)
    } else {
      val newSet: Option[Set[Expression]] = conditions.find(set => orConditionEquals(cachedConditions.head, set))
      logger.info("Found condition %s that is semantic equal to condition %s.".format(newSet, cachedConditions.head))
      if (newSet.isDefined)
        conditionReduce(cachedConditions.tail, conditions - newSet.get)
      else
        None
    }
  }

  private def dataReduce(cachedPlan: CaerusPlan, initialPlan: Seq[CaerusPlan]): DataTransform = {
    logger.info("Data Reduce Plans:\n%s\nto\n%s".format(cachedPlan, initialPlan.mkString("\n")))
    var plan: Seq[CaerusPlan] = initialPlan
    var dataTransform: DataTransform = (cachedPlan, plan.head) match {
      case (CaerusSourceLoad(_, cachedSources, cachedFormat), CaerusSourceLoad(output, sources, format))
        if format == cachedFormat =>
        val newSources: Option[Set[SourceInfo]] =
          SourceInfo.subsetOf(cachedSources.toSet[SourceInfo], sources.toSet[SourceInfo])
        if (newSources.isDefined) {
          val unionLoad: Option[CaerusPlan] =
            if (newSources.get.nonEmpty)
              Some(CaerusSourceLoad(output, newSources.get.toSeq, format))
            else
              None
          plan = plan.tail
          Some(Set.empty[Set[Expression]], output.map(attrib => indexOfAttribute(attrib)), unionLoad, plan)
        } else {
          None
        }
      case (Filter(cachedCondition: Expression, cachedChild: CaerusPlan), _) =>
        val dataTransform: DataTransform = dataReduce(cachedChild, plan)
        logger.info("Child Data Transform: %s".format(dataTransform))
        if (dataTransform.isEmpty) {
          None
        } else {
          val cachedConditions: Set[Set[Expression]] = conjuctiveNormalForm(cachedCondition)
          logger.info("Cached Conditions: %s".format(cachedConditions))
          val conditions: Set[Set[Expression]] = dataTransform.get._1
          val newConditions: Option[Set[Set[Expression]]] = conditionReduce(cachedConditions, conditions)
          if (newConditions.isDefined)
            Some((newConditions.get, dataTransform.get._2, dataTransform.get._3, dataTransform.get._4))
          else
            None
        }
      case _ => None
    }
    logger.info("Intermediate Data Transform:\n%s".format(dataTransform))
    while (dataTransform.isDefined && plan.nonEmpty) {
      dataTransform = plan.head match {
        case Filter(condition, _) =>
          val newConditions = dataTransform.get._1 ++ conjuctiveNormalForm(condition)
          val newUnionLoad: Option[CaerusPlan] =
            if (dataTransform.get._3.isDefined)
              Some(Filter(condition, dataTransform.get._3.get))
            else
              None
          plan = plan.tail
          Some((newConditions, dataTransform.get._2, newUnionLoad, plan))
        case Project(projectList, _) =>
          val newSequence: Seq[Int] = projectList.map(attrib => dataTransform.get._2(attrib.exprId.id.toInt))
          val newUnionLoad: Option[CaerusPlan] =
            if (dataTransform.get._3.isDefined)
              Some(Project(projectList, dataTransform.get._3.get))
            else
              None
          plan = plan.tail
          Some((dataTransform.get._1, newSequence, newUnionLoad, plan))
        case _ =>
          return dataTransform
      }
    }
    dataTransform
  }

  private def fromConjuctiveNormalForm(andExpressions: Set[Set[Expression]]): Expression = {
    makeAndCondition(andExpressions.flatMap(orExpressions => {
      val res: Expression = makeOrCondition(orExpressions)
      res match {
        case Literal(true, BooleanType) => None
        case _ => Some(res)
      }
    }))
  }

  private def makeAndCondition(andExpressions: Set[Expression]): Expression = {
    if (andExpressions.isEmpty)
      Literal(true, BooleanType)
    else if (andExpressions.size == 1)
      andExpressions.head
    else {
      And(andExpressions.head, makeAndCondition(andExpressions.tail))
    }
  }

  private def makeOrCondition(orExpressions: Set[Expression]): Expression = {
    if (orExpressions.isEmpty)
      Literal(true, BooleanType)
    else if (orExpressions.size == 1)
      orExpressions.head
    else {
      Or(orExpressions.head, makeOrCondition(orExpressions.tail))
    }
  }

  private def dataReduce(
      cachedPlan: CaerusPlan,
      initialPlan: Seq[CaerusPlan],
      caerusCacheLoad: CaerusCacheLoad
  ): Option[(CaerusPlan, Option[CaerusPlan])] = {
    val dataTransform: DataTransform = dataReduce(cachedPlan, initialPlan)
    if (dataTransform.isEmpty || dataTransform.get._4.nonEmpty)
      return None
    var res: CaerusPlan = caerusCacheLoad
    if (dataTransform.get._1.nonEmpty) {
      val condition: Expression = fromConjuctiveNormalForm(dataTransform.get._1)
      res = condition match {
        case Literal(true, BooleanType) => res
        case _ => Filter(condition, res)
      }
    }
    logger.info("DataTransform: %s\nOutput:%s".format(dataTransform.get._2, res.output))
    if (dataTransform.get._2.length < res.output.length)
      res = Project(dataTransform.get._2.map(index => res.output(index)), res)
    Some((res, dataTransform.get._3))
  }

  private def updateReferences(plan: CaerusPlan, addReference: String => Unit): Unit = {
    plan match {
      case cacheLoad: CaerusCacheLoad => cacheLoad.sources.foreach(addReference)
      case _ => plan.children.foreach(child => updateReferences(child, addReference))
    }
  }

  private def lookupCaching(
      plan: CaerusPlan,
      contents: Map[Candidate,String],
      addReference: String => Unit
  ): Seq[(CaerusPlan, Option[CaerusPlan])] = {
    // Search for plans that are equal first.
    val res1: Seq[(CaerusPlan, Option[CaerusPlan])] = contents.keys.flatMap {
      case candidate@Caching(cachedPlan, _) if cachedPlan == plan =>
        val cacheLoad: CaerusCacheLoad = CaerusCacheLoad(cachedPlan.output, Seq(contents(candidate)), "parquet")
        Some(cacheLoad, None)
      case _ =>
        None
    }.toSeq
    if (res1.nonEmpty) {
      updateReferences(res1.head._1, addReference)
      return res1
    }

    // Search for plans for which the plan can benefit from.
    val postOrderPlan = CaerusPlan.postOrder(plan)
    logger.info("Post-order plan:\n%s".format(postOrderPlan.mkString("\n")))
    val res2: Seq[(CaerusPlan, Option[CaerusPlan])] = contents.keys.flatMap(candidate => candidate match {
      case Caching(cachedPlan, _) =>
        val cacheLoad = CaerusCacheLoad(cachedPlan.output, Seq(contents(candidate)), "parquet")
        dataReduce(cachedPlan, postOrderPlan, cacheLoad)
      case _ => None
    }).toSeq
    if (res2.nonEmpty)
      updateReferences(res2.head._1, addReference)
    res2
  }

  private def optimizeWithAttributes(
      plan: CaerusPlan,
      shuffleAttributes: Set[Int],
      filterStatus: Map[Int,Expression],
      contents: Map[Candidate,String],
      addReference: String => Unit
  ): CaerusPlan = {
    logger.info("Optimize plan:\n%s".format(plan))

    // Look for cached intermediate results.
    logger.info("Look for cached node:\n%s".format(plan))
    val cachingCandidates: Seq[(CaerusPlan,Option[CaerusPlan])] = lookupCaching(plan, contents, addReference)
    if (cachingCandidates.nonEmpty) {
      logger.info("Found caching candidates:\n%s".format(cachingCandidates.mkString("\n")))
      val optimizedPlan: CaerusPlan = cachingCandidates.head._1
      val remainderPlan: Option[CaerusPlan] = cachingCandidates.head._2
      if (remainderPlan.isDefined)
        return CaerusUnion(plan.output, Seq(
            optimizedPlan,
            optimizeWithAttributes(remainderPlan.get, shuffleAttributes, filterStatus, contents, addReference)
        ))
      else
        return optimizedPlan
    }

    // Detect new attributes for repartitioning and file-skipping indexing.
    logger.info("Shuffle attributes: %s\n".format(shuffleAttributes.mkString(",")))
    logger.info("Filter status: %s\n".format(filterStatus))
    val newAttributes: (Set[Int],Map[Int,Expression]) = plan match {
      case Aggregate(groupingExpressions, _, _) =>
        val newShuffleAttributes: Set[Int] =
          groupingExpressions.flatMap(_.references.toSeq).map(indexOfAttribute).toSet
        logger.info("Detected shuffle attributes in aggregate node: %s\n".format(newShuffleAttributes.mkString(",")))
        (shuffleAttributes ++ newShuffleAttributes, filterStatus)
      case Filter(condition, _) =>
        val tempFilterStatus: Map[Int,Expression] = normalizeCondition(condition)
        val attributes: Seq[Int] = filterStatus.keys.toSet.union(tempFilterStatus.keys.toSet).toSeq
        val newFilterStatus: mutable.HashMap[Int,Expression] = mutable.HashMap.empty[Int,Expression]
        attributes.foreach(attrib =>
          if (filterStatus.contains(attrib) && tempFilterStatus.contains(attrib))
            newFilterStatus(attrib) = And(filterStatus(attrib), tempFilterStatus(attrib))
          else if (filterStatus.contains(attrib))
            newFilterStatus(attrib) = filterStatus(attrib)
          else
            newFilterStatus(attrib) = tempFilterStatus(attrib)
        )
        logger.info("Modified filter status in filter node: %s\n".format(newFilterStatus))
        (shuffleAttributes, newFilterStatus.toMap[Int,Expression])
      case RepartitionByExpression(partitionExpressions, _, _) =>
        val newShuffleAttributes: Set[Int] =
          partitionExpressions.flatMap(_.references.toSeq).map(indexOfAttribute).toSet
        logger.info("Detected shuffle attributes in repartition node: %s\n", newShuffleAttributes.mkString(","))
        (shuffleAttributes ++ newShuffleAttributes, filterStatus)
      case _ =>
        (shuffleAttributes, filterStatus)
    }

    // Optimize plan in case it is a source. Otherwise, optimize children.
    plan match {
      case caerusSourceLoad: CaerusSourceLoad =>
        logger.info("Look for repartition for source node:\n%s".format(caerusSourceLoad))
        val repartitionAttributes: Set[Int] = shuffleAttributes ++ filterStatus.keys.toSet
        logger.info("Indices for repartition: %s\n".format(repartitionAttributes.mkString(",")))
        val repartitionCandidates: Seq[(String, Seq[SourceInfo])] =
          lookupRepartitioning(caerusSourceLoad, repartitionAttributes, contents)
        logger.info("Found repartitions: %s\n".format(repartitionCandidates.map(_._1).mkString(",")))
        if (repartitionCandidates.nonEmpty) {
          val path: String = repartitionCandidates.head._1
          val remainderSources: Seq[SourceInfo] = repartitionCandidates.head._2
          addReference(path)
          val caerusCacheLoad: CaerusCacheLoad = CaerusCacheLoad(caerusSourceLoad.output, Seq(path), "parquet")
          if (remainderSources.nonEmpty) {
            val remainderSource: CaerusSourceLoad =
              CaerusSourceLoad(caerusSourceLoad.output, remainderSources, caerusSourceLoad.format)
            val optimizedSource: CaerusPlan =
              optimizeWithAttributes(remainderSource, shuffleAttributes, filterStatus, contents, addReference)
            optimizedSource match {
              case _: CaerusLoad | _: CaerusUnion =>
                return caerusCacheLoad.merge(optimizedSource)
              case other =>
                assert(
                  assertion=false,
                  "The following plans should have never been equivalent.\n%s%s".format(caerusSourceLoad, other)
                )
            }
          } else {
            return caerusCacheLoad
          }
        }
        logger.info("Look for file-skipping indices for source node:\n%s".format(caerusSourceLoad))
        logger.info("Potential file-skipping indices: %s\n".format(filterStatus.keys.mkString(",")))
        val fileSkippingIndexingCandidates: Seq[(String, Int, Seq[SourceInfo])] =
          lookupFileSkippingIndexing(caerusSourceLoad, filterStatus.keys.toSet[Int], contents)
        logger.info("Found file-skipping indices: %s\n".format(fileSkippingIndexingCandidates.map(_._1).mkString(",")))
        if (fileSkippingIndexingCandidates.nonEmpty) {
          val path: String = fileSkippingIndexingCandidates.head._1
          val index: Int = fileSkippingIndexingCandidates.head._2
          val remainderSources: Seq[SourceInfo] = fileSkippingIndexingCandidates.head._3
          addReference(path)
          val indexLoadPlan: CaerusPlan =
            prepareFileSkippingIndexingLoad(Seq(path), filterStatus(index), caerusSourceLoad.output(index).dataType)
          val caerusLoadWithIndices: CaerusLoadWithIndices =
            CaerusLoadWithIndices(caerusSourceLoad.output, indexLoadPlan, caerusSourceLoad.sources.map(_.path), index)
          if (remainderSources.nonEmpty) {
            val remainderSource: CaerusSourceLoad =
              CaerusSourceLoad(caerusSourceLoad.output, remainderSources, caerusSourceLoad.format)
            val optimizedSource: CaerusPlan =
              optimizeWithAttributes(remainderSource, shuffleAttributes, filterStatus, contents, addReference)
            optimizedSource match {
              case caerusUnionLoad: CaerusUnion =>
                CaerusUnion(caerusSourceLoad.output, caerusLoadWithIndices +: caerusUnionLoad.children)
              case caerusLoad: CaerusLoad =>
                CaerusUnion(caerusSourceLoad.output, Seq(caerusLoadWithIndices, caerusLoad))
              case other =>
                throw new RuntimeException("The following plans should have never been equivalent.\n%s%s"
                  .format(caerusSourceLoad, other))
            }
          } else {
            caerusLoadWithIndices
          }
        } else {
          caerusSourceLoad
        }
      case _ =>
        val childrenShuffleAttributes: Seq[Set[Int]] = propagateAttributesToChildren(newAttributes._1, plan.children)
        val childrenFilterStatus: Seq[Map[Int,Expression]] =
          propagateFilterStatusToChildren(newAttributes._2, plan.children)
        plan.withNewChildren(plan.children.indices.map(i => optimizeWithAttributes(
            plan.children(i),
            childrenShuffleAttributes(i),
            childrenFilterStatus(i),
            contents,
            addReference
        )))
    }
  }

  override def optimize(plan: CaerusPlan, contents: Map[Candidate,String], addReference: String => Unit): CaerusPlan = {
    logger.info("Contents available for optimization:\n%s".format(contents.mkString("(", "\n", ")")))
    optimizeWithAttributes(plan, Set.empty[Int], Map.empty[Int,Expression], contents, addReference)
  }
}
