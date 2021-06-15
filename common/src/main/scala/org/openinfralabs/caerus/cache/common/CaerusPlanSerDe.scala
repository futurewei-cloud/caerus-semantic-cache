package org.openinfralabs.caerus.cache.common

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.DateTimeConstants.{MICROS_PER_HOUR, MICROS_PER_MINUTE}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.openinfralabs.caerus.cache.common.plans.{CaerusPlan, CaerusSourceLoad}

import java.sql.Timestamp
import java.time.LocalDate
import java.util.UUID

class CaerusPlanSerDe {
  val ru: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe

  private[common] def serializeCaerusPlan(plan: CaerusPlan): String = {
    plan.toJSON
  }

  private[common] def deserializeCaerusPlan(json: String): CaerusPlan = {
    val jsonAst = parse(json)

    assert(jsonAst.isInstanceOf[JArray])
    reconstruct[CaerusPlan](jsonAst.asInstanceOf[JArray])
  }

  private[common] def serializeCandidate(candidate: Candidate): String = {
    val jsonFields: List[(String,JValue)] = candidate match {
      case repartitioning: Repartitioning =>
        assert(repartitioning.sizeInfo.isDefined)
        List(
          ("class", JString(repartitioning.getClass.getName)),
          ("source", JString(repartitioning.source.toJSON)),
          ("index", JInt(repartitioning.index)),
          ("sizeInfo", JString(repartitioning.sizeInfo.get.toJSON))
        )
      case fileSkippingIndexing: FileSkippingIndexing =>
        assert(fileSkippingIndexing.sizeInfo.isDefined)
        List(
          ("class", JString(fileSkippingIndexing.getClass.getName)),
          ("source", JString(fileSkippingIndexing.source.toJSON)),
          ("index", JInt(fileSkippingIndexing.index)),
          ("sizeInfo", JString(fileSkippingIndexing.sizeInfo.get.toJSON))
        )
      case caching: Caching =>
        assert(caching.sizeInfo.isDefined)
        List(
          ("class", JString(caching.getClass.getName)),
          ("plan", JString(caching.plan.toJSON)),
          ("sizeInfo", JString(caching.sizeInfo.get.toJSON))
        )
      case _ =>
        throw new RuntimeException("Invalid candidate %s.".format(candidate))
    }
    compact(render(JObject(jsonFields)))
  }

  private[common] def deserializeCandidate(json: String): Candidate = {
    val jsonAst = parse(json)

    assert(jsonAst.isInstanceOf[JObject])

    val jsonObject = jsonAst.asInstanceOf[JObject]
    val sizeInfo: SizeInfo = SizeInfo.fromJSON((jsonObject \ "sizeInfo").asInstanceOf[JString].s)
    val cls = Class.forName((jsonObject \ "class").asInstanceOf[JString].s)

    cls match {
      case _ if cls == classOf[Repartitioning] =>
        val source: CaerusSourceLoad =
          deserializeCaerusPlan((jsonObject \ "source").asInstanceOf[JString].s).asInstanceOf[CaerusSourceLoad]
        val index: Int = (jsonObject \ "index").asInstanceOf[JInt].num.toInt
        Repartitioning(source, index, Some(sizeInfo))
      case _ if cls == classOf[FileSkippingIndexing] =>
        val source: CaerusSourceLoad =
          deserializeCaerusPlan((jsonObject \ "source").asInstanceOf[JString].s).asInstanceOf[CaerusSourceLoad]
        val index: Int = (jsonObject \ "index").asInstanceOf[JInt].num.toInt
        FileSkippingIndexing(source, index, Some(sizeInfo))
      case _ if cls == classOf[Caching] =>
        val plan: CaerusPlan = deserializeCaerusPlan((jsonObject \ "plan").asInstanceOf[JString].s)
        Caching(plan, Some(sizeInfo))
      case _ =>
        throw new RuntimeException("Invalid class to deserialize %s.".format(cls))
    }
  }

  private[common] def serializeSizeInfo(sizeInfo: SizeInfo): String = {
    val jsonFields: List[(String,JValue)] =
      List(
        ("writeSize", JInt(sizeInfo.writeSize)),
        ("readSizeInfo", JString(sizeInfo.readSizeInfo.toJSON))
      )
    compact(render(JObject(jsonFields)))
  }

  private[common] def deserializeSizeInfo(json: String): SizeInfo = {
    val jsonAst = parse(json)

    assert(jsonAst.isInstanceOf[JObject])

    val jsonObject = jsonAst.asInstanceOf[JObject]
    val writeSize = (jsonObject \ "writeSize").asInstanceOf[JInt].num.toLong
    val readSizeInfo = ReadSizeInfo.fromJSON((jsonObject \ "readSizeInfo").asInstanceOf[JString].s)
    SizeInfo(writeSize, readSizeInfo)
  }

  private[common] def serializeReadSizeInfo(readInfo: ReadSizeInfo): String = {
    val jsonFields: List[(String,JValue)] = readInfo match {
      case basicReadInfo: BasicReadSizeInfo =>
        List(
          ("class", JString(basicReadInfo.getClass.getName)),
          ("readSize", JInt(basicReadInfo.readSize))
        )
      case _ =>
        throw new RuntimeException("Invalid ReadSizeInfo %s.".format(readInfo))
    }
    compact(render(JObject(jsonFields)))
  }

  private[common] def deserializeReadSizeInfo(json: String): ReadSizeInfo = {
    val jsonAst = parse(json)

    assert(jsonAst.isInstanceOf[JObject])

    val jsonObject = jsonAst.asInstanceOf[JObject]
    val cls = Class.forName((jsonObject \ "class").asInstanceOf[JString].s)

    cls match {
      case _ if cls == classOf[BasicReadSizeInfo] =>
        val readSize: Long = (jsonObject \ "readSize").asInstanceOf[JInt].num.toLong
        BasicReadSizeInfo(readSize)
      case _ =>
        throw new RuntimeException("Invalid class to deserialize %s.".format(cls))
    }
  }

  private def mirror: ru.Mirror = {
    ru.runtimeMirror(getClass.getClassLoader)
  }

  private def reconstruct[T](array: JArray): T = {
    assert(array.arr.forall(_.isInstanceOf[JObject]))

    val jsonNodes: Seq[JObject] = array.arr.map(_.asInstanceOf[JObject])
    val result = parseNextNode(jsonNodes)

    assert(result._1 == List.empty[JObject])
    result._2
  }

  private def parseNextNode[T](jsonNodes: Seq[JObject]): (Seq[JObject], T) = {
    val nextNode: JObject = jsonNodes.head
    var restNodes: Seq[JObject] = jsonNodes.tail
    val cls = Class.forName((nextNode \ "class").asInstanceOf[JString].s)

    assert (!cls.getName.endsWith("$"))

    if (cls == classOf[Literal]) {
      val numChildren: Int = (nextNode \ "num-children").asInstanceOf[JInt].num.toInt
      val dataType: DataType = DataType.fromJson(compact(render(nextNode \ "dataType")))
      val jValue: JValue = nextNode \ "value"

      assert(numChildren == 0)

      val value: Any = dataType match {
        case _ if jValue == JNull =>
          null
        case BooleanType =>
          jValue.asInstanceOf[JString].s.toBoolean
        case ByteType =>
          jValue.asInstanceOf[JString].s.toByte
        case ShortType =>
          jValue.asInstanceOf[JString].s.toShort
        case IntegerType =>
          jValue.asInstanceOf[JString].s.toInt
        case LongType =>
          jValue.asInstanceOf[JString].s.toLong
        case FloatType =>
          jValue.asInstanceOf[JString].s.toFloat
        case DoubleType =>
          jValue.asInstanceOf[JString].s.toDouble
        case _: DecimalType =>
          val dec = new Decimal()
          dec.set(BigDecimal(jValue.asInstanceOf[JString].s))
        case TimestampType =>
          Timestamp.valueOf(jValue.asInstanceOf[JString].s).getTime*1000L
        case DateType =>
          val dates: Seq[Int] = jValue.asInstanceOf[JString].s.split("-").map(_.toInt)
          assert(dates.length == 3)
          LocalDate.of(dates.head, dates(1), dates(2))
        case CalendarIntervalType =>
          var values = jValue.asInstanceOf[JString].s
          val times = Seq("years", "months", "days", "hours", "minutes").map(sep => {
            val newPair = parseCalendarValue(values, sep)
            values = newPair._2
            newPair._1
          })
          val microseconds: Long = {
            if (values.contains(" seconds ")) {
              val dec: Decimal = new Decimal
              dec.set(BigDecimal(values.split(" seconds ").head) * 1000000)
              dec.toLong
            } else {
              0L
            }
          }
          new CalendarInterval(
            (times.head*12 + times(1)).toInt,
            times(2).toInt,
            times(3)*MICROS_PER_HOUR + times(4)*MICROS_PER_MINUTE + microseconds)
        case BinaryType =>
          val hexString: String = jValue.asInstanceOf[JString].s
          assert(hexString.startsWith("0x"))
          BigInt(Integer.parseInt(hexString.substring(2), 16)).toByteArray
        case StringType => UTF8String.fromString(jValue.asInstanceOf[JString].s)
        case _ =>
          throw new RuntimeException("Cannot parse data type %s and jValue %s as Literal.".format(dataType, jValue))
      }
      (restNodes, Literal(value, dataType).asInstanceOf[T])
    } else {
      val numChildren = (nextNode \ "num-children").asInstanceOf[JInt].num.toInt
      val children:Seq[T] = (1 to numChildren).map(_ => {
        val childResult = parseNextNode[T](restNodes)
        restNodes = childResult._1
        childResult._2
      })
      val fields = getConstructorParameters(cls)
      val parameters: Array[AnyRef] = fields.map {
        case (fieldName, fieldType) =>
          parseFromJson[T](nextNode \ fieldName, fieldType, children)
      }.toArray
      val maybeCtor = cls.getConstructors.find { p =>
        val expectedTypes = p.getParameterTypes
        expectedTypes.length == fields.length && expectedTypes.zip(fields.map(_._2)).forall {
          case (cls, tpe) => cls == mirror.runtimeClass(erasure(tpe).dealias.typeSymbol.asClass)
        }
      }

      if (maybeCtor.isEmpty) {
        throw new RuntimeException("No valid constructor for %s.\n".format(cls.getName))
      } else {
        try {
          val treeNode: T = maybeCtor.get.newInstance(parameters: _*).asInstanceOf[T]
          (restNodes, treeNode)
        } catch {
          case _: java.lang.IllegalArgumentException =>
            throw new RuntimeException("Failed to construct tree node: %s\nctor:%s\ntypes:%s\nargs:%s\n"
              .format(
                cls.getName,
                maybeCtor.get,
                parameters.map(_.getClass).mkString(", "),
                parameters.mkString(", ").stripMargin))
        }
      }
    }
  }

  private def parseCalendarValue(str: String, sep: String): (Long, String) = {
    if (str.contains(sep)) {
      val values: Seq[String] = str.split(sep)
      val value: Long = values.head.toLong

      if (values.length == 1) {
        (value, "")
      } else {
        (value, values(1))
      }
    } else {
      (0L, str)
    }
  }

  private def parseFromJson[T](value: JValue, expectedType: ru.Type, children: Seq[T]): AnyRef = {
    if (value == JNull)
      return null

    expectedType match {
      case t if t <:< ru.typeOf[Boolean] =>
        value.asInstanceOf[JBool].value: java.lang.Boolean
      case t if t <:< ru.typeOf[Byte] =>
        value.asInstanceOf[JInt].num.toByte: java.lang.Byte
      case t if t <:< ru.typeOf[Short] =>
        value.asInstanceOf[JInt].num.toShort: java.lang.Short
      case t if t <:< ru.typeOf[Int] =>
        value.asInstanceOf[JInt].num.toInt: java.lang.Integer
      case t if t <:< ru.typeOf[Long] =>
        value.asInstanceOf[JInt].num.toLong: java.lang.Long
      case t if t <:< ru.typeOf[Float] =>
        value.asInstanceOf[JDouble].num.toFloat: java.lang.Float
      case t if t <:< ru.typeOf[Double] =>
        value.asInstanceOf[JDouble].num: java.lang.Double
      case t if t <:< ru.typeOf[BigInt] => value.asInstanceOf[JInt].num
      case t if t <:< ru.typeOf[java.lang.String] => value.asInstanceOf[JString].s
      case t if t <:< ru.typeOf[UUID] => UUID.fromString(value.asInstanceOf[JString].s)
      case t if t <:< ru.typeOf[DataType] => DataType.fromJson(compact(render(value)))
      case t if t <:< ru.typeOf[Metadata] => Metadata.fromJson(compact(render(value)))
      case t if t <:< ru.typeOf[StorageLevel] =>
        val JBool(useDisk) = value \ "useDisk"
        val JBool(useMemory) = value \ "useMemory"
        val JBool(useOffHeap) = value \ "useOffHeap"
        val JBool(deserialized) = value \ "deserialized"
        val JInt(replication) = value \ "replication"
        StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication.toInt)
      case t if t <:< ru.typeOf[TreeNode[_]] =>
        value match {
          case JInt(i) => children(i.toInt).asInstanceOf[AnyRef]
          case arr: JArray =>
            val cls = mirror.runtimeClass(erasure(t).dealias.typeSymbol.asClass)
            reconstruct[cls.type](arr)
          case _ => throw new RuntimeException("Json %s is not a valid for tree node.".format(value))
        }
      case t if t <:< ru.typeOf[Option[_]] =>
        if (value == JNothing) {
          None
        } else {
          val ru.TypeRef(_, _, Seq(optType)) = t
          Option(parseFromJson[T](value, optType, children))
        }
      case t if t <:< ru.typeOf[Seq[_]] =>
        val ru.TypeRef(_, _, Seq(elementType)) = t
        value match {
          case JString(elements) =>
            assert(elementType <:< ru.typeOf[String])
            elements.substring(1, elements.length-1).split(", ").toSeq
          case JArray(elements) =>
            elements.map(parseFromJson[T](_, elementType, children))
          case _ => throw new RuntimeException("Cannot parse %s for type Seq[%s]".format(value, elementType))
        }
      case t if t <:< ru.typeOf[Map[_, _]] =>
        val ru.TypeRef(_, _, Seq(_, valueType)) = t
        val JObject(fields) = value
        fields.map {
          case (name, value) => name -> parseFromJson[T](value, valueType, children)
        }.toMap
      case _ if isScalaObject(value) =>
        val JString(clsName) = value \ "object"
        val cls = Class.forName(clsName)
        cls.getField("MODULE$").get(cls)
      case t if t <:< ru.typeOf[Product] =>
        val fields = getConstructorParameters(t)
        val clsName = getClassNameFromType(t)
        parseToProduct[T](clsName, fields, value, children)
      case _ if isScalaProduct(value) =>
        val JString(clsName) = value \ "product-class"
        val fields = getConstructorParameters(Class.forName(clsName))
        parseToProduct[T](clsName, fields, value, children)
      case _ => throw new RuntimeException("Do not support type %s with json %s.".format(expectedType, value))
    }
  }

  private def parseToProduct[T](
      clsName: String,
      fields: Seq[(String, ru.Type)],
      value: JValue,
      children: Seq[T]): AnyRef = {
    val parameters: Array[AnyRef] = fields.map {
      case (fieldName, fieldType) =>
        parseFromJson[T](value \ fieldName, fieldType, children)
    }.toArray
    val ctor = Class.forName(clsName).getConstructors.maxBy(_.getParameterTypes.length)
    ctor.newInstance(parameters: _*).asInstanceOf[AnyRef]
  }

  private def isScalaObject(jValue: JValue): Boolean = jValue \ "object" match {
    case JString(str) if str.endsWith("$") => true
    case _ => false
  }

  private def isScalaProduct(jValue: JValue): Boolean = jValue \ "product-class" match {
    case _: JString => true
    case _ => false
  }

  private def getConstructorParameters(cls: Class[_]): Seq[(String, ru.Type)] = {
    val classSymbol = mirror.staticClass(cls.getName)
    val t = classSymbol.selfType
    getConstructorParameters(t)
  }

  def getConstructorParameters(tpe: ru.Type): Seq[(String, ru.Type)] = {
    val dealiasedTpe = tpe.dealias
    val formalTypeArgs = dealiasedTpe.typeSymbol.asClass.typeParams
    val ru.TypeRef(_, _, actualTypeArgs) = dealiasedTpe
    val params = constructParams(dealiasedTpe)

    if (actualTypeArgs.nonEmpty) {
      params.map { p =>
        (p.name.decodedName.toString, p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs))
      }
    } else {
      params.map { p =>
        p.name.decodedName.toString -> p.typeSignature
      }
    }
  }

  protected def constructParams(tpe: ru.Type): Seq[ru.Symbol] = {
    val constructorSymbol: ru.Symbol = tpe.member(ru.termNames.CONSTRUCTOR) match {
      case ru.NoSymbol => getCompanionConstructor(tpe)
      case sym => sym
    }
    val params = if (constructorSymbol.isMethod) {
      constructorSymbol.asMethod.paramLists
    } else {
      // Find the primary constructor, and use its parameter ordering.
      val primaryConstructorSymbol: Option[ru.Symbol] = constructorSymbol.asTerm.alternatives.find(
        s => s.isMethod && s.asMethod.isPrimaryConstructor)
      if (primaryConstructorSymbol.isEmpty) {
        throw new RuntimeException("Internal SQL error: Product object did not have a primary constructor.")
      } else {
        primaryConstructorSymbol.get.asMethod.paramLists
      }
    }
    params.flatten
  }

  private def getCompanionConstructor(tpe: ru.Type): ru.Symbol = {
    def throwUnsupportedOperation: Nothing = {
      throw new UnsupportedOperationException("Unable to find constructor for %s. This could happen if ".format(tpe) +
        "%s is an interface, or a trait without companion object constructor.".format(tpe))
    }

    tpe.typeSymbol.asClass.companion match {
      case ru.NoSymbol => throwUnsupportedOperation
      case sym => sym.asTerm.typeSignature.member(ru.TermName("apply")) match {
        case ru.NoSymbol => throwUnsupportedOperation
        case constructorSym => constructorSym
      }
    }
  }

  private def erasure(tpe: ru.Type): ru.Type = {
    if ((tpe <:< ru.typeOf[AnyVal]) && !tpe.toString.startsWith("scala")) {
      tpe
    } else {
      tpe.erasure
    }
  }

  private def getClassNameFromType(tpe: ru.`Type`): String = {
    erasure(tpe).dealias.typeSymbol.asClass.fullName
  }
}