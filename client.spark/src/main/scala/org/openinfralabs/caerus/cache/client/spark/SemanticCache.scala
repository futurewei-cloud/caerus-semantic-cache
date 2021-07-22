package org.openinfralabs.caerus.cache.client.spark

import io.grpc.{Channel, ManagedChannelBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.openinfralabs.caerus.cache.client.{BasicCandidateSelector, BasicSizeEstimator, CandidateSelector, SizeEstimator}
import org.openinfralabs.caerus.cache.common.Mode.Mode
import org.openinfralabs.caerus.cache.common.Tier.Tier
import org.openinfralabs.caerus.cache.common._
import org.openinfralabs.caerus.cache.common.plans.{SourceInfo, _}
import org.openinfralabs.caerus.cache.grpc.service._
import org.slf4j.{Logger, LoggerFactory}

/**
 * Client module used with Scala Spark Client that allows interaction with a Semantic Cache service.
 *
 * @since 0.0.0
 */
class SemanticCache(
  spark: SparkSession,
  serverAddress: String,
) extends SparkListener {
  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val ipPair: (String, Int) = getIPFromString(serverAddress)
  private val channel: Channel = ManagedChannelBuilder.forAddress(ipPair._1, ipPair._2).usePlaintext().build()
  private val clientId: String = spark.sparkContext.applicationId
  private var applyOptimization: Boolean = true
  private var skip: Boolean = false
  private val sizeEstimator: SizeEstimator = BasicSizeEstimator()
  private val candidateSelector: CandidateSelector = BasicCandidateSelector()
  private var fullOptimizeTime: Long = 0
  private var serializeDeserializeTime: Long = 0
  private var transformTime: Long = 0
  private var writeTime: Long = 0

  // Activate optimization for spark session.
  logger.info("Activate Semantic Cache optimization.")
  private val registerReply: RegisterReply = try {
    val registerRequest: RegisterRequest = RegisterRequest(clientId)
    val stub = SemanticCacheServiceGrpc.blockingStub(channel)
    stub.register(registerRequest)
  } catch {
    case e: Exception =>
      logger.warn("Failed to register Semantic Cache Client with the following message: %s".format(e.getMessage))
      throw new RuntimeException("Semantic Cache Client was unable to register to Semantic Cache Manager.")
  }
  private val mode: Mode = {
    logger.info("Mode %s activated.".format(registerReply.mode))
    Mode(registerReply.mode)
  }
  private val timeout: Long = registerReply.terminationTimeout/3
  logger.info("Start heartbeats every %s ms".format(timeout))
  private val heartbeatSender: HeartbeatSender = new HeartbeatSender(clientId, timeout, channel)
  private val heartbeatThread: Thread = new Thread(heartbeatSender)
  heartbeatThread.start()
  spark.sparkContext.addSparkListener(listener=this)
  spark.experimental.extraOptimizations ++= Seq(Optimization)

  private def getIPFromString(address: String): (String, Int) = {
    val pair = address.split(":")
    if (pair.length != 2)
      throw new RuntimeException("The serverAddress should be of the form <host>:<port>")
    val host: String = pair(0)
    val port: Int = {
      if (pair(1) forall Character.isDigit) {
        pair(1).toInt
      } else {
        throw new RuntimeException("The serverAddress should contain a number after char `:`.")
      }
    }
    (host, port)
  }

  private def transform(plan: LogicalPlan): CaerusPlan = SemanticCache.transform(plan)

  private def pullUpUnion(plan: LogicalPlan): LogicalPlan = {
    val optimizedPlan: LogicalPlan = plan.withNewChildren(plan.children.map(pullUpUnion))
    optimizedPlan match {
      case filterPlan: Filter if filterPlan.child.isInstanceOf[Union] =>
        new Union(filterPlan.child.children.map(grandchild => filterPlan.clone().withNewChildren(Seq(grandchild))))
      case projectPlan: Project if projectPlan.child.isInstanceOf[Union] =>
        new Union(projectPlan.child.children.map(grandchild => projectPlan.clone().withNewChildren(Seq(grandchild))))
      case _ => optimizedPlan
    }
  }

  private def replaceNames(condition: Expression, index: Int): Expression = {
    condition match {
      case attribute: AttributeReference if attribute.exprId.id.toInt == 0 =>
        AttributeReference("path", attribute.dataType)()
      case attribute: AttributeReference if attribute.exprId.id.toInt == 1 =>
        AttributeReference("min_" + index.toString, attribute.dataType)()
      case attribute: AttributeReference if attribute.exprId.id.toInt == 2 =>
        AttributeReference("max_" + index.toString, attribute.dataType)()
      case _ =>
        condition.withNewChildren(condition.children.map(replaceNames(_, index)))
    }
  }

  private def getSchemaForLoadWithIndices(dataType: DataType, index: Int): StructType = {
    StructType(Array(
      StructField("path", StringType),
      StructField("min_" + index.toString, dataType),
      StructField("max_" + index.toString, dataType)
    ))
  }

  private def transformExpressionBack(expr: Expression, output: Seq[Attribute]): Expression = {
    logger.debug("Expression: %s --- Output: %s --- Class: %s".format(expr, output, expr.getClass.getName))
    expr match {
      case caerusAttrib: CaerusAttribute => output(SemanticCache.getIndex(caerusAttrib))
      case _ => expr.withNewChildren(expr.children.map(transformExpressionBack(_, output)))
    }
  }

  private def transformBack(
      outputCaerusPlan: CaerusPlan,
      inputPlan: LogicalPlan,
      inputCaerusPlan: CaerusPlan): LogicalPlan = {
    outputCaerusPlan match {
      case caerusSourceLoad: CaerusSourceLoad =>
        assert(inputPlan.isInstanceOf[LogicalRelation])
        assert(inputCaerusPlan.isInstanceOf[CaerusSourceLoad])
        val logicalRelation = inputPlan.asInstanceOf[LogicalRelation]
        if (caerusSourceLoad.sameResult(inputCaerusPlan))
          return logicalRelation
        assert(logicalRelation.relation.isInstanceOf[HadoopFsRelation])
        val hadoopFsRelation = logicalRelation.relation.asInstanceOf[HadoopFsRelation]
        val tempPlan: LogicalRelation = spark.read
          .format(caerusSourceLoad.format)
          .options(hadoopFsRelation.options)
          .schema(hadoopFsRelation.dataSchema)
          .load(caerusSourceLoad.sources.map(source => source.path):_*)
          .queryExecution
          .analyzed
          .asInstanceOf[LogicalRelation]
        new LogicalRelation(
          tempPlan.relation,
          inputPlan.output.asInstanceOf[Seq[AttributeReference]],
          tempPlan.catalogTable,
          tempPlan.isStreaming)
      case caerusCacheLoad: CaerusCacheLoad =>
        logger.info("Cache Load: %s".format(caerusCacheLoad.sources.mkString("[", ",", "]")))
        applyOptimization = false
        val tempPlan: LogicalRelation =
          spark.read
            .format(caerusCacheLoad.format)
            .load(caerusCacheLoad.sources:_*)
            .queryExecution
            .analyzed
            .asInstanceOf[LogicalRelation]
        applyOptimization = true
        new LogicalRelation(
          tempPlan.relation,
          inputPlan.output.asInstanceOf[Seq[AttributeReference]],
          tempPlan.catalogTable,
          tempPlan.isStreaming
        )
      case caerusUnion: CaerusUnion =>
        new Union(caerusUnion.children.map(child => transformBack(child, inputPlan, inputCaerusPlan)))
      case caerusLoadWithIndices: CaerusLoadWithIndices =>
        applyOptimization = false
        assert(inputPlan.isInstanceOf[LogicalRelation])
        val logicalRelation: LogicalRelation = inputPlan.asInstanceOf[LogicalRelation]
        assert(inputCaerusPlan.isInstanceOf[CaerusSourceLoad])
        val sourceLoad: CaerusSourceLoad = inputCaerusPlan.asInstanceOf[CaerusSourceLoad]
        val indexLoad: CaerusPlan = caerusLoadWithIndices.child
        logger.info("Load indices caerus plan:\n%s".format(indexLoad))
        assert(indexLoad.isInstanceOf[Project])
        val project: Project = indexLoad.asInstanceOf[Project]
        assert(project.child.isInstanceOf[Filter])
        val filter: Filter = project.child.asInstanceOf[Filter]
        assert(filter.child.isInstanceOf[CaerusCacheLoad])
        val cacheLoad = filter.child.asInstanceOf[CaerusCacheLoad]
        val index: Int = caerusLoadWithIndices.index
        val filterSql: String = replaceNames(filter.condition, index).sql
        logger.info("Filter sql: %s".format(filterSql))
        val filteredPath: Seq[String] =
          spark.read
            .format(cacheLoad.format)
            .schema(getSchemaForLoadWithIndices(caerusLoadWithIndices.output(index).dataType, index))
            .load(cacheLoad.sources:_*)
            .filter(filterSql)
            .select("path")
            .collect()
            .map(row => row(0))
            .toSeq
            .asInstanceOf[Seq[String]]
        logger.info("Filtered path: %s".format(filteredPath.mkString("[", ",", "]")))
        applyOptimization = true
        val newPath: Seq[String] = caerusLoadWithIndices.path.intersect(filteredPath)
        logger.info("New path: %s".format(newPath.mkString("[", ",", "]")))
        val newSourceLoad: CaerusSourceLoad = CaerusSourceLoad(
          sourceLoad.output,
          sourceLoad.sources.filter(source => newPath.contains(source.path)),
          sourceLoad.format
        )
        logger.info("New CaerusSourceLoad:\n%s".format(newSourceLoad))
        transformBack(newSourceLoad, logicalRelation, sourceLoad)
      case CaerusIf(children) =>
        val conditionPlan: LogicalPlan = transformBack(children.head, inputPlan, inputCaerusPlan)
        conditionPlan match {
          case CaerusTrue() => transformBack(children(1), inputPlan, inputCaerusPlan)
          case CaerusFalse() => transformBack(children(2), inputPlan, inputCaerusPlan)
        }
      case CaerusRepartitioning(name, child, index) =>
        applyOptimization = false
        assert(inputPlan.isInstanceOf[LogicalRelation])
        val logicalRelation: LogicalRelation = inputPlan.asInstanceOf[LogicalRelation]
        assert(logicalRelation.relation.isInstanceOf[HadoopFsRelation])
        val hadoopFsRelation = logicalRelation.relation.asInstanceOf[HadoopFsRelation]
        val loadDF: DataFrame = spark.read
          .format(child.format)
          .options(hadoopFsRelation.options)
          .schema(hadoopFsRelation.dataSchema)
          .load(child.sources.map(source => source.path):_*)
        applyOptimization = false
        val bytesWritten: Long = startRepartitioning(
          loadDF,
          loadDF.columns(index),
          Tier.STORAGE_DISK,
          name.split(Path.SEPARATOR).last
        )
        if (bytesWritten >= 0) {
          CaerusTrue()
        } else {
          CaerusFalse()
        }
      case CaerusFileSkippingIndexing(name, child, index) =>
        assert(inputPlan.isInstanceOf[LogicalRelation])
        val logicalRelation: LogicalRelation = inputPlan.asInstanceOf[LogicalRelation]
        assert(logicalRelation.relation.isInstanceOf[HadoopFsRelation])
        val hadoopFsRelation = logicalRelation.relation.asInstanceOf[HadoopFsRelation]
        val loadDF: DataFrame = spark.read
          .format(child.format)
          .options(hadoopFsRelation.options)
          .schema(hadoopFsRelation.dataSchema)
          .load(child.sources.map(source => source.path):_*)
        val bytesWritten: Long = startFileSkippingIndexing(
          loadDF,
          loadDF.columns(index),
          Tier.STORAGE_DISK,
          name.split(Path.SEPARATOR).last
        )
        if (bytesWritten >= 0) {
          CaerusTrue()
        } else {
          CaerusFalse()
        }
      case CaerusCaching(name, child) =>
        val bytesWritten: Long = startCacheIntermediateData(child, Tier.STORAGE_DISK, name.split(Path.SEPARATOR).last)
        if (bytesWritten >= 0) {
          CaerusTrue()
        } else {
          CaerusFalse()
        }
      case Project(projectList, child) =>
        val newChild: LogicalPlan = inputPlan match {
          case project: Project => transformBack(child, project.child, inputCaerusPlan.asInstanceOf[Project].child)
          case _ => transformBack(child, inputPlan, inputCaerusPlan)
        }
        val newProjectList: Seq[NamedExpression] =
          projectList.map(transformExpressionBack(_, newChild.output).asInstanceOf[NamedExpression])
        Project(newProjectList, newChild)
      case Filter(condition, child) =>
        val newChild: LogicalPlan = inputPlan match {
            case filter: Filter => transformBack(child, filter.child, inputCaerusPlan.asInstanceOf[Filter].child)
            case _ => transformBack(child, inputPlan, inputCaerusPlan)
        }
        val newCondition: Expression = transformExpressionBack(condition, newChild.output)
        Filter(newCondition, newChild)
      case _ =>
        assert(inputPlan.children.size == outputCaerusPlan.children.size)
        assert(inputPlan.children.size == inputCaerusPlan.children.size)
        inputPlan.withNewChildren(inputPlan.children.indices.map(i =>
          transformBack(outputCaerusPlan.children(i), inputPlan.children(i), inputCaerusPlan.children(i))))
    }
  }

  private def checkAccessNode(plan: LogicalPlan): Boolean = {
    plan match {
      case _: Aggregate | _: Join | _: Project | _: Filter | LogicalRelation(_: HadoopFsRelation, _, _, _) => true
      case _ => false
    }
  }

  private def optimize(inputPlan: LogicalPlan): LogicalPlan = {
    val supportTree: Support[Boolean] = SemanticCache.checkSupport(inputPlan)
    logger.info("Support: %s\n".format(supportTree))
    optimize(inputPlan, supportTree)
  }

  private def optimize(inputPlan: LogicalPlan, supportTree: Support[Boolean]): LogicalPlan = {
    // If LogicalPlan is supported for Semantic Cache optimization then optimize it using the Semantic Cache. Otherwise,
    // try to optimize potential sub-plans (children).
    if (supportTree.support && checkAccessNode(inputPlan)) {
      // Transform native Spark's LogicalPlan to CaerusPlan and serialize CaerusPlan.
      logger.info("Initial Logical Plan:\n%s".format(inputPlan))
      val beginTime1: Long = System.nanoTime()
      val inputCaerusPlan: CaerusPlan = transform(inputPlan)
      val endTime1: Long = System.nanoTime()
      transformTime += endTime1 - beginTime1
      logger.info("Initial Caerus Plan:\n%s".format(inputCaerusPlan))
      val beginTime2: Long = System.nanoTime()
      val inputSerializedCaerusPlan: String = inputCaerusPlan.toJSON
      logger.info("Initial Serialized Caerus Plan: %s\n".format(inputSerializedCaerusPlan))
      val candidatePairs: Seq[(LogicalPlan, Candidate)] = {
        if (mode == Mode.FULLY_AUTOMATIC)
          candidateSelector.getCandidates(inputPlan, inputCaerusPlan)
        else
          Seq.empty[(LogicalPlan, Candidate)]
      }
      candidatePairs.foreach(candidatePair => sizeEstimator.estimateSize(candidatePair._1, candidatePair._2))
      val jsonCandidates: Seq[String] = candidatePairs.map(_._2.toJSON)
      val endTime2: Long = System.nanoTime()
      serializeDeserializeTime += endTime2 - beginTime2
      logger.info("JSON Candidates:\n%s".format(jsonCandidates.mkString("\n")))

      // Make optimization request to Semantic Cache Manager.
      val outputSerializedCaerusPlan: String = try {
        val optRequest: OptimizationRequest = OptimizationRequest(clientId, inputSerializedCaerusPlan, jsonCandidates)
        val optStub = SemanticCacheServiceGrpc.blockingStub(channel)
        val optReply: OptimizationReply = optStub.optimize(optRequest)
        optReply.optimizedCaerusPlan
      } catch {
        case e: Exception =>
          logger.warn("Optimization request failed with this message: %s".format(e))
          inputSerializedCaerusPlan
      }

      // Deserialize CaerusPlan and transform it back to native Spark's LogicalPlan.
      logger.info("Output Serialized Caerus Plan: %s\n".format(outputSerializedCaerusPlan))
      val beginTime3: Long = System.nanoTime()
      val outputCaerusPlan: CaerusPlan = CaerusPlan.fromJSON(outputSerializedCaerusPlan)
      val endTime3: Long = System.nanoTime()
      serializeDeserializeTime += endTime3 - beginTime3
      logger.info("Output Caerus Plan:\n%s".format(outputCaerusPlan))
      val beginTime4: Long = System.nanoTime()
      val outputPlan: LogicalPlan = pullUpUnion(transformBack(outputCaerusPlan, inputPlan, inputCaerusPlan))
      val endTime4: Long = System.nanoTime()
      transformTime += endTime4 - beginTime4
      logger.info("Output Logical Plan:\n%s".format(outputPlan))
      outputPlan
    } else {
      inputPlan.withNewChildren(
        inputPlan.children.indices.map(i => optimize(inputPlan.children(i), supportTree.children(i)))
      )
    }
  }

  /**
   * Terminate heartbeat sender when application ends.
   */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info("Breakdown: %s,%s,%s,%s".format(fullOptimizeTime, serializeDeserializeTime, transformTime, writeTime))
    heartbeatThread.interrupt()
  }

  private[SemanticCache] def startRepartitioning(
    loadDF: DataFrame,
    partitionAttribute: String,
    tier: Tier,
    name: String
  ): Long = {
    val beginTime: Long = System.nanoTime()

    // Make runtime checks, to ensure support.
    val logicalPlan: LogicalPlan = loadDF.queryExecution.logical
    if (!logicalPlan.isInstanceOf[LogicalRelation]) {
      logger.warn("Plan provided in repartitioning does not correspond to a logical relation. Plan:\n%s"
        .format(logicalPlan))
      return 0L
    }
    val logicalRelation: LogicalRelation = logicalPlan.asInstanceOf[LogicalRelation]
    if (!logicalRelation.relation.isInstanceOf[HadoopFsRelation]) {
      logger.warn("Plan provided in repartitioning does not correspond to a Hadoop FS relation. Base Relation:\n%s"
        .format(logicalRelation.relation))
      return 0L
    }
    val index: Int = loadDF.columns.indexOf(partitionAttribute)
    if (index == -1) {
      logger.warn("Attribute %s cannot be found in plan:\n%s".format(partitionAttribute, logicalPlan))
      return 0L
    }

    // Transform to CaerusPlan.
    val caerusPlan: CaerusPlan = transform(logicalPlan)
    assert(caerusPlan.isInstanceOf[CaerusSourceLoad])
    val caerusSourceLoad: CaerusSourceLoad = caerusPlan.asInstanceOf[CaerusSourceLoad]

    // Find related information (metadata) about load.
    val numPartitions: Int = loadDF.rdd.getNumPartitions

    // Make reservation request.
    val candidate: Repartitioning = Repartitioning(caerusSourceLoad, index)
    sizeEstimator.estimateSize(logicalPlan, candidate)
    val path: String = try {
      val stub = SemanticCacheServiceGrpc.blockingStub(channel)
      val request: ReservationRequest = ReservationRequest(clientId, candidate.toJSON, tier.id, name)
      stub.reserve(request).path
    } catch {
      case e: Exception =>
        logger.warn("Reservation failed: %s".format(e.getMessage))
        return 0L
    }

    // Repartition data.
    applyOptimization = false
    val triggeredException: Boolean = try {
      loadDF.repartitionByRange(numPartitions, loadDF.col(partitionAttribute)).write.parquet(path)
      false
    } catch {
      case e: Exception =>
        logger.warn("Cannot write candidate %s. Exception message: %s".format(candidate, e.getMessage))
        true
    } finally {
      applyOptimization = true
    }
    if (triggeredException) {
        try {
          val stub = SemanticCacheServiceGrpc.blockingStub(channel)
          val request: CommitReservationRequest = CommitReservationRequest(clientId, commit=false, name)
          stub.commitReservation(request)
        } catch {
          case e: Exception =>
            logger.warn("Cancel reservation failed: %s".format(e.getMessage))
        }
        return -1L
    }

    // Calculate actual size.
    val outputDF: DataFrame = spark.read.parquet(path)
    val outputSize: Long = outputDF.queryExecution.logical.stats.sizeInBytes.toLong

    // Commit candidate to become available for reading and exit.
    try {
      val stub = SemanticCacheServiceGrpc.blockingStub(channel)
      val request: CommitReservationRequest = CommitReservationRequest(clientId, commit=true, name, outputSize)
      stub.commitReservation(request)
    } catch {
      case e: Exception =>
        logger.warn("Commit reservation failed: %s".format(e.getMessage))
        return 0L
    }
    val endTime: Long = System.nanoTime()
    writeTime += endTime - beginTime
    outputSize
  }

  /**
   * Repartition and store given dataset according to the provided parameters.
   *
   * This method works only for single partition attributes and it produces only range re-partitioning. The data is
   * additionally sorted, not only partitioned by the given attribute.
   *
   * @param loadDF Source dataframe used for repartitioning.
   * @param partitionAttribute Name of the primary partition attribute.
   * @param name Name used to find the repartitioned dataset, if operation is a success.
   * @param tier [[Tier]] to which to store the repartitioned dataset.
   * @return Returns the size of the repartitioned data if operation is successful. Otherwise, it prints a warning
   *         failure message in the log, explaining why repartitioning failed and returns 0.
   * @since 0.0.0
   */
  def repartitioning(loadDF: DataFrame, partitionAttribute: String, tier: Tier, name: String): Long = {
    // See if mode allows this operation.
    if (mode != Mode.MANUAL_WRITE) {
      logger.warn("Repartitioning not allowed for mode %s. Ignoring request.".format(mode))
      return 0L
    }
    startRepartitioning(loadDF, partitionAttribute, tier, name)
  }

  private[SemanticCache] def startFileSkippingIndexing(
    loadDF: DataFrame,
    indexedAttribute: String,
    tier: Tier,
    name: String
  ): Long = {
    val beginTime: Long = System.nanoTime()

    // Transform load DataFrame to CaerusLoad.
    val logicalPlan: LogicalPlan = loadDF.queryExecution.logical
    if (!logicalPlan.isInstanceOf[LogicalRelation]) {
      logger.warn("Plan provided in repartition does not correspond to a logical relation. Plan:\n%s"
        .format(logicalPlan))
      return 0L
    }
    val logicalRelation: LogicalRelation = logicalPlan.asInstanceOf[LogicalRelation]
    if (!logicalRelation.relation.isInstanceOf[HadoopFsRelation]) {
      logger.warn("Plan provided in repartition does not correspond to a Hadoop FS relation. Base Relation:\n%s"
        .format(logicalRelation.relation))
      return 0L
    }
    val hadoopFsRelation: HadoopFsRelation = logicalRelation.relation.asInstanceOf[HadoopFsRelation]
    val caerusPlan: CaerusPlan = transform(logicalPlan)
    assert(caerusPlan.isInstanceOf[CaerusSourceLoad])
    val caerusSourceLoad: CaerusSourceLoad = caerusPlan.asInstanceOf[CaerusSourceLoad]

    // Find index of file-skipping attribute.
    val index: Int = loadDF.columns.indexOf(indexedAttribute)
    if (index == -1) {
      logger.warn("Attribute %s cannot be found in plan:\n%s".format(indexedAttribute, logicalPlan))
      return 0L
    }

    // See if file format is supported.
    if (caerusSourceLoad.format != "csv") {
      logger.warn("Format %s is not supported.".format(caerusSourceLoad))
    }

    // Make reservation request.
    val candidate: FileSkippingIndexing = FileSkippingIndexing(caerusSourceLoad, index)
    sizeEstimator.estimateSize(logicalPlan, candidate)
    val path: String = try {
      val stub = SemanticCacheServiceGrpc.blockingStub(channel)
      val request: ReservationRequest = ReservationRequest(clientId, candidate.toJSON, tier.id, name)
      stub.reserve(request).path
    } catch {
      case e: Exception =>
        logger.warn("Reservation failed: %s".format(e.getMessage))
        return 0L
    }

    // Construct indices.
    applyOptimization = false
    val triggeredException: Boolean = try {
      val fileDFs: Seq[DataFrame] = caerusSourceLoad.sources.map(source =>
        spark.read
          .format(caerusSourceLoad.format)
          .options(hadoopFsRelation.options)
          .schema(hadoopFsRelation.dataSchema)
          .load(source.path)
          .agg(functions.min(indexedAttribute), functions.max(indexedAttribute))
          .withColumnRenamed(existingName="min("+indexedAttribute+")",newName="min_"+index)
          .withColumnRenamed(existingName="max("+indexedAttribute+")",newName="max_"+index)
          .withColumn(colName="path",lit(source.path))
          .select("path", "min_" + index, "max_" + index)
      )
      fileDFs.reduce((df1,df2) => df1.union(df2)).coalesce(numPartitions=1).write.json(path)
      false
    } catch {
      case e: Exception =>
        logger.warn("Cannot write candidate %s. Exception message: %s".format(candidate, e.getMessage))
        true
    } finally {
      applyOptimization = true
    }
    if (triggeredException) {
      try {
        val stub = SemanticCacheServiceGrpc.blockingStub(channel)
        val request: CommitReservationRequest = CommitReservationRequest(clientId, commit=false, name)
        stub.commitReservation(request)
      } catch {
        case e: Exception =>
          logger.warn("Cancel reservation failed: %s".format(e.getMessage))
      }
      return 0L
    }

    // Calculate actual size.
    val outputDF: DataFrame =
      spark.read
        .schema(getSchemaForLoadWithIndices(caerusSourceLoad.output(index).dataType, index))
        .json(path)
    val outputSize: Long = outputDF.queryExecution.logical.stats.sizeInBytes.toLong

    // Commit candidate to become available for reading and exit.
    try {
      val stub = SemanticCacheServiceGrpc.blockingStub(channel)
      val request: CommitReservationRequest = CommitReservationRequest(clientId, commit=true, name, outputSize)
      stub.commitReservation(request)
    } catch {
      case e: Exception =>
        logger.warn("Commit reservation failed: %s".format(e.getMessage))
        return 0L
    }
    val endTime: Long = System.nanoTime()
    writeTime += endTime - beginTime
    outputSize
  }

  /**
   * Create file-skipping indices from the given dataset and store them, according to the provided parameters.
   *
   * This method produces only min/max file-skipping indices for a single attribute.
   *
   * @param loadDF Source dataframe used for producing file-skipping indices.
   * @param indexedAttribute Attribute for which file-skipping indices is constructed.
   * @param tier [[Tier]] to which to store the file-skipping indices.
   * @param name Name used to find the file-skipping indices, if operation is a success.
   * @return Returns the size of the resulting file-skipping indices if operation is successful. Otherwise, it prints a
   *         warning failure message in the log, explaining why the operation failed and returns 0.
   * @since 0.0.0
   */
  def fileSkippingIndexing(loadDF: DataFrame, indexedAttribute: String, tier: Tier, name: String): Long = {
    // See if mode allows this operation.
    if (mode != Mode.MANUAL_WRITE) {
      logger.warn("Creating files-skipping indices not allowed for mode %s. Ignoring request.".format(mode))
      return 0L
    }
    startFileSkippingIndexing(loadDF, indexedAttribute, tier, name)
  }

  private[SemanticCache] def startCacheIntermediateData(logicalPlan: LogicalPlan, tier: Tier, name: String): Long = {
    val beginTime: Long = System.nanoTime()

    // Transform logical plan to CaerusPlan.
    val caerusPlan = transform(logicalPlan)
    val candidate: Caching = Caching(caerusPlan)
    sizeEstimator.estimateSize(logicalPlan, candidate)
    logger.debug("JSON Candidate:\n%s".format(candidate.toJSON))
    val path: String = try {
      val stub = SemanticCacheServiceGrpc.blockingStub(channel)
      val request: ReservationRequest = ReservationRequest(clientId, candidate.toJSON, tier.id, name)
      stub.reserve(request).path
    } catch {
      case e: Exception =>
        logger.warn("Reservation failed: %s".format(e.getMessage))
        return 0L
    }

    // Cache intermediate data.
    if (mode == Mode.FULLY_AUTOMATIC)
      applyOptimization = false
    val triggeredException: Boolean = try {
      val writePlan: LogicalPlan = InsertIntoHadoopFsRelationCommand(
        outputPath = new Path(path),
        staticPartitions = Map.empty,
        ifPartitionNotExists = false,
        partitionColumns = Seq.empty[Attribute],
        bucketSpec = None,
        fileFormat = new ParquetFileFormat(),
        options = Map.empty[String,String],
        query = logicalPlan,
        mode = SaveMode.Overwrite,
        catalogTable = None,
        fileIndex = None,
        outputColumnNames = logicalPlan.output.map(_.name))
      val queryExecution: QueryExecution = spark.sessionState.executePlan(writePlan)
      queryExecution.toRdd
      false
    } catch {
      case e: Exception =>
        logger.warn("Cannot write candidate %s. Exception message:\n%s\n".format(candidate, e.getMessage))
        true
    } finally {
      applyOptimization = false
    }
    if (triggeredException) {
      try {
        val stub = SemanticCacheServiceGrpc.blockingStub(channel)
        val request: CommitReservationRequest = CommitReservationRequest(clientId, commit=false, name)
        stub.commitReservation(request)
      } catch {
        case e: Exception =>
          logger.warn("Cancel reservation failed: %s".format(e.getMessage))
      }
      return 0L
    }

    // Calculate actual size.
    val outputDF: DataFrame = spark.read.parquet(path)
    val outputSize: Long = outputDF.queryExecution.logical.stats.sizeInBytes.toLong

    // Commit candidate to become available for reading and exit.
    try {
      val stub = SemanticCacheServiceGrpc.blockingStub(channel)
      val request: CommitReservationRequest = CommitReservationRequest(clientId, commit=true, name, outputSize)
      stub.commitReservation(request)
    } catch {
      case e: Exception =>
        logger.warn("Commit reservation failed: %s".format(e.getMessage))
        return 0L
    }
    val endTime: Long = System.nanoTime()
    writeTime += endTime - beginTime
    outputSize
  }

  /**
   * Create Intermediate Data from the given dataset and store it, according to the provided parameters.
   *
   * @param intermediateDF Intermediate dataframe used for storing.
   * @param tier [[Tier]] to which to store the intermediate data.
   * @param name Name used to find the intermediate data, if operation is a success.
   * @return Returns the size of the resulting data if operation is successful. Otherwise, it prints a
   *         warning failure message in the log, explaining why the operation failed and returns 0.
   * @since 0.0.0
   */
  def cacheIntermediateData(intermediateDF: DataFrame, tier: Tier, name: String): Long = {
    // See if mode allows this operation.
    if (mode != Mode.MANUAL_WRITE) {
      logger.warn("Caching intermediate data not allowed for mode %s. Ignoring request.".format(mode))
      return 0L
    }

    // Get logical plan from intermediate DF.
    applyOptimization = false
    val logicalPlan: LogicalPlan = intermediateDF.queryExecution.optimizedPlan
    applyOptimization = true
    if (!SemanticCache.checkSupport(logicalPlan).support || !checkAccessNode(logicalPlan)) {
      logger.warn("The following plan is not supported for caching:\n%s".format(logicalPlan))
      return 0L
    }
    startCacheIntermediateData(logicalPlan, tier, name)
  }

  /**
   * Delete cached content.
   *
   * @param name Unique name of candidate to delete.
   *
   * @return Returns the size of the extra available data if operation is successful. Otherwise, it prints a
   *         warning failure message in the log, explaining why the operation failed and returns 0.
   *
   * @since 0.0.0
   */
  def delete(name: String): Long = {
    val beginTime: Long = System.nanoTime()

    // See if mode allows this operation.
    if (mode != Mode.MANUAL_WRITE) {
      logger.warn("Delete not allowed for mode %s. Ignoring request.".format(mode))
      return 0L
    }

    // Send delete request and get path to delete.
    val deleteReply: DeleteReply = try {
      val stub = SemanticCacheServiceGrpc.blockingStub(channel)
      val request: DeleteRequest = DeleteRequest(clientId, name)
      stub.delete(request)
    } catch {
      case e: Exception =>
        logger.warn("Delete failed: %s".format(e.getMessage))
        return 0L
    }

    // Find freed space.
    val deleteDF: DataFrame = spark.read.format(deleteReply.format).load(deleteReply.path)
    val freedSize: Long = deleteDF.queryExecution.analyzed.stats.sizeInBytes.toLong

    // Delete actual content.
    val config: Configuration = new Configuration()
    val pathToDelete = new Path(deleteReply.path)
    val fs: FileSystem = pathToDelete.getFileSystem(config)
    try {
      fs.delete(pathToDelete, true)
    } catch {
      case e: Exception =>
        logger.warn("Cannot delete file %s. Exception message: %s".format(deleteReply.path, e.getMessage))
        return 0L
    }

    // Reply with space freed.
    try {
      val stub = SemanticCacheServiceGrpc.blockingStub(channel)
      val request: FreeRequest = FreeRequest(clientId, deleteReply.path, freedSize)
      stub.free(request)
    } catch {
      case e: Exception =>
        logger.warn("Free failed: %s".format(e.getMessage))
        return 0L
    }
    val endTime: Long = System.nanoTime()
    writeTime += endTime - beginTime
    freedSize
  }

  /**
   * Semantic Cache Optimization for Spark. It extends SparkOptimizer with this extra Optimization.
   */
  object Optimization extends Rule[LogicalPlan] {
    def apply(inputPlan: LogicalPlan): LogicalPlan = {
      val beginTime: Long = System.nanoTime()
      val outputPlan: LogicalPlan = if (mode == Mode.NO_CACHE) {
        inputPlan
      } else if (applyOptimization && !skip) {
        logger.info("Initial Spark's Logical Plan:\n%s".format(inputPlan))
        val outputPlan: LogicalPlan = optimize(inputPlan)
        logger.info("Optimized Spark's Logical Plan:\n%s".format(outputPlan))
        if (!inputPlan.fastEquals(outputPlan))
          skip = true
        outputPlan
      } else if (!applyOptimization) {
        logger.info("Semantic Cache Optimization is deactivated for plan:\n.%s".format(inputPlan))
        inputPlan
      } else {
        logger.info("Skipped Semantic Cache optimization for plan:\n%s".format(inputPlan))
        skip = false
        inputPlan
      }
      val endTime: Long = System.nanoTime()
      fullOptimizeTime += endTime - beginTime
      outputPlan
    }
  }

  /**
   * Get status of Semantic Cache service.
   *
   * @return all the contents stored in semantic cache as well as other useful information.
   */
  def status: String = {
    val cacheStatusRequest: CacheStatusRequest = CacheStatusRequest(spark.sparkContext.applicationId)
    val stub = SemanticCacheServiceGrpc.blockingStub(channel)
    val cacheStatusReply: CacheStatusReply = stub.getStatus(cacheStatusRequest)
    cacheStatusReply.status
  }
}

/**
 * Easy way to activate semantic cache optimization for users.
 */
object SemanticCache {
  var semanticCache: Option[SemanticCache] = None
  var logger: Option[Logger] = None

  private[client] def getIndex(attrib: Attribute) = attrib.exprId.id.toInt

  private[cache] def checkSupport(plan: LogicalPlan): Support[Boolean] = {
    val children: Seq[Support[Boolean]] = plan.children.map(checkSupport)
    val support: Boolean = {
      if (children.forall(child => child.support)) {
        plan match {
          case Aggregate(groupingExpressions, aggregateExpressions, _) =>
            val groupCheck: Boolean = groupingExpressions.forall(expr => !expr.isInstanceOf[Nondeterministic])
            val aggregateCheck: Boolean = aggregateExpressions.forall(expr => !expr.isInstanceOf[Nondeterministic])
            groupCheck && aggregateCheck
          case Filter(condition, _) => !condition.isInstanceOf[Nondeterministic]
          case RepartitionByExpression(partitionExpressions, _, _) =>
            partitionExpressions.forall(expr => !expr.isInstanceOf[Nondeterministic])
          case _: Distinct | _: Project | _: Repartition => true
          case LogicalRelation(_: HadoopFsRelation, _, _, _) => true
          case _ => false
        }
      } else {
        false
      }
    }
    Support[Boolean](support, children)
  }

  private def transformAttributesInExpression(expression: Expression): Expression = {
    expression match {
      case ref: AttributeReference => CaerusAttribute(getIndex(ref), ref.dataType)
      case _ => expression.withNewChildren(expression.children.map(transformAttributesInExpression))
    }
  }

  private def transformAttributesInPlan(plan: CaerusPlan): CaerusPlan = {
    plan match {
      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        val newGroupingExpressions: Seq[Expression] = groupingExpressions.map(transformAttributesInExpression)
        val newAggregateExpressions: Seq[NamedExpression] =
          aggregateExpressions.map(transformAttributesInExpression).asInstanceOf[Seq[NamedExpression]]
        val newChild: CaerusPlan = transformAttributesInPlan(child)
        Aggregate(newGroupingExpressions, newAggregateExpressions, newChild)
      case Filter(condition, child) =>
        val newCondition: Expression = transformAttributesInExpression(condition)
        val newChild: CaerusPlan = transformAttributesInPlan(child)
        Filter(newCondition, newChild)
      case Project(projectList, child) =>
        val newProjectList: Seq[NamedExpression] =
          projectList.map(transformAttributesInExpression(_).asInstanceOf[NamedExpression])
        val newChild: CaerusPlan = transformAttributesInPlan(child)
        Project(newProjectList, newChild)
      case RepartitionByExpression(partitionExpressions, child, numPartitions) =>
        val newPartitionExpressions: Seq[Expression] = partitionExpressions.map(transformAttributesInExpression)
        val newChild: CaerusPlan = transformAttributesInPlan(child)
        RepartitionByExpression(newPartitionExpressions, newChild, numPartitions)
      case CaerusSourceLoad(output, sources, format) =>
        val newOutput: Seq[Attribute] = output.map(transformAttributesInExpression).asInstanceOf[Seq[Attribute]]
        CaerusSourceLoad(newOutput, sources, format)
      case _ => plan.withNewChildren(plan.children.map(transformAttributesInPlan))
    }
  }

  private def transformCanonicalized(plan: LogicalPlan): CaerusPlan = {
    val caerusPlan = plan match {
      case LogicalRelation(hadoopFsRelation: HadoopFsRelation, output, _, _) =>
        if (!hadoopFsRelation.fileFormat.isInstanceOf[DataSourceRegister]) {
          if (logger.isDefined)
            logger.get.warn("Format provided for Data-Skipping Indices is not supported. Format: %s\n"
              .format(hadoopFsRelation.fileFormat))
          return plan
        }
        val inputFiles: Seq[FileStatus] = hadoopFsRelation.location.asInstanceOf[PartitioningAwareFileIndex].allFiles()
        val inputs: Seq[SourceInfo] =
          inputFiles.map(f => SourceInfo(f.getPath.toString, f.getModificationTime, 0, f.getLen))
        val format: String = hadoopFsRelation.fileFormat.asInstanceOf[DataSourceRegister].shortName()
        CaerusSourceLoad(output, inputs, format)
      case _ =>
        plan.withNewChildren(plan.children.map(transformCanonicalized))
    }
    transformAttributesInPlan(caerusPlan)
  }

  private[cache] def transform(plan: LogicalPlan): CaerusPlan = {
    val canonicalizedPlan: CaerusPlan = plan.canonicalized
    if (logger.isDefined)
      logger.get.debug("Canonicalized Spark Plan:\n%s".format(canonicalizedPlan))
    transformCanonicalized(canonicalizedPlan)
  }

  def activate(sparkSession: SparkSession, semanticCacheURI: String): Unit = {
    semanticCache = Some(new SemanticCache(sparkSession, semanticCacheURI))
    logger = Some(semanticCache.get.logger)
  }
}