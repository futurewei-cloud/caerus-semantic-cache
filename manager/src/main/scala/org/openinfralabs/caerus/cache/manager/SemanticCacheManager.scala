package org.openinfralabs.caerus.cache.manager

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.grpc.{Server, ServerBuilder}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.openinfralabs.caerus.cache.common.Mode.Mode
import org.openinfralabs.caerus.cache.common.Tier.Tier
import org.openinfralabs.caerus.cache.common._
import org.openinfralabs.caerus.cache.common.plans.{CaerusCacheLoad, CaerusCaching, CaerusDelete, CaerusFileSkippingIndexing, CaerusIf, CaerusLoadWithIndices, CaerusPlan, CaerusRepartitioning, CaerusWrite}
import org.openinfralabs.caerus.cache.grpc.service._

import scala.collection.{breakOut, mutable}
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

/**
 * Semantic Cache service, which is shared and offers semantic-aware caching capabilities.
 *
 * @since 0.0.0
 */
class SemanticCacheManager(execCtx: ExecutionContext, conf: Config) extends LazyLogging {
  private val tierNames: Map[String, Tier] = Map(
    "storage-disk" -> Tier.STORAGE_DISK,
    "storage-memory" -> Tier.STORAGE_MEMORY,
    "compute-disk" -> Tier.COMPUTE_DISK,
    "compute-memory" -> Tier.COMPUTE_MEMORY
  )
  private val tiers: mutable.HashMap[Tier, String] = mutable.HashMap.empty[Tier, String]
  private val capacity: mutable.HashMap[Tier, Long] = mutable.HashMap.empty[Tier, Long]
  private var lastTier: Option[Tier] = None
  for (tier <- tierNames.keys) {
    val tierConfStr: String = "caches." + tier
    val siteConfStr: String = tierConfStr + ".site"
    val sizeConfStr: String = tierConfStr + ".size"
    if (conf.hasPath(siteConfStr) && conf.hasPath(sizeConfStr)) {
      val site: String = conf.getString(siteConfStr)
      val size: Long = conf.getMemorySize(sizeConfStr).toBytes
      if (size <= 0) {
        logger.warn("Size for tier %s is lower or equal to 0".format(tier))
      } else {
        tiers(tierNames(tier)) = site
        capacity(tierNames(tier)) = size
        lastTier = Some(tierNames(tier))
      }
    } else {
        logger.warn("Not all parameters are set for tier %s.".format(tier))
    }
  }

  private val operationMode: Mode = {
    if (conf.hasPath("server.mode")) {
      val operationMode: Int = conf.getInt("server.mode")
      logger.info("Operation mode set to %s".format(operationMode))
      Mode(operationMode)
    } else {
      logger.warn("Operation mode not set. Using default mode 3.")
      Mode(3)
    }
  }
  // print out tier configuration for mode3
  if (operationMode == Mode.FULLY_AUTOMATIC && tiers.size >1) {
    logger.info("Tiers info for Mode 3")
    tiers.foreach(tierPair=>logger.info("Tier: %s, Path: %s".format(tierPair._1, tierPair._2)))
  }
  // disable this since we now try to support multi-tier
  /*
  if (operationMode == Mode.FULLY_AUTOMATIC && (tiers.size > 1 || !tiers.contains(Tier.STORAGE_DISK))) {
    logger.error("Operation mode 3 cannot be defined with multiple tiers. Only with STORAGE_DISK.")
    System.exit(-1)
  }*/

  private val port: Int = {
    if (conf.hasPath("server.port"))
      conf.getInt("server.port")
    else
      35001
  }

  private val timeout: Long = {
    if (conf.hasPath("timeout"))
      conf.getLong("timeout") * 60000L
    else
      60000L
  }

  private var pathId: Long = 0L
  private val names: mutable.HashMap[String, Candidate] = mutable.HashMap.empty[String, Candidate]
  private val namesTier: mutable.HashMap[String, Tier] = mutable.HashMap.empty[String, Tier]
  private val contents: mutable.HashMap[Candidate,String] = mutable.HashMap.empty[Candidate,String]
  private val multiTierContents: mutable.HashMap[Tier, mutable.HashMap[Candidate,String]] = mutable.HashMap.empty[Tier, mutable.HashMap[Candidate,String]]
  for((tier, path) <- tiers) multiTierContents(tier) = mutable.HashMap.empty[Candidate,String]
  private val reservations: mutable.HashMap[(String,Candidate),String] =
    mutable.HashMap.empty[(String,Candidate),String]
  private val markedForDeletion: mutable.HashSet[String] = mutable.HashSet.empty[String]
  private val optimizer: Optimizer = UnifiedOptimizer()
  private val predictorConfStr: String = "predictor"
  private val windowSize: Int = conf.getInt(predictorConfStr + ".windowSize")
  private val predictor: Predictor = conf.getString(predictorConfStr + ".type") match {
    case "oracle" =>
      val filename: String = conf.getString(predictorConfStr + ".file")
      val source = Source.fromFile(filename)
      val futurePlans: Seq[CaerusPlan] = source.getLines().map(CaerusPlan.fromJSON).toSeq
      logger.debug("Future Plans:\n%s".format(futurePlans.mkString("\n")))
      OraclePredictor(futurePlans, windowSize)
    case "reverseorder" =>
      logger.info("Reverse order predictor with limit : %s".format(windowSize))
      ReverseOrderPredictor(windowSize)
  }
  private val outputPath: String = {
    if (tiers.contains(Tier.STORAGE_DISK))
      tiers(Tier.STORAGE_DISK)
    else
      "none"
  }
  // private val planner: Planner = BasicStorageIOPlanner(optimizer, predictor, outputPath)
  private val planner: Planner = BasicStorageIOMultiTierPlanner(optimizer, predictor,tiers.toMap)
  private var server: Option[Server] = None
  private val references: mutable.HashMap[String, Set[String]] = mutable.HashMap.empty[String, Set[String]]
  private val registeredClients: mutable.HashMap[String, Long] = mutable.HashMap.empty[String, Long]
  private val reverseReferences: mutable.HashMap[String, Set[String]] = mutable.HashMap.empty[String, Set[String]]

  private def start(): Unit = {
    val service = SemanticCacheServiceGrpc.bindService(new SemanticCacheService, execCtx)
    server = Some(ServerBuilder.forPort(port).addService(service).build.start)
    logger.info("Server started with mode %d on port %s\n".format(operationMode.id, port))
    sys.addShutdownHook {
      logger.warn("Shutting down gRPC server...\n")
      stop()
      logger.warn("The gRPC server is terminated.\n")
    }
  }

  private def stop(): Unit = {
    if (server.isDefined) {
      server.get.shutdown()
    }
  }

  def terminateClient(clientId: String): Unit = {
    reservations.keys.filter(elem => elem._1 == clientId).toSet.foreach(reservations.remove)
    registeredClients.remove(clientId)
    references(clientId).foreach(path => reverseReferences(path) = reverseReferences(path) - clientId)
    references.remove(clientId)
  }

  private def terminateClients(): Unit = {
    val terminatedClients = registeredClients.keys.filter(clientId => {
      System.currentTimeMillis() - registeredClients(clientId) > timeout
    })
    terminatedClients.foreach(terminateClient)
  }

  private def blockUntilShutdown(): Unit = {
    if (server.isDefined) {
      server.get.awaitTermination()
    }
  }

  private def getPathId: Long = {
    val res: Long = pathId
    pathId += 1
    res
  }

  private def generatePath(candidateName: String, tier: Tier): String = {
    if (!tiers.contains(tier))
      throw new RuntimeException("Tier %s is not associated with a cache.".format(tier))
    tiers(tier) + Path.SEPARATOR + candidateName + getPathId.toString
  }

  private def getPath(candidateName: String, tier: Tier): String = {
    if (!tiers.contains(tier))
      throw new RuntimeException("Tier %s is not associated with a cache.".format(tier))
    tiers(tier) + Path.SEPARATOR + candidateName
  }

  private def getTierFromPath(path: String): Tier = {
    val tierCandidates = tiers.filter(tuple => path.startsWith(tuple._2)).keys
    if (tierCandidates.isEmpty)
      throw new RuntimeException("Could not find tier for path %s.".format(path))
    tierCandidates.head
  }

  private def getCacheStatus: CacheStatusReply = {
    CacheStatusReply(status =
      "Reservations: %s\nContents: %s\nMark for Deletion: %s\n".format(reservations, contents, markedForDeletion)
    )
  }

  /**
   * Add one to reference count.
   * @param clientId Semantic Cache Client which makes the reference.
   * @param path  Referenced content.
   */
  private def addReference(clientId: String, path: String): Unit = {
    references(clientId) = references(clientId) + path
  }

  private def emptyAddReference(path: String): Unit = {}

  private class SemanticCacheService extends SemanticCacheServiceGrpc.SemanticCacheService {
    /**
     * Semantic Cache Administrator API. Reservation RPC.
     */
    override def reserve(request: ReservationRequest): Future[PathReply] = {
      if (!registeredClients.contains(request.clientId)) {
        val message: String = "Client %s is not registered.".format(request.clientId)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      registeredClients(request.clientId) = System.currentTimeMillis()
      val candidate: Candidate = Candidate.fromJSON(request.candidate)
      val reservedSize: Long = candidate.sizeInfo.get.writeSize
      val path = request.name
      val tier: Tier = getTierFromPath(path)
      //val tier: Tier = Tier(request.tier)

      logger.info("input parameters for reserve, candidate: %s, tier:%s, name: %s".format(candidate.toString,tier,request.name))
      /*val path = try {
        if (operationMode == Mode.FULLY_AUTOMATIC || operationMode == Mode.MANUAL_EVICTION)
          getPath(request.name, tier)
        else
          generatePath(request.name, tier)
      } catch {
        case e: Exception =>
          logger.warn(e.getMessage)
          return Future.failed(e)
      }*/
      if (names.contains(request.name)) {
        val message: String = "Name %s is already used for cached or reserved contents.".format(request.name)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      if (reservations.contains((request.clientId,candidate)) || contents.contains(candidate)) {
        val message: String = "The following candidate is already cached or reserved:\n%s".format(candidate)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      if (capacity(tier) < candidate.sizeInfo.get.writeSize) {
        val message: String = "Insufficient capacity for tier %s.\tAsked to reserve: %s\tAvailable: %s"
            .format(tier, candidate.sizeInfo.get.writeSize, capacity(tier))
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      names(request.name) = candidate
      namesTier(request.name) = tier
      reservations((request.clientId,candidate)) = path
      capacity(tier) -= reservedSize
      logger.info("Reserve for candidate:\n%s\nReserved size: %s\tUpdated capacity of tier %s: %s."
          .format(candidate.toString, reservedSize, tier, capacity(tier)))
      Future.successful(PathReply(path))
    }

    /**
     * Semantic Cache Administrator API. Commit reservation RPC.
     */
    override def commitReservation(request: CommitReservationRequest): Future[Ack] = {
      if (!registeredClients.contains(request.clientId)) {
        val message: String = "Client %s is not registered.".format(request.clientId)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      registeredClients(request.clientId) = System.currentTimeMillis()
      if (!names.contains(request.name)) {
        val message = "Candidate with name %s cannot be found.".format(request.name)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      val candidate: Candidate = names(request.name)
      if (contents.contains(candidate)) {
        val message: String = "The following candidate is already cached:\n%s".format(candidate)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      if (!reservations.contains((request.clientId,candidate))) {
        val message: String = "The following candidate is not reserved:\n%s".format(candidate)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      val path: String = reservations((request.clientId,candidate))
      val reservedSize: Long = candidate.sizeInfo.get.writeSize
      val tier: Tier = getTierFromPath(path)
      if (request.commit) {
        logger.info("Submit candidate:\n %s\nReserved size: %s\tReal size: %s\tCorrected Capacity: %s.".format(
          candidate.toString,
          reservedSize,
          request.realSize,
          capacity(tier) + reservedSize - request.realSize
        ))
        val newSizeInfo: SizeInfo = SizeInfo(request.realSize, candidate.sizeInfo.get.readSizeInfo)
        val newCandidate: Candidate = candidate.withNewSizeInfo(newSizeInfo)
        names(request.name) = newCandidate
        contents.put(newCandidate,path)
        val temp_contents: mutable.HashMap[Candidate,String]  = multiTierContents(tier)
        temp_contents.put(newCandidate,path)
        multiTierContents(tier) = temp_contents
        reverseReferences(path) = Set.empty[String]
        logger.info("Updated Contents: %s\n".format(contents))
      } else {
        logger.info("Cancel candidate:\n%s\nReserved size: %s\tCorrected Capacity: %s.".format(
          candidate.toString,
          reservedSize,
          capacity(tier) + reservedSize
        ))
        names.remove(request.name)
      }
      capacity(tier) += reservedSize - request.realSize
      reservations.remove((request.clientId,candidate))
      Future.successful(Ack())
    }

    /**
     * Semantic Cache Administrator API. Deletion RPC.
     */
    override def delete(request: DeleteRequest): Future[DeleteReply] = {
      if (!registeredClients.contains(request.clientId)) {
        val message: String = "Client %s is not registered.".format(request.clientId)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      registeredClients(request.clientId) = System.currentTimeMillis()
      val name: String = request.name
      if (!names.contains(name)) {
        val message = "Candidate with name %s cannot be found.".format(name)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      val candidate: Candidate = names(name)
      val format: String = candidate match {
        case _: Repartitioning | _: Caching => "parquet"
        case _: FileSkippingIndexing => "json"
      }
      if (!contents.contains(candidate)) {
        val message = "Submission is ignored since there is no content for the following candidate:\n%s"
          .format(candidate.toString)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      val path: String = contents(candidate)
      assert(reverseReferences.contains(path))
      val tier: Tier = getTierFromPath(path)
      logger.info("Mark candidate for deletion: %s\n".format(candidate))
      markedForDeletion.add(path)
      terminateClients()
      if (reverseReferences(path).nonEmpty) {
        val message = "Submission is ignored since path %s has still active references %s."
          .format(path, reverseReferences(path).mkString(","))
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      reverseReferences(path).foreach(clientId => references(clientId) = references(clientId) - path)
      reverseReferences.remove(path)
      logger.info("References: %s".format(references))
      logger.info("Reverse References: %s".format(reverseReferences))
      contents.remove(candidate)
      logger.info("Updated Contents: %s\n".format(contents))
      val temp_contents: mutable.HashMap[Candidate,String]  = multiTierContents(tier)
      temp_contents.remove(candidate)
      multiTierContents(tier) = temp_contents
      logger.info("Update Multi-tire Contents. Tier: %s, Contents: %s\n".format(tier, temp_contents))
      Future.successful(DeleteReply(path, format))
    }

    /**
     * Semantic Cache Administrator API. Commit deletion RPC.
     */
    override def free(request: FreeRequest): Future[Ack] = {
      if (!registeredClients.contains(request.clientId)) {
        val message: String = "Client %s is not registered.".format(request.clientId)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      registeredClients(request.clientId) = System.currentTimeMillis()
      if (!markedForDeletion(request.path)) {
        val message: String = "Release for path %s is ignored since it is not marked for deletion.".format(request.path)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      val tier: Tier = getTierFromPath(request.path)
      capacity(tier) -= request.realSize
      logger.info("Free:%s\tReleased size: %s\tCorrected Capacity: %s."
        .format(request.path, request.realSize, capacity(tier)))
      Future.successful(Ack())
    }

    /**
     * Semantic Cache Client API. Registration RPC.
     */
    override def register(request: RegisterRequest): Future[RegisterReply] = {
      if (registeredClients.contains(request.clientId)) {
        val message: String = "Ignoring registration since client %s is already registered.".format(request.clientId)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      logger.info("Register client %s with timeout %s and mode %d".format(request.clientId, timeout, operationMode.id))
      registeredClients(request.clientId) = System.currentTimeMillis()
      references(request.clientId) = Set.empty[String]
      Future.successful(RegisterReply(operationMode.id, timeout))
    }

    /**
     * Semantic Cache Client API. Heartbeat RPC.
     */
    override def heartbeat(request: HeartbeatRequest): Future[Ack] = {
      if (!registeredClients.contains(request.clientId)) {
        val message: String = "Client %s is not registered.".format(request.clientId)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      registeredClients(request.clientId) = System.currentTimeMillis()
      Future.successful(Ack())
    }

    private def printLoadWithIndices(caerusPlan: CaerusPlan): Unit = {
      caerusPlan match {
        case caerusLoadWithIndices: CaerusLoadWithIndices =>
          logger.info("CaerusLoadWithIndices path before serialization:")
          for (path <- caerusLoadWithIndices.sources.map(_.path))
            logger.debug(path)
          logger.info("JSON Plan: %s".format(caerusLoadWithIndices.toJSON))
        case _ =>
          caerusPlan.children.foreach(printLoadWithIndices)
      }
    }

    private def insertCaerusWrite(
                                   plan: CaerusPlan,
                                   backupPlan: CaerusPlan,
                                   caerusWrite: CaerusWrite,
                                   caerusDelete: Option[CaerusDelete],
                                 ): CaerusPlan = {
      logger.info("SemanticCacheManager: Insert Caerus Write for plan:\n%s \n, backupPlan:\n%s \n, caeruswrite:\n%s \n, caerusdelete \n%s \n ".format(plan, backupPlan, caerusWrite,caerusDelete))
      plan match {
        case caerusCacheLoad: CaerusCacheLoad if caerusCacheLoad.sources == Seq(caerusWrite.name) =>
          var caerusIf: CaerusIf = caerusWrite match {
            case _: CaerusRepartitioning | _: CaerusCaching =>
              CaerusIf(Seq(caerusWrite, plan, backupPlan))
            case _ =>
              logger.warn("Found caerus write %s, when we should only have Repartitioning and Caching"
                .format(caerusWrite))
              return plan
          }
          if (caerusDelete.isDefined)
            caerusIf = CaerusIf(Seq(caerusDelete.get, caerusIf, caerusIf.children(2)))
          logger.info("in caerusCacheLoad, returned plan \n %s".format(caerusIf))
          caerusIf
        case CaerusLoadWithIndices(_, loadChild, _, loadIndex) =>
          logger.info("in caerusCacheLoad")
          caerusWrite match {
            case CaerusFileSkippingIndexing(name, _, index) if index == loadIndex =>
              val caerusCacheLoad: CaerusCacheLoad =
                loadChild.asInstanceOf[Project].child.asInstanceOf[Filter].child.asInstanceOf[CaerusCacheLoad]
              if (caerusCacheLoad.sources == Seq(name)) {
                var caerusIf = CaerusIf(Seq(caerusWrite, plan, backupPlan))
                if (caerusDelete.isDefined)
                  caerusIf = CaerusIf(Seq(caerusDelete.get, caerusIf, caerusIf.children(2)))
                caerusIf
              } else {
                plan
              }
            case _ =>
              plan
          }
        case _ =>
          logger.info("in write else")
          assert(plan.children.size == backupPlan.children.size)
          val newChildren: Seq[CaerusPlan] = plan.children.indices.map(i =>
            insertCaerusWrite(plan.children(i), backupPlan.children(i), caerusWrite, caerusDelete))
          plan.withNewChildren(newChildren)
      }
    }

    /**
     * Semantic Cache Client API. Optimization RPC.
     */
    override def optimize(request: OptimizationRequest): Future[OptimizationReply] = {
      if (!registeredClients.contains(request.clientId)) {
        val message: String = "Client %s is not registered.".format(request.clientId)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      registeredClients(request.clientId) = System.currentTimeMillis()
      val caerusPlan: CaerusPlan = CaerusPlan.fromJSON(request.caerusPlan)
      val availableContents: mutable.Map[Candidate,String] =
        contents.filter(element => !markedForDeletion.contains(element._2))
      val availableMultiTierContents: mutable.HashMap[Tier, Map[Candidate,String]] = mutable.HashMap.empty[Tier, Map[Candidate,String]]
      for((tier, contents) <- multiTierContents){
        availableMultiTierContents(tier) = contents.filter(element => !markedForDeletion.contains(element._2)).toMap
      }
      val optimizedPlan = {
        if (operationMode == Mode.MANUAL_WRITE) {
          optimizer.optimize(
            caerusPlan,
            availableContents.toMap[Candidate,String],
            path => addReference(request.clientId, path)
          )
        } else if (operationMode == Mode.FULLY_AUTOMATIC) {
          /*val cap: Long = {
            if (lastTier.isDefined)
              capacity(lastTier.get)
            else
              0L
          }*/
          val initialOptimizedPlan:CaerusPlan = {
            var allContents : mutable.Map[Candidate,String] = mutable.Map.empty[Candidate,String]
            for((tier, contents) <- availableMultiTierContents){
              allContents= allContents ++ contents
            }
            optimizer.optimize(caerusPlan, allContents.toMap, emptyAddReference)
          }
          logger.info("Initial optimized plan without candidate: %s".format(initialOptimizedPlan))

          val newMultiTierContents:Map[Tier, Map[Candidate,String]] = planner.optimize(
            caerusPlan,
            availableMultiTierContents.toMap,
            request.candidates.map(Candidate.fromJSON),
            capacity.toMap
          )
          // so now we have new multi-tier contents, and will loop through tiers to issue write and eviction
          var interOptimalPlan: mutable.HashMap[Tier, CaerusPlan] = mutable.HashMap.empty[Tier, CaerusPlan]
          for((tier,contents) <- availableMultiTierContents ){
            val newContents = newMultiTierContents(tier)
            logger.info("New Multi-tire Contents. Tier: %s, Contents: %s\n".format(tier, newContents.mkString("\n")))
            val top_can : Set[Candidate] = newContents.keySet -- contents.keySet
            if(top_can.nonEmpty && top_can.size == 1){ // means we have a new candidate to write
              val topCandidate: Candidate = top_can.head
              val topCandidatePath = newContents(topCandidate)
              logger.info("Top Candidate to write: %s, path: %s".format(topCandidate, topCandidatePath))
              val caerusWrite: CaerusWrite = topCandidate match {
                case Repartitioning(source, index, _) =>
                  CaerusRepartitioning(topCandidatePath, source, index)
                case FileSkippingIndexing(source, index, _) =>
                  CaerusFileSkippingIndexing(topCandidatePath, source, index)
                case Caching(cachedPlan, _) =>
                  val optimizedCachedPlan = optimizer.optimize(cachedPlan, newContents - topCandidate, emptyAddReference)
                  CaerusCaching(topCandidatePath, cachedPlan, optimizedCachedPlan)
              }
              val caerusDelete: Option[CaerusDelete] = if (!contents.keySet.subsetOf(newContents.keySet)) {
                val deletedCandidates: Set[Candidate] = contents.keySet -- newContents.keySet
                Some(CaerusDelete(deletedCandidates.toSeq.map(contents)))
              } else {
                None
              }
              val optimizedPlan: CaerusPlan = optimizer.optimize(initialOptimizedPlan, newContents, emptyAddReference)
              val backupPlan: CaerusPlan = optimizer.optimize(initialOptimizedPlan, newContents-topCandidate, emptyAddReference)
              interOptimalPlan(tier) = insertCaerusWrite(optimizedPlan, backupPlan, caerusWrite, caerusDelete)
            }
          }
          var finalOptimizedPlan: CaerusPlan = initialOptimizedPlan
          if(interOptimalPlan.isEmpty){
            logger.info("No new Contents need to write/update, Doing last optimization with all contents from all tiers")
            //so now we updated all the contents, will use all the contents, new and old to do one last optimization
            var allContents : mutable.Map[Candidate,String] = mutable.Map.empty[Candidate,String]
            for((tier, contents) <- newMultiTierContents){
              logger.info("Add Contents from Tier: %s, to allContents:  %s\n".format(tier, contents.mkString("\n")))
              allContents= allContents ++ contents
            }
            logger.info("Contents from all the Tiers: %s\n".format(allContents.mkString("\n")))
            finalOptimizedPlan = optimizer.optimize(caerusPlan, allContents.toMap, emptyAddReference)
          }
          else { // need to figure out a better way to pick plan, now simply pick plan from higher tier
            for(tier<-Tier.values.toList.reverse){
              if(interOptimalPlan.contains(tier)){
                finalOptimizedPlan = interOptimalPlan(tier)
                logger.info("optimize plan from tier %s : %s".format(tier, finalOptimizedPlan))
              }
            }
          }
          logger.info("Final optimized plan: %s".format(finalOptimizedPlan))
          finalOptimizedPlan
        } else {
          val message = "Mode %s is not supported yet.".format(operationMode.id)
          logger.warn(message)
          caerusPlan
        }
      }
      logger.info("References: %s".format(references))
      logger.info("Reverse References: %s".format(reverseReferences))
      logger.info("optimized CaerusPlan before send out: %s".format(optimizedPlan))
      //printLoadWithIndices(optimizedPlan)
      Future.successful(OptimizationReply(optimizedCaerusPlan = optimizedPlan.toJSON))
    }

    /**
     * Semantic Cache Administrator Monitoring API. GetStatus RPC
     */
    override def getStatus(request: CacheStatusRequest): Future[CacheStatusReply] = {
      if (!registeredClients.contains(request.clientId)) {
        val message: String = "Client %s is not registered.".format(request.clientId)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      registeredClients(request.clientId) = System.currentTimeMillis()
      if (operationMode != Mode.MANUAL_WRITE) {
        val message: String = "Get Status API is not allowed for mode %s".format(operationMode)
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      Future.successful(getCacheStatus)
    }
  }
}

object SemanticCacheManager {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      Console.err.println("Configuration file should be the only argument.")
      return
    }
    val conf: Config = ConfigFactory.parseResources(args(0))
    val server = new SemanticCacheManager(ExecutionContext.global, conf)
    server.start()
    server.blockUntilShutdown()
  }
}
