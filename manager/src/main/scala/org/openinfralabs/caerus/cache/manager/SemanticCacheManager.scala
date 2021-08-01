package org.openinfralabs.caerus.cache.manager

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.grpc.{Server, ServerBuilder}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.openinfralabs.caerus.cache.common.Mode.Mode
import org.openinfralabs.caerus.cache.common.Tier.Tier
import org.openinfralabs.caerus.cache.common._
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
import org.openinfralabs.caerus.cache.grpc.service._

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

/**
 * Semantic Cache service, which is shared and offers semantic-aware caching capabilities.
 *
 * @since 0.0.0
 */
class SemanticCacheManager(execCtx: ExecutionContext, conf: Config) extends LazyLogging {
  private val tierNames: Map[String, Tier] = Map(
    "compute-memory" -> Tier.COMPUTE_MEMORY,
    "compute-disk" -> Tier.COMPUTE_DISK,
    "storage-memory" -> Tier.STORAGE_MEMORY,
    "storage-disk" -> Tier.STORAGE_DISK
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
  if (operationMode == Mode.FULLY_AUTOMATIC && (tiers.size > 1 || !tiers.contains(Tier.STORAGE_DISK))) {
    logger.error("Operation mode 3 cannot be defined with multiple tiers. Only with STORAGE_DISK.")
    System.exit(-1)
  }

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

  private var plannerStr : String  = ""
  private val optimizer: Optimizer ={
    if (conf.hasPath("planner.type"))
      {
        if(conf.getString("planner.type") == "lrc") {
          plannerStr = conf.getString("planner.type")
        }
        else if(conf.getString("planner.type") == "mrd"){
          plannerStr = conf.getString("planner.type")
        }
        else if(conf.getString("planner.type") == "basic"){
          plannerStr = conf.getString("planner.type")
        }
        else
          {
            logger.error("Planner type does not exist")
          }
        UnifiedOptimizer(plannerStr)
      }
    else
      {
        plannerStr = "basic"
        UnifiedOptimizer("basic")
      }
  }
  private var pathId: Long = 0L
  private val names: mutable.HashMap[String, Candidate] = mutable.HashMap.empty[String, Candidate]
  private val contents: mutable.HashMap[Candidate,String] = mutable.HashMap.empty[Candidate,String]
  private val reservations: mutable.HashMap[(String,Candidate),String] =
    mutable.HashMap.empty[(String,Candidate),String]
  private val markedForDeletion: mutable.HashSet[String] = mutable.HashSet.empty[String]
  //private val optimizer: Optimizer = UnifiedOptimizer()
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
  private val planner: Planner = {
    if(plannerStr == "lrc")
      LRCPlanner(optimizer, predictor, outputPath)
    else if(plannerStr == "mrd")
      MRDPlanner(optimizer, predictor, outputPath)
    else
      BasicStorageIOPlanner(optimizer, predictor, outputPath)

  }
  logger.info("%s planner initialized".format(plannerStr))
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
      val tier: Tier = Tier(request.tier)
      val path = try {
        if (operationMode == Mode.FULLY_AUTOMATIC || operationMode == Mode.MANUAL_EVICTION)
          getPath(request.name, tier)
        else
          generatePath(request.name, tier)
      } catch {
        case e: Exception =>
          logger.warn(e.getMessage)
          return Future.failed(e)
      }
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
      val optimizedPlan = {
        if (operationMode == Mode.MANUAL_WRITE) {
          optimizer.optimize(
            caerusPlan,
            availableContents.toMap[Candidate,String],
            path => addReference(request.clientId, path)
          )
        } else if (operationMode == Mode.FULLY_AUTOMATIC) {
          val cap: Long = {
            if (lastTier.isDefined)
              capacity(lastTier.get)
            else
              0L
          }
          planner.optimize(
            caerusPlan,
            availableContents.toMap[Candidate,String],
            request.candidates.map(Candidate.fromJSON),
            cap
          )
        } else {
          val message = "Mode %s is not supported yet.".format(operationMode.id)
          logger.warn(message)
          caerusPlan
        }
      }
      logger.info("References: %s".format(references))
      logger.info("Reverse References: %s".format(reverseReferences))
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