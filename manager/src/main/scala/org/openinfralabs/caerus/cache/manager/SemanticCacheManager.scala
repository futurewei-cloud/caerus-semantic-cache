package org.openinfralabs.caerus.cache.manager

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.grpc.{Server, ServerBuilder}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.openinfralabs.caerus.cache.common.Tier.Tier
import org.openinfralabs.caerus.cache.common._
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan
import org.openinfralabs.caerus.cache.grpc.service._

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

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
    "storage-disk" -> Tier.STORAGE_DISK)
  private val tiers: mutable.HashMap[Tier, String] = mutable.HashMap.empty[Tier, String]
  private val capacity: mutable.HashMap[Tier, Long] = mutable.HashMap.empty[Tier, Long]
  for (tier <- tierNames.keys) {
    val tierConfStr: String = "caches." + tier
    val siteConfStr: String = tierConfStr + ".site"
    val sizeConfStr: String = tierConfStr + ".size"
    if (conf.hasPath(siteConfStr) && conf.hasPath(sizeConfStr)) {
      val site: String = conf.getString(siteConfStr)
      val size: Long = conf.getMemorySize(sizeConfStr).toBytes
      if (size <= 0) {
        logger.warn("Size for tier %s is lower or equal to 0\n".format(tier))
      } else {
          tiers(tierNames(tier)) = site
          capacity(tierNames(tier)) = size
      }
    } else {
        logger.warn("Not all parameters are set for tier %s.\n".format(tier))
    }
  }

  private val port: Int = {
    if (conf.hasPath("server.port"))
      conf.getInt("server.port")
    else
      35001
  }

  private val timeout: Long = {
    if (conf.hasPath("timeout"))
      conf.getLong("timeout")
    else
      60000
  }

  private var pathId: Long = 0L
  private val names: mutable.HashMap[String, Candidate] = mutable.HashMap.empty[String, Candidate]
  private val contents: mutable.HashMap[Candidate,String] = mutable.HashMap.empty[Candidate,String]
  private val reservations: mutable.HashMap[(String,Candidate),(String,Long)] =
    mutable.HashMap.empty[(String,Candidate),(String,Long)]
  private val markedForDeletion: mutable.HashSet[String] = mutable.HashSet.empty[String]
  private val optimizer: Optimizer = UnifiedOptimizer()
  private var server: Option[Server] = None
  private val references: mutable.HashMap[String, Set[String]] = mutable.HashMap.empty[String, Set[String]]
  private val registeredClients: mutable.HashMap[String, Long] = mutable.HashMap.empty[String, Long]
  private val reverseReferences: mutable.HashMap[String, Set[String]] = mutable.HashMap.empty[String, Set[String]]

  private def start(): Unit = {
    val service = SemanticCacheServiceGrpc.bindService(new SemanticCacheService, execCtx)
    server = Some(ServerBuilder.forPort(port).addService(service).build.start)
    logger.info("Server started on port %s\n".format(port))
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

  private def getPath(candidate: Candidate, tier: Tier): String = {
    if (!tiers.contains(tier))
      throw new RuntimeException("Tier %s is not associated with a cache.".format(tier))
    val candidateName = candidate match {
      case _: Repartitioning => "R"
      case _: FileSkippingIndexing => "FSI"
      case _: Caching => "C"
      case _ => throw new RuntimeException("Candidate %s not recognized.".format(candidate))
    }
    tiers(tier) + Path.SEPARATOR + candidateName + getPathId.toString
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
      val tier: Tier = Tier(request.tier)
      val path = try {
        getPath(candidate, tier)
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
      if (capacity(tier) < request.estimatedSize) {
        val message: String = "Insufficient capacity for tier %s.\tAsked to reserve: %s\tAvailable: %s"
            .format(tier, request.estimatedSize, capacity(tier))
        logger.warn(message)
        return Future.failed(new RuntimeException(message))
      }
      names(request.name) = candidate
      reservations((request.clientId,candidate)) = (path,request.estimatedSize)
      capacity(tier) -= request.estimatedSize
      logger.info("Reserve for candidate:\n%s\nReserved size: %s\tUpdated capacity of tier %s: %s."
          .format(candidate.toString, request.estimatedSize, tier, capacity(tier)))
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
      val reservationStatus: (String,Long) = reservations((request.clientId,candidate))
      val path: String = reservationStatus._1
      val reservedSize: Long = reservationStatus._2
      val tier: Tier = getTierFromPath(path)
      if (request.commit) {
        logger.info("Submit candidate:\n %s\nReserved size: %s\tReal size: %s\tCorrected Capacity: %s.".format(
          candidate.toString,
          reservedSize,
          request.realSize,
          capacity(tier) + reservedSize - request.realSize
        ))
        contents.put(candidate,path)
        reverseReferences(path) = Set.empty[String]
        logger.info("Updated Contents: %s\n".format(contents))
      } else {
        logger.info("Cancel candidate:\n%s\nReserved size: %s\tCorrected Capacity: %s.".format(
          candidate.toString,
          reservedSize,
          capacity(tier) + reservedSize - request.realSize
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
    override def delete(request: DeleteRequest): Future[PathReply] = {
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
      Future.successful(PathReply(path))
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
      if (!markedForDeletion.contains(request.path)) {
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
      registeredClients(request.clientId) = System.currentTimeMillis()
      references(request.clientId) = Set.empty[String]
      Future.successful(RegisterReply(timeout*60000))
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
      val optimizedPlan = optimizer.optimize(
          caerusPlan,
          availableContents.toMap[Candidate,String],
          path => addReference(request.clientId, path)
      )
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