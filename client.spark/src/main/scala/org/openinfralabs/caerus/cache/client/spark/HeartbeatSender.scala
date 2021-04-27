package org.openinfralabs.caerus.cache.client.spark

import io.grpc.Channel
import org.openinfralabs.caerus.cache.grpc.service.{HeartbeatRequest, SemanticCacheServiceGrpc}
import org.slf4j.{Logger, LoggerFactory}

private[cache] class HeartbeatSender(sparkId: String, timeout: Long, channel: Channel) extends Runnable {
  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def run(): Unit = {
    while (!Thread.currentThread().isInterrupted) {
      logger.info("Sending heartbeat...")
      try {
        val heartbeatRequest: HeartbeatRequest = HeartbeatRequest(sparkId)
        val stub = SemanticCacheServiceGrpc.blockingStub(channel)
        stub.heartbeat(heartbeatRequest)
        Thread.sleep(timeout)
      } catch {
        case _: InterruptedException =>
          logger.warn("Terminated heartbeats.")
          return
        case e: Exception =>
          logger.warn("Failed to send heartbeat for Semantic Cache Client %s with the following message: %s"
            .format(sparkId, e.getMessage))
          throw new RuntimeException("Semantic Cache Client failed to send heartbeat to Semantic Cache Manager.")
      }
    }
    logger.warn("Terminated heartbeats.")
  }
}
