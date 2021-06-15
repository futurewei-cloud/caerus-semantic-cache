package org.openinfralabs.caerus.cache.examples.spark.gridpocket

import org.apache.spark.sql.SparkSession
import org.openinfralabs.caerus.cache.client.spark.SemanticCache

object Delete {
  def main(args: Array[ String ]) {
    // Get arguments.
    val sparkURI: String = args(0)
    val semanticCacheURI: String = args(1)
    val name: String = args(2)

    // Initialize Spark.
    val spark: SparkSession = SparkSession.builder()
      .master(sparkURI)
      .appName(name = "SemanticCacheDelete")
      .getOrCreate()

    // Initialize Semantic Cache connector.
    val semanticCache: SemanticCache = new SemanticCache(spark, semanticCacheURI)

    // Cache repartition content.
    val freedSize: Long = semanticCache.delete(name)
    Console.out.println("Freed space: %s".format(freedSize))
    Console.out.println(semanticCache.status)
    spark.stop()
  }
}
