package org.openinfralabs.caerus.cache.examples.spark.gridpocket

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.openinfralabs.caerus.cache.client.spark.SemanticCache
import org.openinfralabs.caerus.cache.common.Tier

object Caching {
  def main(args: Array[ String ]) {
    // Get arguments.
    val sparkURI: String = args(0)
    val semanticCacheURI: String = args(1)
    val inputPath: String = args(2)
    val minTemp: String = args(3)
    val maxTemp: String = args(4)
    val useMaxTemp: Boolean = args(5).toBoolean
    val cachedContentName: String = args(6)

    // Initialize Spark.
    val spark: SparkSession = SparkSession.builder()
      .master(sparkURI)
      .appName(name = "SemanticCacheCaching")
      .getOrCreate()

    // Initialize Semantic Cache connector.
    val semanticCache = new SemanticCache(spark, semanticCacheURI)

    // Initialize data format.
    val sch = StructType(Array(
      StructField("vid", StringType, nullable = true),
      StructField("date", TimestampType, nullable = true),
      StructField("index", DoubleType, nullable = true),
      StructField("sumHC", DoubleType, nullable = true),
      StructField("sumHP", DoubleType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("size", IntegerType, nullable = true),
      StructField("temp", DoubleType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("region", StringType, nullable = true),
      StructField("lat", DoubleType, nullable = true),
      StructField("lng", DoubleType, nullable = true)))

    // Create leaf node.
    val loadDF = spark.read.schema(sch).option("header", value = true).csv(inputPath)
    val conditionExpression: String =
      if (useMaxTemp)
        "temp >= " + minTemp + " AND temp < " + maxTemp
      else
        "temp >= " + minTemp
    val filterDF = loadDF.filter(conditionExpr = conditionExpression)

    // Cache repartition content.
    val bytesWritten = semanticCache.cacheIntermediateData(filterDF, Tier.STORAGE_DISK, cachedContentName)
    assert(bytesWritten > 0L)
    Console.out.println(semanticCache.status)
    spark.stop()
  }
}
