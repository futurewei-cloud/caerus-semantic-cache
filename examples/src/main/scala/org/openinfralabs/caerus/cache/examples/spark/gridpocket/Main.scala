package org.openinfralabs.caerus.cache.examples.spark.gridpocket

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.openinfralabs.caerus.cache.client.spark.SemanticCache

object Main {
  def main(args: Array[ String ]) {
    // Get arguments.
    val sparkURI: String = args(0)
    val semanticCacheURI: String = args(1)
    val inputPath: String = args(2)
    val outputPath: String = args(3)

    // Initialize Spark.
    val spark: SparkSession = SparkSession.builder()
      .master(sparkURI)
      .appName(name = "SemanticCacheExample")
      .getOrCreate()

    // Activate Semantic Cache optimizations.
    SemanticCache.activate(spark, semanticCacheURI)

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

    // Run queries.
    loadDF.createOrReplaceTempView("meter")
    val q1 = "SELECT * FROM meter WHERE temp >= 9.0 AND temp < 12.0"
    val q2 = "SELECT vid,city FROM meter WHERE temp >= 9.0 AND temp < 12.0"
    val q3 = "SELECT city, avg(temp) as avg_temp FROM meter WHERE temp >= 9.0 AND temp < 12.0 GROUP BY city"

    val q1DF = spark.sql(q1)
    val q2DF = spark.sql(q2)
    val q3DF = spark.sql(q3)

    q1DF.explain(mode = "extended")
    q2DF.explain(mode = "extended")
    q3DF.explain(mode = "extended")

    q1DF.write.csv(outputPath + "/example1.csv")
    q2DF.write.csv(outputPath + "/example2.csv")
    q3DF.write.csv(outputPath + "/example3.csv")
    spark.stop()
  }
}
