package org.openinfralabs.caerus.cache.examples.spark.gridpocket

import org.apache.spark.sql.types._
import org.openinfralabs.caerus.cache.examples.spark.SchemaProvider

/**
 * Provides schema information for GridPocket queries in Spark.
 */
class GridPocketSchemaProvider extends SchemaProvider {
  /**
   * Function that calculates Spark schema for GridPocket queries.
   * @return corresponding schema information for Spark (StructType)
   */
  def getSchema: StructType = StructType(Array(
      StructField("vid", StringType, nullable=true),
      StructField("date", TimestampType, nullable=true),
      StructField("index", DoubleType, nullable=true),
      StructField("sumHC", DoubleType, nullable=true),
      StructField("sumHP", DoubleType, nullable=true),
      StructField("type", StringType, nullable=true),
      StructField("size", IntegerType, nullable=true),
      StructField("temp", DoubleType, nullable=true),
      StructField("city", StringType, nullable=true),
      StructField("region", StringType, nullable=true),
      StructField("lat", DoubleType, nullable=true),
      StructField("lng", DoubleType, nullable=true)
  ))
}