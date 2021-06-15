package org.openinfralabs.caerus.cache.examples.spark

import org.apache.spark.sql.types.StructType

/**
 * Parent class for providing schema information in Spark.
 */
abstract class SchemaProvider {
  /**
   * Get schema information for a specific dataset in Spark.
   * @return corresponding schema in Spark
   */
  def getSchema: StructType
}
