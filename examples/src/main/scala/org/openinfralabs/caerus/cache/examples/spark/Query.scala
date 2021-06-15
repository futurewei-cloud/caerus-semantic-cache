package org.openinfralabs.caerus.cache.examples.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Parent class for queries in Spark.
 */
abstract class Query {
  /**
   * Get name of query in Spark.
   * @return corresponding name of query class in Spark
   */
  def getName: String = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")
  def execute(args: String*): DataFrame
}
