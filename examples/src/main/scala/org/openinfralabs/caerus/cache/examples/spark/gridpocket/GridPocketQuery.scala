package org.openinfralabs.caerus.cache.examples.spark.gridpocket

import org.apache.spark.sql.SparkSession
import org.openinfralabs.caerus.cache.examples.spark.Query

/**
 * Parent class for GridPocket trace.
 */
abstract class GridPocketQuery extends Query {}