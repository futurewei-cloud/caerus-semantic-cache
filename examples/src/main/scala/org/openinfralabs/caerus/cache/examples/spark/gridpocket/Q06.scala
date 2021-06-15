package org.openinfralabs.caerus.cache.examples.spark.gridpocket

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.util.Calendar

/**
 * Query 6 in Spark for GridPocket dataset. Obtain data for drawing peak versus shallow hour consumption.
 * @param spark          Spark session used to execute the query.
 * @param schemaProvider Schema provider in Spark for GridPocket dataset.
 */
case class Q06(spark: SparkSession, schemaProvider: GridPocketSchemaProvider) extends GridPocketQuery {
  override def execute(args: String*): DataFrame = {
    // Take arguments.
    if (args.length != 3) {
      throw new RuntimeException("Wrong number of arguments for %s. Year, month, and input path should be provided.")
    }
    val year: Int = args(0).toInt
    val month: Int = args(1).toInt
    val inputPath: String = args(2)

    // Initialize data schema.
    val schema: StructType = schemaProvider.getSchema

    // Create leaf node.
    val loadDF = spark.read.schema(schema).option("header", value=true).csv(inputPath)

    // Construct query.
    val cal: Calendar = Calendar.getInstance()
    cal.set(year, month-1, 1, 0, 0, 0)
    val minDate: String = "%04d-%02d-01 00:00:00".format(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1)
    cal.add(Calendar.MONTH, 1)
    val maxDate: String = "%04d-%02d-01 00:00:00".format(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1)
    loadDF
      .filter(col("date") >= lit(minDate) and col("date") < lit(maxDate) and col("region") === 75)
      .groupBy(functions.substring(col("date"), pos=0, len=10).as("day"), col("vid"))
      .agg(
        functions.max(col("sumHC")).as("HC"),
        functions.max(col("sumHP")).as("HP")
      )
      .orderBy(col("day"),col("vid"))
  }
}