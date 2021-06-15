package org.openinfralabs.caerus.cache.examples.spark.gridpocket

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, lag, lit, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

import java.util.Calendar

/**
 * Query 5 in Spark for GridPocket dataset. Obtain consumption for a specific subset of France consumption.
 * @param spark          Spark session used to execute the query.
 * @param schemaProvider Schema provider in Spark for GridPocket dataset.
 */
case class Q05(spark: SparkSession, schemaProvider: GridPocketSchemaProvider) extends GridPocketQuery {
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
    cal.add(Calendar.DAY_OF_MONTH, -1)
    val minDate: String = "%04d-%02d-%02d 00:00:00".format(
      cal.get(Calendar.YEAR),
      cal.get(Calendar.MONTH) + 1,
      cal.get(Calendar.DAY_OF_MONTH)
    )
    cal.add(Calendar.DAY_OF_MONTH, 1)
    cal.add(Calendar.MONTH, 1)
    val maxDate: String = "%04d-%02d-01 00:00:00".format(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1)
    cal.add(Calendar.MONTH, -1)
    val curMonth: String = "%04d-%02d".format(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1)
    val windowSpec: WindowSpec = Window.partitionBy("region").orderBy("day")
    val prevIndex: Column = lag("index",1) over windowSpec
    val curIndex: Column = col("index")
    loadDF
      .filter(
        col("date") >= lit(minDate) and
          col("date") < lit(maxDate) and
          col("region") >= 70 and
          col("region") < 80
      )
      .groupBy(functions.substring(col("date"), pos=0, len=10).as("day"), col("region"))
      .agg(functions.max(col("index")).as("index"))
      .withColumn("cons", when(prevIndex.isNull, curIndex).otherwise(curIndex-prevIndex))
      .filter(functions.substring(col("day"), pos=0, len=7) === curMonth)
      .select(col("day"), col("region"), col("cons"))
      .orderBy(col("day"),col("region"))
  }
}
