package org.openinfralabs.caerus.cache.client

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.{DateTime, DateTimeZone}
import org.openinfralabs.caerus.cache.common.plans.CaerusPlan

case class BasicRuntimePredictor(spark: SparkSession) extends RuntimePredictor {

  import spark.implicits._

  private var stageTimeStart: Long = 0
  private var schema: StructType = StructType(
    StructField("numPartitions", IntegerType, false) ::
      StructField("outTuples", IntegerType, false) ::
      StructField("stageTime", LongType, false) :: Nil)
  private var training = spark.createDataFrame(spark.sparkContext
    .emptyRDD[Row], schema)

  def extractFeatures(inputPlan: LogicalPlan, caerusPlan: CaerusPlan): Unit = {
    val queryExecution: QueryExecution = new QueryExecution(spark, inputPlan)
    val numPartitions = queryExecution.toRdd.getNumPartitions
    val outTuples = 10 //tbf input from size estimator

    val stageTime = DateTime.now(DateTimeZone.UTC).getMillis() - stageTimeStart
    stageTimeStart = DateTime.now(DateTimeZone.UTC).getMillis()
    training = training.union(Seq((numPartitions, outTuples, stageTime)).toDF())

  }

  def train(): Unit = {
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(5)
      .fit(training)

    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)

    // Chain indexer and GBT in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, gbt))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(training)
    model.write.save("gbtregressortrained.model")
  }

  override def predictRuntime(inputPlan: LogicalPlan, caerusPlan: CaerusPlan): Long = {
    val queryExecution: QueryExecution = new QueryExecution(spark, inputPlan)
    val numPartitions = queryExecution.toRdd.getNumPartitions
    val outTuples = 10 //tbf input from size estimator

    val model = GBTRegressionModel.load("gbtregressortrained.model")
    val prediction = model.transform(Seq(numPartitions, outTuples).toDF())

    prediction.first().getLong(0)
  }

  def featureExtractor: Unit = {

  }
  /*
Cache is empty.
We have initial candidates:
1) Repartitioning(source, "temp")
2) FileSkippingIndexing(source, "temp")
3) Caching(
  -Filter("temp > 90")
  --Source
)
4) Caching(
  -Aggregate(["region"],["count("temp")])
  --Filter("temp > 90")
  ---Source
)
plan
futurePlans (prediction)
optimizedPlan = optimize(plan, contents)
initialTime = estimateRuntime(optimizedPlan) --- Initial plan runtime prediction
For all candidates {
writeTime = estimateRuntime(writePlanFor(i))
optimizedPlan = optimize(plan, contents + {i})
readTime = estimateRuntime(optimizedPlan)
if (readTime + writeTime > initialTime + thresholdTime)
  eliminate candidate i
}
initialTime = futurePlans.map(optimize(_, contents).map(estimateRuntime).sum
For all candidates {
candidateTime(i) = futurePlans.map(optimize(_, contents + {i}).map(estimateRuntime).sum + writeTime
}
candidates.filter(initialTime - candidateTime(i) > 0)
Pick one with max score (if there is any)
add contents


 */


}
