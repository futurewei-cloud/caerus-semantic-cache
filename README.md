# Caerus Semantic Cache
Semantic Cache is a shared semantic cache that utilized semantic information of cached contents to make optimization of Big Data jobs easier and more efficient.

## Prerequisites (Ubuntu)
Java 11 (https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)

Maven 3.6.3 (https://maven.apache.org/docs/3.6.3/release-notes.html)

Apache Hadoop 2.9.2 or higher, 3.x.x not tested (https://hadoop.apache.org/)

Apache Spark 3.0.x (https://spark.apache.org/)

Scala 2.12.10 (https://www.scala-lang.org/download/2.12.10.html)

## Setup
1. Install java and set environment variable JAVA_HOME.
2. Install Maven and set environment variable MAVEN_HOME (or include mvn bin folder in PATH).
3. Install Hadoop and setup HDFS cluster.
4. Install Spark and setup Spark cluster.
5. Install Scala and setup SCALA_HOME (or include scala bin folder in PATH).
6. Compile Semantic Cache by running the following at root directory:
```
${MAVEN_HOME}/bin/mvn clean package
```
7. Configure Semantic Cache Manager with the correct parameters using the following templates:
```
${CAERUS_HOME}/manager/src/main/resources/{manager.conf,log4j.properties}.template
```
8. Run Semantic Cache Manager by runnning:
```
${SCALA_HOME}/bin/scala -Dlog4j.configuration=<log4j.properties path> ${CAERUS_HOME}/manager/target/manager-0.0.0-jar-with-dependencies.jar <manager.conf path>
```
9. Use the following option along with Spark when you run a Scala application to have access to Semantic Cache API:
```
--driver-class-path ${CAERUS_HOME}/client.spark/target/client.spark-0.0.0-jar-with-dependencies.jar
```

## Semantic Cache Read API
Simply use the following in the Scala Spark application code:
```scala
SemanticCache.activate(sparkSession, semanticCacheManagerURI)
```

## Semantic Cache Write API
Create new semantic cache client:
```scala
val semanticCache = new SemanticCache(sparkSession, semanticCacheManagerURI)
```

Write to cache:
```scala
val bytesWritten = semanticCache.repartitioning(loadDataFrame, partitionAttribute, tier, name)
val bytesWritten = semanticCache.fileSkippingIndexing(loadDataFrame, partitionAttribute, tier, name)
val bytesWritten = semanticCache.caching(dataframe, tier, name)
```

Delete from cache:
```scala
val bytesFreed = semanticCache.delete(name)
```

Get current status of the semantic cache:
```scala
Console.out.println(semanticCache.status)
```

## Semantic Cache Examples
To run a simple user example using GridPocket data (https://github.com/gridpocket/project-iostack), use the following:
```
// Initialize Spark.
val spark: SparkSession = SparkSession.builder()
  .master(sparkURI)
  .appName(name="SemanticCacheExample")
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
```

To fill the cache with contents, the Write API should be used. Here is an example of performing repartitioning on GridPocket data on the attribute of temperature:
```
// Initialize Spark.
val spark: SparkSession = SparkSession.builder()
  .master(sparkURI)
  .appName(name="SemanticCacheRepartitioning")
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

// Cache repartition content.
val bytesWritten = semanticCache.repartitioning(loadDF, partitionAttribute=attributeName, Tier.STORAGE_DISK, name=repartitionName)
assert(bytesWritten > 0L)
Console.out.println(semanticCache.status)
spark.stop()
```

After the repartitioning content is created, the initial leaf node changes in the Optimized Logical Plan for all three queries from
```
+- Relation[vid#0,date#1,index#2,sumHC#3,sumHP#4,type#5,size#6,temp#7,city#8,region#9,lat#10,lng#11] csv
```
to
```
+- Relation[vid#0,date#1,index#2,sumHC#3,sumHP#4,type#5,size#6,temp#7,city#8,region#9,lat#10,lng#11] parquet
```
revealing changes in the optimization process from Semantic Cache. The ensuing Physical Plan reveals even further information (files that are loaded) if printed.
