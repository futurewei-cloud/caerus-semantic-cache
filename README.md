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
7. Configure Semantic Cache Manager with the correct parameters using the following template:
```
${CAERUS_HOME}/manager/src/main/resources/manager.conf.template
```
8. Run Semantic Cache Manager by runnning:
```
${SCALA_HOME}/bin/scala ${CAERUS_HOME}/manager/target/manager-0.0.0-jar-with-dependencies.jar <manager.conf>
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
To be filled soon.
