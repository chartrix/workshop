package com.mx.datio

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Launcher extends App {


    val sparkConf = new SparkConf().setAppName("ALS with ML Pipeline")
    val spark = SparkSession
      .builder()
      .appName("ExampleKaos")
      .config(sparkConf)
      .getOrCreate()



    val crimes = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/chartrix/crimes/CrimesSample.csv")
    import spark.implicits._
    val crimesE1 = crimes.map( row => (row.getString(0),row.getString(0).length)   ).select(col("_1").alias("ID"),col("_2").alias("length"))


     crimesE1.write.mode("overwrite").parquet("hdfs://localhost:9000/user/chartrix/crimes/parquet2/")



}
