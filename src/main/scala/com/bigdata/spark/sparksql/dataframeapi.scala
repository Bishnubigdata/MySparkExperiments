package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object dataframeapi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("dataframeapi").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\dataset\\bank-full.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true")
        .option("delimiter",";").load(data)
    //df.show()
    df.createOrReplaceTempView("my")
    val res = spark.sql("select marital, count(*) cnt from my group by marital order by cnt desc")
    res.show()


    /*
    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("cars.csv")
     */

    spark.stop()
  }
}