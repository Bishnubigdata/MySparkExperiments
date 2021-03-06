package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._

object sparkstreaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkstreaming").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
   // val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}