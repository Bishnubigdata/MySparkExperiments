package com.bigdata.spark.sparkstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._


object sparkstreamingwh {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkstreamingwh").getOrCreate()
       val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext
    val lines=ssc.socketTextStream(hostname="18.219.148.199",port=1234)
    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}