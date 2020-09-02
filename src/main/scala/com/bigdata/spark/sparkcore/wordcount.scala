package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object wordcount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  //  val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val sc = spark.sparkContext
    val data = "C:\\bigdata\\dataset\\logdata.txt"
    val wrdd = sc.textFile(data)
    val res = wrdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)

    res.take(20).foreach(println)
    spark.stop()

  }
}