package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object sparkProgSpeSchema {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkProgSpeSchema").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\bigdata\\dataset\\bank-full.csv"
    val brdd = sc.textFile(data)

    //data cleaning & make structure
    val skip = brdd.first()
    val fields = skip.replaceAll("\"","").split(";").map(x => StructField(x, StringType, nullable = true))
    val schema = StructType(fields)
    //val userSchema = new StructType().add("age", "integer").add("job", "string").add("marital", "string").add("education", "string")

    val rawdata = brdd.filter(x=>x!=skip).map(x=>x.replaceAll("\"","")).map(x=>x.split(";")).map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16)))
val df = spark.createDataFrame(rawdata,schema)
    df.show()
    spark.stop()
  }
}