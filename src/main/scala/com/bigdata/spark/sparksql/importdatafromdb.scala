package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object importdatafromdb {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("importdatafromdb").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql

    val url ="jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb"
    val prop = new java.util.Properties()
    prop.setProperty("user","msuername")
    prop.setProperty("password","mspassword")
    prop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val df = spark.read.jdbc(url,"DEPT",prop) // table name DEPT is case sensitive
    df.show()
    spark.stop()
  }
}