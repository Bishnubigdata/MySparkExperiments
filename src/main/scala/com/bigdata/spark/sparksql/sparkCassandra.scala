package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.cassandra._
object sparkCassandra {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkCassandra").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val edf= spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","bishnuks").option("table","emp").load()
    val adf= spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","bishnuks").option("table","asl").load()
    edf.createOrReplaceTempView("T1")
    adf.createOrReplaceTempView("T2")
    val res = spark.sql("select T1.*, T2.* from T1 join T2 on T1.first_name=T2.name").drop("first_name")
    res.show()
    // before export data to cassandra first create cassandra table in advanced
   // spark.sql("create table jointab(empid int, deptid int, last_name varchar, name varchar, city varchar, id int);")
    val qry = """CREATE TABLE bishnuks.tab (empid int, deptid int, last_name string, name string, city string, id int)
                 USING cassandra
                  TBLPROPERTIES (
                    compaction='{class=SizeTieredCompactionStrategy,bucket_high=1001}'
                  )"""
    spark.sql(qry)
    res.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("keyspace","bishnuks").option("table","tab").save()
    spark.stop()
  }
}