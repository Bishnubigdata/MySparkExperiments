package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object rddtoDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rddtoDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\bigdata\\dataset\\bank-full.csv"
    val brdd = sc.textFile(data)

    //data cleaning & make structure
    val skip = brdd.first()

    val clean = brdd.filter(x=>x!=skip).map(x=>x.replaceAll("\"","")).map(x=>x.split(";")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16)))
//structured rdd convert to dataframe.
    //in this scenario must use     import spark.implicits._ ... it convert rdd to dataframe dataframe to dataset etc..

    //toDF() method used convert rdd to dataframe. and rename all columns
  //  val df = clean.toDF("age","job","marital","education","default1","balance","housing","loan","contact","day","month","duration","campaign","pdays","previous","poutcome","y")
 //   df.createOrReplaceTempView("my")
    //val res = spark.sql("select marital, count(*) cnt from my group by marital order by cnt desc")
  //  val res = spark.sql("select * from my where balance>90000")
   // df.show()
  //  res.show()
   // clean.take(10).foreach(println)

    spark.stop()
  }
}