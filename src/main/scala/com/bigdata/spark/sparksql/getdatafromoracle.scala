package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object getdatafromoracle {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getdatafromoracle").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql


    val ourl ="jdbc:oracle:thin:@//sqooppoc.cjxashekxznm.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop = new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val odf = spark.read.jdbc(ourl,"EMP",oprop) // table name DEPT is case sensitive
    odf.show()
    // if u get  java.lang.ClassNotFoundException: oracle.jdbc.OracleDriver error pls add oracle ojdbc7.jar as well
    odf.createOrReplaceTempView("e")
    odf.createOrReplaceTempView("d")
    val res = spark.sql("select e.*,d.loc, d.dname from e join d on e.deptno=d.deptno")
    res.show()
    spark.stop()
  }
}