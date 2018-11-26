package com.oswt.analytics.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ParquetViewer {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("First Spark app with Scala")

    var input = "hdfs://192.168.30.130:8020/user/cloudera/demo/retail_dl/orders/"

    if (args.length < 1) {
      Logger.getLogger(ParquetViewer.toString).getLevel
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      conf.setMaster("local")
      System.setProperty("hadoop.home.dir", "C:\\Users\\srinivas\\Dropbox\\Kogentix Team Folder\\code\\practice\\oswt\\")
    } else {
      input = args(0)
      println(args.toList)
    }

    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)


    val df = sqlContext.read
      .format("parquet")
      .parquet(input)

    df.limit(5)
      .show()
  }
}
