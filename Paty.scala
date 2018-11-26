package com.oswt.analytics

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by srinivas on 9/12/2017
  */
object Paty {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("hive.exec.dynamic.partition", "true")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    conf.set("spark.sql.parquet.filterPushdown", "false")
    conf.set("spark.shuffle.spill.compress", "true")
    conf.set("spark.shuffle.compress", "true")

    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new HiveContext(sc)

    val df = sqlContext.read.table("")

    df.write
      .partitionBy("") //column name for partition
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .saveAsTable("") //target table name
  }
}


