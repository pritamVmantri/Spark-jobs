package com.oswt.bdata.salesanalytics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by srinivas on 9/2/2017
  */
object TopNAreasByOrgs {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Top N areas by organization count")

    var customersFilePath = "..\\data\\customer.csv"

    if (args.length < 1) {

      Logger.getLogger(TopNAreasByOrgs.toString).getLevel
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      conf.setMaster("local")
      System.setProperty("hadoop.home.dir", "C:\\Users\\srinivas\\Dropbox\\work\\code\\practice\\oswt\\")
    } else {
      customersFilePath = args(0)
    }


    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate

    import spark.implicits._

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load(customersFilePath)

    /**
      * SELECT POSTAL_CODE, COUNT(PARTY_TYPE) AS ORG_COUNT
      * WHERE PARTY_TYPE = "ORGANIZATION"
      * GROUP BY POSTAL_CODE
      * ORDER BY ORG_COUNT DESC
      */


    val resultDf = df.where($"PARTY_TYPE" === lit("ORGANIZATION") && $"POSTAL_CODE".isNotNull)
      .groupBy("POSTAL_CODE")
      .agg(count("PARTY_TYPE").as("ORG_COUNT"))
      .orderBy(desc("ORG_COUNT"))

    resultDf.show()
  }

}
