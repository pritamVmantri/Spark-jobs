package com.oswt.analytics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object FlatOrders {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Flatten Orders table")

    var ordersPath = "hdfs://192.168.30.130:8020/user/cloudera/demo/retail_db/orders"
    var customersPath = "hdfs://192.168.30.130:8020/user/cloudera/demo/retail_db/customers"
    var targetLocaton = "hdfs://192.168.30.130:8020/user/cloudera/demo/retail_dl/orders"

    if (args.length < 1) {

      Logger.getLogger(FlatOrders.toString).getLevel
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      conf.setMaster("local")
      System.setProperty("hadoop.home.dir", "C:\\Users\\srinivas\\Dropbox\\Kogentix Team Folder\\code\\practice\\oswt\\")
    } else {
      ordersPath = args(0)
      customersPath = args(1)
      targetLocaton = args(2)
    }
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val ordersDf = sqlContext.read
      .format("parquet")
      .parquet(ordersPath)

    val customersDf = sqlContext.read
      .format("parquet")
      .parquet(customersPath)

    //register dataframe as table to use in SQL way
    ordersDf.registerTempTable("orders")
    customersDf.registerTempTable("customers")

    //Do the join and pick the needed columns
    val flattenDf = sqlContext.sql(
      """
        |SELECT
        | o.order_id,
        | o.order_date,
        | o.order_status,
        | c.customer_id,
        | c.customer_fname,
        | c.customer_lname,
        | c.customer_email,
        | c.customer_password,
        | c.customer_street,
        | c.customer_city,
        | c.customer_state,
        | c.customer_zipcode
        |FROM
        |   orders o
        |LEFT JOIN customers c
        |   ON o.order_customer_id = c.customer_id
      """.stripMargin)

    //for testing in IDE
    //    flattenDf.show(10)

    //write dataframe to hdfs location
    flattenDf.write
      .format("parquet")
      .parquet(targetLocaton)
  }
}
