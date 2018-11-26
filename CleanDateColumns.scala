package com.oswt.bdata.salesanalytics

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf


/**
  * Created by Srinivas on 9/3/2017
  */
object CleanDateColumns {

  val date_transform = udf((date: String) => {

    if (date.contains("/")) {
      val dtFormatter = DateTimeFormatter.ofPattern("d/M/y")
      val dt = LocalDate.parse(date, dtFormatter)

      "%2d-%2d-%s"
        .format(dt.getDayOfMonth,
          dt.getMonthValue,
          dt.getYear.toString.substring(2))
        .replaceAll(" ", "0")
    } else {
      date
    }
  })

  def createSession(conf: SparkConf): SparkSession = {
    SparkSession.builder
      .config(conf)
      .getOrCreate
  }

  def setUpLocalConf(conf: SparkConf) = {
    Logger.getLogger(CleanDateColumns.toString).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    conf.setMaster("local")
    System.setProperty("hadoop.home.dir", "C:\\Users\\srinivas\\Dropbox\\work\\code\\practice\\oswt\\")
  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.setAppName("Clean the Date columns")

    var input: String = "..\\data\\customer.csv"
    var output: String = "./io/cleaned/customers_" + System.currentTimeMillis()

    if (args.length < 1) {
      setUpLocalConf(conf)
    } else {
      input = args(0)
      output = args(1)
    }

    val spark: SparkSession = createSession(conf)

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load(input)

    /**
      * 15-03-05 DD-MM-YY
      * 12/3/2005 DD/MM/YYYY
      * Target => DD-MM-YY
      */

    //LAST_UPDATE_DATE CREATION_DATE

    //    df.withColumn("LAST_UPDATE_DATE", date_transform($"LAST_UPDATE_DATE"))
    //      .withColumn("CREATION_DATE", date_transform($"CREATION_DATE"))
    //      .withColumn("CREATION_DATE", $"CREATION_DATE".cast(DataTypes.StringType))
    //      .write
    //      .format("json")
    //      .save(output)

    //
    //    import spark.implicits._
    //    df.withColumn("CUST_ID", $"CUST_ACCOUNT_ID")
    //      .withColumn("ZIP", $"POSTAL_CODE")
    //      .select("CUST_ID", "CITY", "STATE", "ZIP")
    //      .limit(5)
    //      .write
    //      .format("json")
    //      .save(output)
    //
    //
    //


    val jsonDf = spark.read
      .format("json")
      .load("./io/cleaned/sample.json")

    jsonDf.show()


    jsonDf.write
      .partitionBy("ZIP")
      .save("s3/bucket/location/to/save")
  }
}
