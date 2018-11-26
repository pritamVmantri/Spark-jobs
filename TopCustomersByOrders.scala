package com.oswt.bdata.salesanalytics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

/**
  * Created by srinivas on 9/17/2017
  */
object TopCustomersByOrders {

  case class CustomerOrders(id: String,
                            name: String,
                            postal_code: String,
                            state: String,
                            orders_count: Long,
                            total_amount: Double)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Top N customers by total amount of business")

    Logger.getLogger(this.toString).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    conf.setMaster("local")
    System.setProperty("hadoop.home.dir", "C:\\Users\\srinivas\\Dropbox\\work\\code\\practice\\oswt\\")


    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate

    import spark.implicits._

    val ordersDf = Seq(
      CustomerOrders("001", "cust01", "500032", "TS", 5, 616),
      CustomerOrders("002", "cust02", "500409", "TS", 74, 172),
      CustomerOrders("003", "cust03", "500032", "TS", 93, 65.9),
      CustomerOrders("004", "cust04", "500081", "TS", 5, 75.9),
      CustomerOrders("006", "cust06", "500101", "TS", 5, 67.9),
      CustomerOrders("007", "cust07", "500409", "TS", 9, 654.9),
      CustomerOrders("008", "cust08", "500082", "TS", 5, 716),
      CustomerOrders("010", "cust10", "500002", "TS", 74, 202),
      CustomerOrders("011", "cust11", "500101", "TS", 53, 45.9),
      CustomerOrders("013", "cust13", "500409", "TS", 9, 637),
      CustomerOrders("014", "cust14", "500082", "TS", 74, 202),
      CustomerOrders("015", "cust15", "500032", "TS", 53, 469),
      CustomerOrders("016", "cust16", "500101", "TS", 74, 302),
      CustomerOrders("017", "cust17", "500032", "TS", 53, 64.9)
    ).toDF()

    ordersDf.show()

    ordersDf.orderBy($"postal_code", $"total_amount".desc, $"orders_count".desc)
      .show()


    /**
      * id,   name, postal_code, state, orders_count, total_amount
      * SELECT id,   name, postal_code, state, orders_count, total_amount,
      * ROW_NUM OVER(PARTITION BY postal_code) ORDER BY(total_amount DESC, orders_count DESC) as rank
      */


    /*
        ordersDf.createOrReplaceGlobalTempView("order_customers")

        spark.sql(
          """
            |SELECT id,
            | name,
            | postal_code,
            | state,
            | orders_count,
            | total_amount,
            | ROW_NUMBER() OVER(PARTITION BY postal_code
            |   ORDER BY total_amount DESC, orders_count DESC) as rank
            |FROM order_customers
          """.stripMargin)
    */

    import org.apache.spark.sql.functions._

    val w = Window.partitionBy($"postal_code")
      .orderBy($"total_amount".desc, $"orders_count".desc)
    //.orderBy($"orders_count".desc)

    ordersDf.withColumn("rnk", row_number().over(w))
      .where($"rnk" <= 2)
      .drop($"rnk")
      .show()


  }
}
