package com.oswt.bdata.salesanalytics

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by srinivas on 9/26/2017
  */
object Neo4jReader {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load("hbase-job.properties")

    println(config.getString("app.name"))

    val conf = new SparkConf()
      .setAppName(config.getString("app.name"))
      .setMaster(config.getString("master"))
      .set("dbms.security.auth_enabled", "false")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.neo4j.bolt.url", "bolt://neo4j:admin@localhost/graph.db")
    //  .set("spark.neo4j.bolt.user","neo4j")
    //  .set("spark.neo4j.bolt.password","admin")

    Logger.getLogger(Neo4jReader.toString).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    System.setProperty("hadoop.home.dir", config.getString("hadoop.home"))

    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate

    import org.neo4j.spark._

    val neo = Neo4j(spark.sparkContext)

    // load via Cypher query
    neo.cypher("MATCH (n:Person) RETURN id(n) as id SKIP {_skip} LIMIT {_limit}")
      .partitions(4)
      .batch(25)
      .loadDataFrame.count
    //   => res36: Long = 100

    val df = neo.pattern("Person", Seq("KNOWS"), "Person")
      .partitions(12)
      .batch(100)
      .loadDataFrame

    df.show()
  }

}
