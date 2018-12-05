import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object project_30_11 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local")
      .appName("dup_delete_app")
      .config("javax.jdo.option.ConnectionDriverName","com.mysql.jdbc.Driver")
      .config("javax.jdo.option.ConnectionURL","jdbc:mysql://localhost:3306/metastore_db?createDatabaseIfNotExist=true")
      .config("javax.jdo.option.ConnectionUserName","hadoop")
      .config("javax.jdo.option.ConnectionPassword","hadoop")
      .config("hive.exec.scratchdir","/tmp/hive/${user.name}")
      .config("spark.sql.warehouse.dir", "/home/hadoop/spark_warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val outputDir = "hdfs://localhost:8020/user/hadoop/project_3011"

    spark.sql("use home_health_care")

    val dup_file = spark.sql(" select ccn, state, provider_name from (select tab.*, row_number() over (partition by ccn order by ccn) rnk from HHC_SOCRATA_PRVDR tab ) where rnk > 1 ")

    val currentTime = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmssSSS").format(LocalDateTime.now)

    dup_file.coalesce(1 )
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save( outputDir + "/" + "duplicate_records" + "_" + currentTime)

    spark.sql(" insert into new_table select ccn, state, provider_name from (select tab.*, row_number() over (partition by ccn order by ccn) rnk from HHC_SOCRATA_PRVDR tab ) where rnk = 1 ")

    // Checking nulls in unique id column

    val count_ccn = spark.sql("select count(ccn) c_ccn from hhc_socrata_prvdr")

    val count_table = spark.sql("select count(*) c_table from hhc_socrata_prvdr")

    if ( count_ccn.collect()(0).getLong(0) == count_table.collect()(0).getLong(0) ) {
      println("No null value found in ccn column")
    } else {
        println("Null value found in ccn column")
        println("Exiting from spark..")
        return
    }
    spark.stop()
  }
}
