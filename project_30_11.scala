import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object project_30_11 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("dup_delete_app")
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

    //spark.sql(" insert into new_table select ccn, state, provider_name from (select tab.*, row_number() over (partition by ccn order by ccn) rnk from HHC_SOCRATA_PRVDR tab ) where rnk = 1 ")
    
  }
}
