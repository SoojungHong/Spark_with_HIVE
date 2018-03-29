package util

import org.apache.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object PostgresConnector {
  def main(args: Array[String]): Unit = {
    println("...................[DEBUG] Postgres Connector Testing....")

    //---------------
    // Spark Context
    //---------------
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")
    val conf = new SparkConf().setAppName("Postgres Application").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext: SQLContext = new SQLContext(sc)
    val df_records = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://192.168.21.16/rubcom")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "dev_inthub_meta_unity_spark.meta_pea_account_subtype")//"public.records")
      .option("user", "rubcom")
      .option("password", "2009Sent").load().select("scd_is_active")

    df_records.printSchema()
    df_records.show()
  }
}
