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

    //read column example using select()
    val sqlContext: SQLContext = new SQLContext(sc)
    val df_records = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://192.168.21.16/rubcom")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "dev_inthub_meta_unity_spark.meta_pea_account_subtype")//"public.records")
      .option("user", "rubcom")
      .option("password", "2009Sent").load().select("meta_past_id")

    df_records.printSchema()
    df_records.show()

    //read whole table using load()
    val df = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://192.168.21.16/rubcom")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "dev_inthub_meta_unity_spark.meta_pea_account_subtype")
      .option("user", "rubcom")
      .option("password", "2009Sent").load()

    df.show()

    //read only 'meta_past_id', 'meta_pat_id', 'meta_past_halflife', 'scd_is_active', 'scd_is_deleted'
    val selectedDF = df.select(df.col("meta_past_id"), df.col("meta_pat_id"), df.col("meta_past_halflife"), df.col("scd_is_active"), df.col("scd_is_deleted"))
    selectedDF.show()

    //join two dataframes
    val df_l = df_records.as("left_table")
    val df_r = df.as("right_table")
    val joined_table = df_l.join(df_r, df_l.col("meta_past_id") === df_r.col("meta_past_id"), "inner")
    joined_table.show()

    //joined_table.select(joined_table.col())

  }
}
