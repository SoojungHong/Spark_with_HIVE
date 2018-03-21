import java.math.BigInteger

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.functions._

import com.databricks.spark.csv._

import scala.math._

object SimpleApp {

  def main(args: Array[String]) {

    val isTest = false
    if(isTest != true) {
      //---------------
      // Spark Context
      //---------------
      val conf = new SparkConf().setAppName("Simple Application")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      sqlContext.setConf("hive.exec.max.dynamic.partitions.pernode", "1000")
      sqlContext.setConf("hive.enforce.bucketing", "true")
      sqlContext.setConf("hive.exec.compress.output", "true")
      sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")

      import sqlContext.implicits._

      //-------------------------------------------------
      // Read raw peas in Hive table 'pea_account_raw'
      //-------------------------------------------------
      val peaAccountRawDF= sqlContext.sql("SELECT * FROM pea_account_raw")
      println("....... [DEBUG] (SELECT * FROM pea_account_raw")
      peaAccountRawDF.show()

      val selectedCols = peaAccountRawDF.select(peaAccountRawDF.col("meta_past_id"), peaAccountRawDF.col("technical_value"), peaAccountRawDF.col("cust_relevance") )
      println(".......[DEBUG] select columns")
      selectedCols.show()

      //following could not parsed & insert statement for HIVE is too slow
      //sqlContext.sql("insert into cooked_pea_account1 partition (date='2018-01-01',hour=02) values (228918879,92017266,3445801,13671760,'481C4CC6B7A','CUSTOMER','2018-01-01 00:57:25','2018-03-15 13:33:06',2,100.0,112354305)")

      //val hiveDF = new_peaAccountRawDF.select(new_peaAccountRawDF.col("party_id"), new_peaAccountRawDF.col("building_port_id"), new_peaAccountRawDF.col("cpe_equipment_id"), new_peaAccountRawDF.col("cpe_port_id"), new_peaAccountRawDF.col("cpe_mac"), new_peaAccountRawDF.col("trigger"), new_peaAccountRawDF.col("ts_created"), new_peaAccountRawDF.col("ts_received"), new_peaAccountRawDF.col("meta_past_id"), new_peaAccountRawDF.col("score"), new_peaAccountRawDF.col("root_id"), new_peaAccountRawDF.col("date"), new_peaAccountRawDF.col("hour"))
      val hiveDF = calcScoreValue(peaAccountRawDF)
      //could make partitions and save files in parquet
      //OK - BUT spark table not hive table//hiveDF.write.mode(SaveMode.Overwrite).option("compression", "gzip").partitionBy("date", "hour").saveAsTable("cooked_pea_account") //OK
      hiveDF.write.mode(SaveMode.Overwrite).option("compression", "gzip").partitionBy("date", "hour").insertInto("cooked_pea_account") //OK

      //write as csv
      //hiveDF.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").partitionBy("date", "hour").insertInto("cooked_pea_account") //OK

      println("....... [DEBUG] SELECT * FROM cooked_pea_account")
      val appendCookedPea = sqlContext.sql("SELECT * FROM cooked_pea_account")
      appendCookedPea.show()

    } else {
        createAndTestLocalDataFrame()
    }

  }

  def calcScoreValue(peaAccountRawDF : DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.udf
    val scoreFunc = udf(selectAndCalcScoreFunction _)
    val new_peaAccountRawDF = peaAccountRawDF.withColumn("score", scoreFunc(peaAccountRawDF.col("technical_value"), peaAccountRawDF.col("cust_relevance"), peaAccountRawDF.col("meta_past_id")))
    val simplifiedCookedPeasDF = new_peaAccountRawDF.select(new_peaAccountRawDF.col("party_id"), new_peaAccountRawDF.col("building_port_id"), new_peaAccountRawDF.col("cpe_equipment_id"), new_peaAccountRawDF.col("cpe_port_id"), new_peaAccountRawDF.col("cpe_mac"), new_peaAccountRawDF.col("trigger"), new_peaAccountRawDF.col("ts_created"), new_peaAccountRawDF.col("ts_received"), new_peaAccountRawDF.col("meta_past_id"), new_peaAccountRawDF.col("score"), new_peaAccountRawDF.col("root_id"), new_peaAccountRawDF.col("date"), new_peaAccountRawDF.col("hour"))

    simplifiedCookedPeasDF
  }


  def selectAndCalcScoreFunction(x1 : Double, x2 : Double, x3 : Int) : Double = {
    var score = 0.0
    if(x3 == 2) { //if speedtest is 2
      score = speedTestScoreFunction(x1 * x2)
    } else if(x3 == 1) {
      score = HFCScoreFunction(x1 * x2)
    } //else call default function or pass default value

    score
  }


  def speedTestScoreFunction (x : Double) : Double = {
    val zeroThresh = 80
    val slopePos = 0.1
    val slopeNeg = 0.6
    val maxPos = 5
    val maxNeg = -2
    var y:Double = 0

    val factorPos = maxPos/scala.math.atan(slopePos*(100 - zeroThresh))
    val factorNeg = maxNeg/scala.math.atan(-slopeNeg * (zeroThresh))

    if (x > zeroThresh) {
      y = factorPos * scala.math.atan(slopePos*(x-zeroThresh))
    }
    else {
      y = factorNeg * scala.math.atan(-slopeNeg * (zeroThresh-x))
    }

    return(y)
  }

  def HFCScoreFunction (x : Double) : Double = {
    var score = 0.0

    val slope = 1.2
    val maxPos = 0
    val maxMin = -2

    score = (maxPos - maxMin) * math.exp(-slope * x) - (maxPos - maxMin) + maxPos
    return(score)
  }


/*
  def createAndTestLocalDataFrame(): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")

    val conf = new SparkConf().setAppName("Local Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //create sample dataframe
    val df = Seq((12, 23), (888, 44), (2, 6), (19, 233), (98, 100)).toDF("x1", "x2")
    df.show()

    //apply user defined function
    import org.apache.spark.sql.functions.udf
    val myFunc = udf(simpleFunc _)

    import org.apache.spark.sql.functions.udf
    val myScoreFunc = udf(scoreFunction _)

    //add column y after apply function
    val newDF = df.withColumn("y", myScoreFunc(df.col("x1")))
    newDF.show()
  }
*/


  def readTestHive(): Unit = {
    //---------------
    // Spark Context
    //---------------
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._

    //--------------------------------------
    // Read raw peas in Hive table 'post39'
    //--------------------------------------
    val readStart = System.nanoTime()
    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val rawPeasDF = sqlContext.sql("SELECT * FROM post39")
    val readEnd = System.nanoTime()
    println("============= Elapsed Time : " + "Reading " + rawPeasDF.count() + " took " + (readEnd - readStart)/1000000000 + " in second")
    rawPeasDF.show()

    //--------------------------------------------------
    // Apply Score function to column 'x1' in each pea
    //--------------------------------------------------
    import org.apache.spark.sql.functions.udf
    val myScoreFunc = udf(speedTestScoreFunction _)
    val cookedPeasDF = rawPeasDF.withColumn("y", myScoreFunc(rawPeasDF.col("x1")))

    //-------------------------------------------------------------
    // Write cooked peas using score function in Hive table 'peas'
    //-------------------------------------------------------------
    val writeStart = System.nanoTime()
    cookedPeasDF.select(cookedPeasDF.col("x1"), cookedPeasDF.col("y")).write.mode("overwrite").saveAsTable("peas");
    val writeEnd = System.nanoTime() //============= Elapsed Time : Writing 20000000 took 13 in second
    println("============= Elapsed Time : " + "Writing " + cookedPeasDF.count() + " took " + (writeEnd - writeStart)/1000000000 + " in second")

    val df = sqlContext.sql("SELECT * FROM peas")
    df.show()

  }


  def createAndTestLocalDataFrame(): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")

    val conf = new SparkConf().setAppName("Local Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //create sample dataframe
    val myDF = Seq((12, 23, 34), (888, 44, 22), (2, 6, 33), (19, 233, 11), (98, 100, 3)).toDF("x1", "x2", "x3")
    println("... original dataframe ")
    myDF.show()

    println()
    println("... select two columns ")
    val selectedDF = myDF.select(myDF.col("x1"), myDF.col("x2"))
    selectedDF.show()

    println()
    println("... add new column with column")
    selectedDF.withColumn("newCol", lit(null)).show()

    //apply user defined function
    import org.apache.spark.sql.functions.udf
    val myFunc = udf(simpleFunc _)
    val myScoreFunc = udf(simpleFunc _)

    //add column y after apply function
    println("... update the 'newCol' with value of x1 * x2")
    val newDF = selectedDF.withColumn("newCol", (selectedDF.col("x1") * selectedDF.col("x2"))) //this returns new DataFrame
    newDF.show()

    println("... filter rows with x2 is bigger than 50")
    val filteredDF = selectedDF.filter(selectedDF.col("x2") > 50)
    filteredDF.show()

    //selectedDF.select("x1 * x2")
  }

  def simpleFunc(x:Int) : Double = {
    val y = x + 10
    return (y)
  }

}



