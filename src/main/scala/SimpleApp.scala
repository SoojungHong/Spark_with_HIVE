import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.functions._

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

      import sqlContext.implicits._

      //-------------------------------------------------
      // Read raw peas in Hive table 'pea_account_raw'
      //-------------------------------------------------
      val peaAccountRawDF= sqlContext.sql("SELECT * FROM pea_account_raw")
      println("=============================== DEBUG (SELECT * FROM pea_account_raw")
      peaAccountRawDF.show()

      val selectedCols = peaAccountRawDF.select(peaAccountRawDF.col("meta_past_id"), peaAccountRawDF.col("technical_value"), peaAccountRawDF.col("cust_relevance") )
      println("=============================== DEBUG select columns")
      selectedCols.show()

      //val new_peaAccountRawDF = peaAccountRawDF.withColumn("score", lit("100"))
      //println("=============================== DEBUG show new_peaAccountRawDF")
      //new_peaAccountRawDF.show()

      //following could not parsed & insert statement for HIVE is too slow
      //sqlContext.sql("insert into cooked_pea_account1 partition (date='2018-01-01',hour=02) values (228918879,92017266,3445801,13671760,'481C4CC6B7A','CUSTOMER','2018-01-01 00:57:25','2018-03-15 13:33:06',2,100.0,112354305)")

      //val hiveDF = new_peaAccountRawDF.select(new_peaAccountRawDF.col("party_id"), new_peaAccountRawDF.col("building_port_id"), new_peaAccountRawDF.col("cpe_equipment_id"), new_peaAccountRawDF.col("cpe_port_id"), new_peaAccountRawDF.col("cpe_mac"), new_peaAccountRawDF.col("trigger"), new_peaAccountRawDF.col("ts_created"), new_peaAccountRawDF.col("ts_received"), new_peaAccountRawDF.col("meta_past_id"), new_peaAccountRawDF.col("score"), new_peaAccountRawDF.col("root_id"), new_peaAccountRawDF.col("date"), new_peaAccountRawDF.col("hour"))
      val hiveDF = calcScoreValue(peaAccountRawDF)
      hiveDF.write.mode(SaveMode.Append).partitionBy("date", "hour").saveAsTable("cooked_pea_account")

      println("=============================== DEBUG SELECT * FROM cooked_pea_account ===================")
      val appendCookedPea = sqlContext.sql("SELECT * FROM cooked_pea_account")
      appendCookedPea.show()

    } else {
        createAndTestLocalDataFrame()
    }

  }

  def calcScoreValue(peaAccountRawDF : DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.udf
    val stScoreFunc = udf(speedTestScoreFunction _)
    val new_peaAccountRawDF = peaAccountRawDF.withColumn("score", stScoreFunc(peaAccountRawDF.col("technical_value") * peaAccountRawDF.col("cust_relevance")))
    val simplifiedCookedPeasDF = new_peaAccountRawDF.select(new_peaAccountRawDF.col("party_id"), new_peaAccountRawDF.col("building_port_id"), new_peaAccountRawDF.col("cpe_equipment_id"), new_peaAccountRawDF.col("cpe_port_id"), new_peaAccountRawDF.col("cpe_mac"), new_peaAccountRawDF.col("trigger"), new_peaAccountRawDF.col("ts_created"), new_peaAccountRawDF.col("ts_received"), new_peaAccountRawDF.col("meta_past_id"), new_peaAccountRawDF.col("score"), new_peaAccountRawDF.col("root_id"), new_peaAccountRawDF.col("date"), new_peaAccountRawDF.col("hour"))

    simplifiedCookedPeasDF
  }

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

  def speedTestScoreFunction (x : Int) : Double = {
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



