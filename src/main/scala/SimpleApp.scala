import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import scala.math._

object SimpleApp {

  def main(args: Array[String]) {

    val isTest = true
    if(isTest != true) {
      val conf = new SparkConf().setAppName("Simple Application")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
      import sqlContext.implicits._

      val readStart = System.nanoTime()
      // The results of SQL queries are themselves DataFrames and support all normal functions.
      val sqlDF = sqlContext.sql("SELECT * FROM post39")
      val readEnd = System.nanoTime()
      println("============= Elapsed Time : " + "Reading " + sqlDF.count() + " took " + (readEnd - readStart)/ 1000000000 + " in second")
      sqlDF.show()

      import org.apache.spark.sql.functions.udf
      val myScoreFunc = udf(scoreFunction _)
      val cookedPeasDF = sqlDF.withColumn("y", myScoreFunc(sqlDF.col("x1")))

      val writeStart = System.nanoTime()
      cookedPeasDF.select(cookedPeasDF.col("x1"), cookedPeasDF.col("y")).write.mode("overwrite").saveAsTable("peas");
      val writeEnd = System.nanoTime() //============= Elapsed Time : Writing 20000000 took 13 in second
      println("============= Elapsed Time : " + "Writing " + cookedPeasDF.count() + " took " + (writeEnd - writeStart)/ 1000000000 + " in second")

      val df = sqlContext.sql("SELECT * FROM peas")
      df.show()

    } else {
        createAndTestLocalDataFrame()
    }

  }

  def scoreFunction (x : Int) : Double = {
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

  def createAndTestLocalDataFrame(): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")

    val conf = new SparkConf().setAppName("Local Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val df = Seq((12, 23), (888, 44), (2, 6), (19, 233), (98, 100)).toDF("x1", "x2")
    df.show()

    import org.apache.spark.sql.functions.udf
    val myFunc = udf(simpleFunc _)

    import org.apache.spark.sql.functions.udf
    val myScoreFunc = udf(scoreFunction _)

    val newDF = df.withColumn("y", myScoreFunc(df.col("x1")))
    newDF.show()
  }

  def simpleFunc(x:Int) : Double = {
    val y = x + 10
    return (y)
  }

  //ToDo
  def createDataFrameFromCsv(): Unit ={

  }

}



