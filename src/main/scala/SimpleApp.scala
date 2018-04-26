import java.math.BigInteger

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.functions._
import com.databricks.spark.csv._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataType

import scala.math._

object SimpleApp {

  def main(args: Array[String]) {

    val isTestLocal = false
    if(isTestLocal != true) {
      //---------------
      // Spark Context
      //---------------
      val conf = new SparkConf().setAppName("Simple Application")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      //sqlContext.setConf("hive.exec.max.dynamic.partitions", "2048")
      //sqlContext.setConf("hive.enforce.bucketing", "true")
      //sqlContext.setConf("hive.exec.compress.output", "true")
      //sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")

      import sqlContext.implicits._

      //---------------
      // Parameters
      //---------------
      var hiveTableNameToRead = "pea_account_raw"
      var hiveTableNameToWrite = "pea_account_cooked"
      var hiveMetaTableSubType = "meta_pea_account_subtype"
      var hiveTableNameToReadTable = ""
      var hiveTableNameToWriteTable = ""
      var hiveMetaTableSubTypeTable = ""

      val paramDate = args.apply(0)
      val paramHour = args.apply(1)
      val paramEnvironment = args.apply(2)
      val paramPeaType = args.apply(3)

      //ToDo : Make log(debug) class
      println("............. [DEBUG] params : date = " + paramDate + " ,hour = " + paramHour + " ,env = " + paramEnvironment + " ,peaType = " + paramPeaType)
      if (isArgEmpty(paramDate) || isArgEmpty(paramHour) || isArgEmpty(paramEnvironment) || isArgEmpty(paramPeaType)) {
        println(".....................[INFO] Arguments missing, Please put correct argument as <Date> <Hour> <Environment> <PeaType>")
        println(".....................[INFO] Spark-Submit parameters example : 2018-01-01 03 dev NA")
        sc.stop() //ToDo : return Spark job status
      } else {
        //----------------------------
        // mapping correct parameters
        //----------------------------
        if (paramEnvironment.trim.toUpperCase.equals("NA") || paramEnvironment.trim.toUpperCase.equals("LOCAL")) {
          hiveTableNameToReadTable = "pea_account_raw"
          hiveTableNameToWriteTable = "pea_account_cooked"
          hiveMetaTableSubTypeTable = "meta_pea_account_subtype"
        } else if (paramEnvironment.trim.toUpperCase.equals("DEV")){
          val paramEnvironmentPrefix = "" //"ec_dev" - doesn't exist in SQL view
          //'dev_core' to namespace? yes, otherwise can not find table in unity media
          hiveTableNameToReadTable = "dev_core.pea_account_raw"
          hiveTableNameToWriteTable = "dev_core.pea_account_cooked"
          hiveMetaTableSubTypeTable = "meta_pea_account_subtype"
        } else if (paramEnvironment.trim.toUpperCase.equals("TEST")) {
          val paramEnvironmentPrefix = ""//"ec_test" - doesn't exist in SQL view
          hiveTableNameToReadTable = "test_core.pea_account_raw"
          hiveTableNameToWriteTable = "test_core.pea_account_cooked"
          hiveMetaTableSubTypeTable = "meta_pea_account_subtype"
        }

        //-------------------------------------------------
        // Read raw peas from Hive table 'pea_account_raw'
        //-------------------------------------------------
        val peaAccountRawDF= sqlContext.sql("SELECT * FROM " + hiveTableNameToReadTable + " WHERE " + "date='" + paramDate + "' AND hour=" + paramHour);
        println("............. [DEBUG] (SELECT * FROM "+ hiveTableNameToReadTable + " WHERE " + "date='" + paramDate + "' AND hour=" + paramHour);
        peaAccountRawDF.show()

        val selectedCols = peaAccountRawDF.select(peaAccountRawDF.col("meta_past_id"), peaAccountRawDF.col("technical_value"), peaAccountRawDF.col("cust_relevance") )
        println("............[DEBUG] select columns")
        selectedCols.show()

        //-------------------------------------------------
        // Calculate scores using dedicated score function
        //-------------------------------------------------
        val df = calcScoreValue(peaAccountRawDF, sc)

        //----------------------
        // Write to Hive table
        //----------------------
        val isHiveTableOp = false
        if(isHiveTableOp) { //direct write to HIVE
          insertIntoHiveTable(df, hiveTableNameToWriteTable, sqlContext) //--> java.lang.NoSuchMethodException: org.apache.hadoop.hive.ql.metadata.Hive.loadDynamicPartitions
        } else { //indirect
          writeAsCsvFileInHDFS(df, sqlContext, sc, paramDate, paramHour, paramEnvironment, hiveTableNameToWrite, hiveTableNameToWriteTable)
        }

        //----------------------------------------
        // Show 'pea_account_cooked' Hive table
        //----------------------------------------
        //println("....... [DEBUG] SELECT * FROM "+ hiveTableNameToWrite + " WHERE " + "date='" + paramDate + "' AND hour=" + paramHour)
        //val appendCookedPea = sqlContext.sql("SELECT * FROM "+ hiveTableNameToWrite + " WHERE " + "date='" + paramDate + "' AND hour=" + paramHour)
        //appendCookedPea.show()

        println("...............[DEBUG] Finished SimpleApp")
      }
    } else {
        createAndTestLocalDataFrame()
    }
  }

  def isArgEmpty(x : String) = x == null || x.trim.isEmpty //(Option(x).forall(_.isEmpty)


  def writeAsCsvFileInHDFS(hiveDF : DataFrame, sqlContext: SQLContext, sc : SparkContext, paramDate : String, paramHour : String, paramEnv : String, hiveTableNameToWrite:String, hiveTableNameToWriteTable:String): Unit = {
    //-------------------------
    //delete date HDFS folder
    import org.apache.hadoop.fs.FileSystem
    import org.apache.hadoop.fs.Path
    val fs=FileSystem.get(sc.hadoopConfiguration)
    var outPutPath = ""

    if(paramEnv.trim.toUpperCase.equals("LOCAL")) {
      outPutPath="/user/hive/warehouse/"+hiveTableNameToWrite+"/date="+paramDate+"/hour="+paramHour
    } else {
      if(paramEnv.trim.toUpperCase.equals("DEV")) {
        outPutPath = "/user/ec_dev/core/db/"+hiveTableNameToWrite+"/date="+paramDate+"/hour="+paramHour
      } else if(paramEnv.trim.toUpperCase.equals("TEST")) {
        outPutPath = "/user/ec_test/core/db/"+hiveTableNameToWrite+"/date="+paramDate+"/hour="+paramHour
      }
    }

    println("..................[DEBUG] outPutPath : " + outPutPath)

    if(fs.exists(new Path(outPutPath))) {
      fs.delete(new Path(outPutPath), true)
    }

    //-------------------------------------
    //create HDFS folder to save .csv file
    fs.mkdirs(new Path(outPutPath))

    //-------------------
    //write as .csv file
    val outputfile = outPutPath
    var outputFileName = outputfile + "/temp_" + paramDate+"-"+paramHour+".csv"
    var mergedFileName = outputfile + "/" + paramDate+"-"+paramHour+".csv"
    var mergeFindGlob  = outputFileName

    //create file to use for 'load into' purpose
    hiveDF.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "false")
      .save(outputFileName)

    merge(mergeFindGlob, mergedFileName)
    hiveDF.unpersist()

    println(".......................[DEBUG] " + mergedFileName)
    //ToDo : (unitymedia Hive is strange) if partition is not existing, then SELECT * FROM <table> doesn't work in UnityMedia environment
    //msck repair partition is not necessary
    //hqlContext.runSqlHive("msck repair table table_name")

    if(paramEnv.trim.toUpperCase.equals("LOCAL")) {
      sqlContext.sql("load data inpath '"+mergedFileName+"' into table "+hiveTableNameToWriteTable+" partition (`date`='"+paramDate+"',hour="+paramHour+")")
    } else { //ToDo : check which one for unity media
      sqlContext.sql("load data inpath '"+mergedFileName+"' into table "+hiveTableNameToWriteTable+" partition (date='"+paramDate+"',hour="+paramHour+")")
    }

    println(".....................[DEBUG] load data inpath finished...")

    // backup : csv file since the original file is loaded into Hive table
    /*
    hiveDF.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "false")
      .save(outputFileName)

    merge(mergeFindGlob, mergedFileName)
    hiveDF.unpersist() //Destroy all data and metadata related to this broadcast variables
    */
  }

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs._

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)  // the "true" setting deletes the source files once they are merged into the new output
  }

  def calcScoreValue(peaAccountRawDF : DataFrame, sc : SparkContext): DataFrame = {
    import org.apache.spark.sql.functions.udf
    val scoreFunc = udf(selectAndCalcScoreFunction _)
    val new_peaAccountRawDF = peaAccountRawDF.withColumn("score", scoreFunc(peaAccountRawDF.col("technical_value"), peaAccountRawDF.col("cust_relevance"), peaAccountRawDF.col("meta_past_id")))
    //'score' should be in between 'meta_past_halflife' and 'root_id'
    val scoreAddedCookedPeasDF = new_peaAccountRawDF.select(new_peaAccountRawDF.col("party_id"), new_peaAccountRawDF.col("building_port_id"), new_peaAccountRawDF.col("cpe_equipment_id"), new_peaAccountRawDF.col("cpe_port_id"), new_peaAccountRawDF.col("cpe_mac"), new_peaAccountRawDF.col("trigger"), new_peaAccountRawDF.col("ts_created"), new_peaAccountRawDF.col("ts_received"), new_peaAccountRawDF.col("meta_past_id"), new_peaAccountRawDF.col("score"), new_peaAccountRawDF.col("root_id"), new_peaAccountRawDF.col("date"), new_peaAccountRawDF.col("hour"))

    //val sqlContext: SQLContext = new SQLContext(sc)
    //val tmpHalfLifeGetFunc = udf(getSubTypeHalfLife _)

    /* temporary comment out
    val halflifeAddedCookedPeasDF = scoreAddedCookedPeasDF.withColumn("meta_past_halflife", tmpHalfLifeGetFunc(sqlContext, scoreAddedCookedPeasDF))
    val cookedPeasDF = halflifeAddedCookedPeasDF.select(halflifeAddedCookedPeasDF.col("party_id"), halflifeAddedCookedPeasDF.col("building_port_id"), halflifeAddedCookedPeasDF.col("cpe_equipment_id"), halflifeAddedCookedPeasDF.col("cpe_port_id"), halflifeAddedCookedPeasDF.col("cpe_mac"), halflifeAddedCookedPeasDF.col("trigger"), halflifeAddedCookedPeasDF.col("ts_created"), halflifeAddedCookedPeasDF.col("ts_received"), halflifeAddedCookedPeasDF.col("meta_past_id"), halflifeAddedCookedPeasDF.col("meta_past_halflife"), halflifeAddedCookedPeasDF.col("score"), halflifeAddedCookedPeasDF.col("root_id"), halflifeAddedCookedPeasDF.col("date"), halflifeAddedCookedPeasDF.col("hour"))
    */

    val activeSubTypeDF = getSubTypeHalfLife(sc, scoreAddedCookedPeasDF)
    println("........................[DEBUG] activeSubTypeDF")
    activeSubTypeDF.show()

    //join dataframe
    //val df_l = activeSubTypeDF.as("left_table")
    //val df_r = scoreAddedCookedPeasDF.as("right_table")
    //val joined_table = df_l.join(df_r, df_l.col("meta_past_id") === df_r.col("meta_past_id"), "full_outer")
    //ToDo : left full join? check
    val halflifeAddedCookedPeasDFJoined = scoreAddedCookedPeasDF.join(activeSubTypeDF, scoreAddedCookedPeasDF.col("meta_past_id") === activeSubTypeDF.col("meta_past_id"), "left_outer")//(activeSubTypeDF.col("meta_past_id"))
    val halflifeAddedCookedPeasDF = halflifeAddedCookedPeasDFJoined.drop(scoreAddedCookedPeasDF.col("meta_past_id"))
    println("........................[DEBUG] halflifeAddedCookedPeasDF")
    halflifeAddedCookedPeasDF.show()

    //val halflifeAddedCookedPeasDF = scoreAddedCookedPeasDF.withColumn("meta_past_halflife", lit(1)) //tmpHalfLifeGetFunc(scoreAddedCookedPeasDF.col("meta_past_id")))
    val cookedPeasDF = halflifeAddedCookedPeasDF.select(halflifeAddedCookedPeasDF.col("party_id"), halflifeAddedCookedPeasDF.col("building_port_id"), halflifeAddedCookedPeasDF.col("cpe_equipment_id"), halflifeAddedCookedPeasDF.col("cpe_port_id"), halflifeAddedCookedPeasDF.col("cpe_mac"), halflifeAddedCookedPeasDF.col("trigger"), halflifeAddedCookedPeasDF.col("ts_created"), halflifeAddedCookedPeasDF.col("ts_received"), halflifeAddedCookedPeasDF.col("meta_past_id"), halflifeAddedCookedPeasDF.col("meta_past_halflife"), halflifeAddedCookedPeasDF.col("score"), halflifeAddedCookedPeasDF.col("root_id"), halflifeAddedCookedPeasDF.col("date"), halflifeAddedCookedPeasDF.col("hour"))
    println("........................[DEBUG] cookedPeasDF")
    cookedPeasDF.show()
    cookedPeasDF
  }

  def getMetaSubtypeDF(sqlContext : SQLContext): DataFrame = {
    val df = sqlContext.read.format("jdbc").option("url", "jdbc:postgresql://----/rubcom")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "dev_inthub_meta_unity_spark.meta_pea_account_subtype")//"public.records")
      .option("user", "user")
      .option("password", "password").load()//.select("meta_past_id")

    val selectedDF = df.select(df.col("meta_past_id"), df.col("meta_past_halflife"), df.col("scd_is_active"), df.col("scd_is_deleted"))
    println("...............................[DEBUG] show meta_past_id, scd info and halflife value")

    return selectedDF
  }

  def calcScoreValueAndCast(peaAccountRawDF : DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.udf
    val scoreFunc = udf(selectAndCalcScoreFunction _)
    val new_peaAccountRawDF = peaAccountRawDF.withColumn("score", scoreFunc(peaAccountRawDF.col("technical_value"), peaAccountRawDF.col("cust_relevance"), peaAccountRawDF.col("meta_past_id")))
    val simplifiedCookedPeasDF = new_peaAccountRawDF.select(new_peaAccountRawDF.col("party_id"), new_peaAccountRawDF.col("building_port_id"), new_peaAccountRawDF.col("cpe_equipment_id"), new_peaAccountRawDF.col("cpe_port_id"), new_peaAccountRawDF.col("cpe_mac"), new_peaAccountRawDF.col("trigger"), new_peaAccountRawDF.col("ts_created").cast(DataType.toString), new_peaAccountRawDF.col("ts_received").cast(DataType.toString), new_peaAccountRawDF.col("meta_past_id"), new_peaAccountRawDF.col("score"), new_peaAccountRawDF.col("root_id"), new_peaAccountRawDF.col("date"), new_peaAccountRawDF.col("hour"))

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


  def speedTestScoreFunction (x : Double) : Double = { //symmetric tangent
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

  def HFCScoreFunction (x : Double) : Double = { //negative decay function
    var score = 0.0

    val slope = 1.2
    val maxPos = 0
    val maxMin = -2

    score = (maxPos - maxMin) * math.exp(-slope * x) - (maxPos - maxMin) + maxPos
    return(score)
  }

  /*----------------------------------
   * 1 : speedtest customer up
   * 2 : speedtest customer down
   * 3 : unitymedia up
   * 4 : unitymedia down
   * 5 : none up
   * 6 : none down
   ------------------------------------*/
  def getSubTypeMapping(): Unit = {

  }

  //----------------------------------------------
  // Read halflife value from meta subtype table
  //----------------------------------------------
  def getMetaPastHalflife(sqlContext: HiveContext, meta_past_id : Int, hiveMetaTableSubType : String) : Double = {
    var halflife = 0.0

    val subTypeTable = sqlContext.sql("SELECT * FROM "+ hiveMetaTableSubType)
    val filteredDF = subTypeTable.filter(subTypeTable.col("meta_past_id").equalTo(meta_past_id)).filter(subTypeTable.col("scd_is_active").equalTo(1)).filter(subTypeTable.col("scd_is_deleted").equalTo(0))
    filteredDF.col("meta_past_halflife").apply(0)

    //ToDo : return double value of filteredDF.col("meta_past_halflife").apply(0)
    return halflife
  }

  def getSubTypeHalfLifeColumn(sc:SparkContext, rawPeasDF : DataFrame) : Column = {
    //val conf = new SparkConf().setAppName("Postgres Application")//.setMaster("local")
    //val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)
    val subTypeDF = getMetaSubtypeDF(sqlContext)
    val activeSubTypeDF = subTypeDF.filter(subTypeDF.col("scd_is_active") === 1 && subTypeDF.col("scd_is_deleted") === 0)
    println("........................[DEBUG] activeSubType DataFrame")
    activeSubTypeDF.show()
    return activeSubTypeDF.col("meta_past_halflife")
  }

  def getSubTypeHalfLife(sc:SparkContext, rawPeasDF : DataFrame) : DataFrame = {
    //val conf = new SparkConf().setAppName("Postgres Application")//.setMaster("local")
    //val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)
    val subTypeDF = getMetaSubtypeDF(sqlContext)
    val activeSubTypeDF = subTypeDF.filter(subTypeDF.col("scd_is_active") === 1 && subTypeDF.col("scd_is_deleted") === 0)
    //println("........................[DEBUG] activeSubType DataFrame")
    //activeSubTypeDF.show()
    return activeSubTypeDF
  }

  def getSqlContext(): Unit ={

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

  def insertIntoHiveTable(df: DataFrame, tableName : String, sqlContext: HiveContext): Unit = {
    df.coalesce(1).write.mode("append").partitionBy("date", "hour").insertInto(tableName)
  }

  def writeFewColumns(df: DataFrame) : Unit = {
    df.select(df.col("party_id"), df.col("date"), df.col("hour")).write.mode("overwrite").saveAsTable("pea_account_cooked")
  }

  def writeToNewHiveTable(df: DataFrame): Unit = { //error : non existing table does not work - table should be there
    df.write.mode(SaveMode.Overwrite).insertInto("pea_account_cooked_test")
    println("................DEBUG WriteToHiveTable")
    df.show()
  }

  def writeToHiveTable(df: DataFrame): Unit = { //does not write, no error
    df.write.mode(SaveMode.Overwrite).insertInto("pea_account_cooked")
    println("................DEBUG WriteToHiveTable")
    df.show()
  }


  def saveAsTableToHiveTable(df: DataFrame): Unit = { //does not write, no error
    df.write.mode(SaveMode.Overwrite).saveAsTable("pea_account_cooked")
    println("................DEBUG WriteToHiveTable")
    df.show()
  }


  def saveAsTableToHiveTableFormatOrcWithoutPartition(df: DataFrame, tableName : String): Unit = {
    df.write.mode(SaveMode.Overwrite).format("orc").saveAsTable(tableName)
    println("................DEBUG WriteToHiveTable")
    df.show()
  }

  def saveAsTableToHiveTableFormatOrcWithPartition(df: DataFrame, tableName : String): Unit = {
    df.write.mode(SaveMode.Overwrite).format("orc").partitionBy("date", "hour").saveAsTable(tableName)
    println("................DEBUG WriteToHiveTable")
    df.show()
  }


  def insertIntoTableFormatOrcWithPartition(df: DataFrame, tableName : String): Unit = {
    df.write.mode(SaveMode.Append).format("orc").partitionBy("date", "hour").insertInto(tableName)
    println("................DEBUG insertIntoTableFormatOrcWithPartition")
    df.show()
  }

  def insertIntoTableFormatOrcWithoutPartition(df: DataFrame, tableName : String): Unit = {
    df.coalesce(1).write.mode(SaveMode.Append).format("orc").insertInto(tableName)
    println("................DEBUG insertIntoTableFormatOrcWithoutPartition")
    df.show()
  }


  def saveAsTableToHiveTableFormatCsv(df: DataFrame, tableName:String): Unit = {
    //df.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").saveAsTable("dev_core.pea_account_cooked")
    df.write.mode(SaveMode.Overwrite).format("csv").saveAsTable(tableName)
    println("................DEBUG saveAsTableToHiveTableFormatCsv()")
    df.show()
  }

  def saveAsTableToHiveTableFormatCsvWithPartition(df: DataFrame, tableName : String): Unit = {
    df.write.mode(SaveMode.Append).format("com.databricks.spark.csv").partitionBy("date", "hour").insertInto(tableName)
    println("................DEBUG aveAsTableToHiveTableFormatCsvWithPartition()")
    df.show()
  }

  def writeToHiveTableInCsv(df: DataFrame, tableName:String): Unit = {
    df.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").insertInto(tableName)
    println("................DEBUG WriteToHiveTableInCsv")
    df.show()
  }

  //comment : this doesn't show as select * from table since 'FAILED: UnsupportedOperationException Parquet does not support date. See HIVE-6384'
  def writeToHiveTableParquet(df: DataFrame): Unit = {
    df.write.mode("overwrite").saveAsTable("pea_account_cooked");
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

  //------------------------------------------------------------------------------------
  // warning : insert statement is very slow in HIVE and it's not for HIVE in general
  //------------------------------------------------------------------------------------
  def insertIntoHive(sqlContext : HiveContext): Unit = {
    sqlContext.sql("insert into pea_account_cooked1 partition (date='2018-01-01',hour=02) values (228918879,92017266,3445801,13671760,'481C4CC6B7A','CUSTOMER','2018-01-01 00:57:25','2018-03-15 13:33:06',2,100.0,112354305)")
  }

  //--------------------------------------------
  // make partitions and save files in parquet
  //--------------------------------------------
  def writeToHiveWithCompress(hiveDF : DataFrame): Unit = {
    val isSaveTable = true
    if(isSaveTable) {
      hiveDF.write.mode(SaveMode.Overwrite).option("compression", "gzip").partitionBy("date", "hour").saveAsTable("pea_account_cooked")
    } else {
      hiveDF.write.mode(SaveMode.Overwrite).option("compression", "gzip").partitionBy("date", "hour").insertInto("pea_account_cooked")
    }
  }
}



