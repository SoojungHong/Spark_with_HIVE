//--------------------------------------------------
// build.sbt for Cloudera CDH 5.10.2 & Spark 1.6.2
//--------------------------------------------------
name := "CookingPeas"

version := "1.0"
scalaVersion := "2.10.5"
val sparkVersion = "1.6.0"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions += "-deprecation"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % sparkVersion
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.1.0"
libraryDependencies += "org.apache.hive" % "hive-exec" % "1.1.0"




