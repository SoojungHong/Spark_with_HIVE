//--------------------------------------------------
// build.sbt for Cloudera CDH 5.10.2 & Spark 1.6.2
//--------------------------------------------------
name := "CookingPeas"

version := "1.0"
scalaVersion := "2.10.5"
val sparkVersion = "1.6.0"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions += "-deprecation"

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % sparkVersion
