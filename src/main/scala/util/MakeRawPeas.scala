package util

import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConversions._
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.mutable.ListBuffer

object MakeRawPeas {

  def main(args:Array[String]): Unit = {

    val outputFile = new BufferedWriter(new FileWriter("rawpeas.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvHeader = Array("x1", "x2", "x3")
    val x1List = (1 to 10).toList
    val x2List = (1 to 10).toList
    val x3List = (1 to 10).toList
    val random = new Random()
    var listOfPeas = new ListBuffer[Array[String]]()

    //generate peas
    for(i <- 1 to 20000000) {
      listOfPeas += Array(x1List(random.nextInt(x1List.length)).toString,x2List(random.nextInt(x2List.length)).toString,x3List(random.nextInt(x3List.length)).toString)
    }

    csvWriter.writeAll(listOfPeas.toList)
    outputFile.close()
  }

  def createRawPeas(): Unit = {
    val outputFile = new BufferedWriter(new FileWriter("rawpeas.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvHeader = Array("x1", "x2", "x3")
    val x1List = (1 to 10).toList
    val x2List = (1 to 10).toList
    val x3List = (1 to 10).toList
    val random = new Random()
    var listOfPeas = new ListBuffer[Array[String]]()

    //generate peas
    for(i <- 1 to 20) { //20000000) {
      var intArr = Array(x1List(random.nextInt(x1List.length)),x2List(random.nextInt(x2List.length)),x3List(random.nextInt(x3List.length)))
      listOfPeas += intArr.map(_.toString)
    }

    csvWriter.writeAll(listOfPeas.toList)
    outputFile.close()
  }

}
