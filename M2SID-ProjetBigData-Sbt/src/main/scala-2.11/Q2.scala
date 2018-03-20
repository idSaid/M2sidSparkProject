/**
  * Created by Said on 04/03/2018.
  */

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object Q2 {

  val startTimeMillis = System.currentTimeMillis()

  val format = new java.text.SimpleDateFormat("MM/dd/YYYY hh:mm:ss a")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Prog1").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val data = sc.textFile("C:\\Users\\Said\\IdeaProjects\\untitled\\M2SID-ProjetBigData-Sbt\\Crimes-2001-present.csv", 2)
    //val data = sc.textFile("share/Crimes.csv", 2)
    val DateHeader = data.first().split(",")(2)

    data
      .map(l => {
        l.split(",")(2)
      })
      .filter(x => (x.nonEmpty && x != DateHeader))
      .filter(l => {
        val cols =  l.split(" ")
        var hour = cols(1).split(":")(0)
        var ampm = cols(2)
        Try(hour.toInt).isSuccess
      })
      .map(l=> {
        val cols =  l.split(" ")
        var hour = cols(1).split(":")(0).toInt
        var ampm = cols(2)
        if(ampm.toLowerCase == "pm") hour+=12
        hour %= 24
        hour
      })
      .map(h => {
        try{
          val plage = h match {
            case x if x >= 0 && x <= 3 => PlageHorraire.P0_4
            case y if y >= 4 && y < 8 => PlageHorraire.P4_8
            case z if z >= 8 && z < 12 => PlageHorraire.P8_12
            case a if a >= 12 && a < 16 => PlageHorraire.P12_16
            case b if b >= 16 && b < 20 => PlageHorraire.P16_20
            case c if c >= 20 && c <= 23 => PlageHorraire.P20_24
            case _ => null
          }
          (plage, 1)
        }
        catch {
          case e: Throwable => print("")
            (null,1)
        }
      })
      .reduceByKey(_+_)
      .saveAsTextFile("output/Q2")
      //.saveAsTextFile("M2SID-ProjetBigData-Sbt\\Q2")
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println(">>>>>> Execution time : ", durationSeconds , "(sec)")
  }
}

object PlageHorraire extends Enumeration {
  val P0_4,P4_8,P8_12,P12_16,P16_20,P20_24 = Value
}