/**
  * Created by Said on 04/03/2018.
  */

import java.sql.Date
import org.apache.spark.{SparkConf, SparkContext}
import java.text.DateFormatSymbols
import java.text.SimpleDateFormat

object Q2 {


  val format = new java.text.SimpleDateFormat("MM/dd/YYYY hh:mm:ss a")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Prog1").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val data = sc.textFile("C:\\Users\\Said\\IdeaProjects\\untitled\\M2SID-ProjetBigData-Sbt\\extraitCrimes.csv", 2)
    val DateHeader = data.first().split(",")(2)

    data.map(l => {
      l.split(",")(2)
    })
      .filter(x => (x.nonEmpty && x != DateHeader))
      .map(l => {
        try{
          val date = format.parse(l)
          val plage = date.getHours match {
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
          case e: Throwable => println(e)
            (null,1)
        }
      })
      .reduceByKey(_+_)
      .saveAsTextFile("M2SID-ProjetBigData-Sbt\\Q2")

  }

}

object PlageHorraire extends Enumeration {
  val P0_4,P4_8,P8_12,P12_16,P16_20,P20_24 = Value
}