/**
  * Created by Said on 07/03/2018.
  */

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

object Q4 {

  def main(args: Array[String]): Unit = {
    val startTimeMillis = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("Prog1").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //val data = sc.textFile("C:\\Users\\Said\\IdeaProjects\\untitled\\M2SID-ProjetBigData-Sbt\\Crimes-2001-present.csv", 2)
    val data = sc.textFile("share/Crimes.csv", 2)
    val header = data.first()

    data.filter(row => row != header)
      .map(l => {
        val columns = l.split(",")
        val arrested = columns(columns.length - 15)
        val district = columns(columns.length - 12)
        (arrested, district)
      })
      .filter(t => {

        t._1.nonEmpty && t._1.nonEmpty &&
          (t._1.toLowerCase == "true" || t._1.toLowerCase == "false") &&
          Try(t._2.toInt).isSuccess
      })
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile("output/Q4")
      //.saveAsTextFile("M2SID-ProjetBigData-Sbt\\Q4")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println(">>>>>> Execution time : ", durationSeconds , "(sec)")

  }

}
