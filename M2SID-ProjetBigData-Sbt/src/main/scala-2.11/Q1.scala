/**
  * Created by Said on 04/03/2018.
  */

import org.apache.spark.{SparkConf, SparkContext}

object Q1 {
  def main(args: Array[String]): Unit = {
    val startTimeMillis = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("Prog1").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //val data = sc.textFile("C:\\Users\\Said\\IdeaProjects\\untitled\\M2SID-ProjetBigData-Sbt\\Crimes-2001-present.csv", 2)
    val data = sc.textFile("share/Crimes.csv", 2)
    val categoryTypeHeader = data.first().split(",")(5)
    data
      .map(l => {
        l.split(",")(5)
      })
      .filter(x => (x.nonEmpty && x != categoryTypeHeader))
      .map(x => (x,1))
      .reduceByKey(_+_)
      .sortBy(- _._2)
      .saveAsTextFile("output/Q1")
    //.saveAsTextFile("M2SID-ProjetBigData-Sbt\\Q1")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println(">>>>>> Execution time : ", durationSeconds , "(sec)")

  }
}
