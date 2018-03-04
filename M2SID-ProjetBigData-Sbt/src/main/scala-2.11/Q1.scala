/**
  * Created by Said on 04/03/2018.
  */

import org.apache.spark.{SparkConf, SparkContext}

object Q1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Prog1").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    sc.textFile("C:\\Users\\Said\\IdeaProjects\\untitled\\M2SID-ProjetBigData-Sbt\\extraitCrimes.csv", 2)
      .map(l => {
        l.split(",")(5)
      })
      .filter(x => x.nonEmpty)
      .map(x => (x,1))
      .reduceByKey(_+_)
      .sortBy(- _._2)
      .saveAsTextFile("M2SID-ProjetBigData-Sbt\\desc crimes categories")

  }
}
