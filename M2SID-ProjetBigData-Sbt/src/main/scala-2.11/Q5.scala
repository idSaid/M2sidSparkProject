import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Said on 17/03/2018.
  */
object Q5 {

  def main(args: Array[String]): Unit = {
    val startTimeMillis = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("Prog1").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //val data = sc.textFile("C:\\Users\\Said\\IdeaProjects\\untitled\\M2SID-ProjetBigData-Sbt\\Crimes-2001-present.csv", 2)
    val data = sc.textFile("share/Crimes.csv", 2)
    val moisHeader = data.first().split(",")(2)

    val crimesParMois = data.map(l => l.split(",")(2))
      .filter(row => row != moisHeader)
      .map(l => l.split(" ")(0))
      .map(l => l.split("/")(0))
      .map(l => (l,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)
      .take(3)

    sc.parallelize(crimesParMois)
      .saveAsTextFile("output/Q5")
      //.saveAsTextFile("M2SID-ProjetBigData-Sbt\\Q5")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println(">>>>>> Execution time : ", durationSeconds , "(sec)")
  }

}