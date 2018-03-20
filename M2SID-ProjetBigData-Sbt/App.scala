import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by adilbelhaji on 8/31/17.
  */
object App {
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext()
    sc.textFile("data/crimes.csv")
      .take(3)
    .foreach(println)
  }

  def getSparkContext() = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CountingSheep")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc
  }
}
