import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans

import scala.util.Try


/**
  * Created by Said on 06/03/2018.
  */
object Q3 {

  def main(args: Array[String]): Unit = {
    val startTimeMillis = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("Prog1").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //val data = sc.textFile("C:\\Users\\Said\\IdeaProjects\\untitled\\M2SID-ProjetBigData-Sbt\\Crimes-2001-present.csv", 2)
    val data = sc.textFile("share/Crimes.csv", 2)
    val header = data.first()
    val filterdData = data.filter(row => row != header)

    val latLong = filterdData
      .filter(l => {
        val line = l.split(",")
        val lattitude = line(line.length - 4)
        val longitude = line(line.length - 3)
        lattitude.nonEmpty && longitude.nonEmpty && Try(lattitude.toDouble).isSuccess && Try(longitude.toDouble).isSuccess
      })
      .map(l => {
        val line = l.split(",")
        val lattitude = line(line.length - 4)
        val longitude = line(line.length - 3)
        (lattitude.toDouble, longitude.toDouble)
      })

    val vectors = latLong.map(ll => Vectors.dense( ll._1,ll._2)).cache()
    val kMeansModel = KMeans.train(vectors, 12, 40)
    val predictions = latLong.map{ll => (kMeansModel.predict(Vectors.dense(ll._1, ll._2)),1)}

    val mostDagerousZones = predictions.reduceByKey(_+_)
      .sortBy(-_._2)
      .take(3)

    val lessDagerousZones = predictions.reduceByKey(_+_)
      .sortBy(_._2)
      .take(3)

    //sc.parallelize(mostDagerousZones).saveAsTextFile("M2SID-ProjetBigData-Sbt\\Q3-MostDangerousZones")
    //sc.parallelize(lessDagerousZones).saveAsTextFile("M2SID-ProjetBigData-Sbt\\Q3-LessDangerousZones")

    sc.parallelize(mostDagerousZones).saveAsTextFile("output/Q3-MostDangerousZones")
    sc.parallelize(lessDagerousZones).saveAsTextFile("output/Q3-LessDangerousZones")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println(">>>>>> Execution time : ", durationSeconds , "(sec)")

  }

}
