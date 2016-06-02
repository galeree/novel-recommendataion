package dekd.novel

/**
  * Created by Galle on 5/11/2016 AD.
  */
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}

// Define container class
case class Prop(id: String, feature: Vector)

object clustering {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // Set up environment
    val conf = new SparkConf()
      .setAppName("ClusteringDekd")
      .setMaster("local[2]")

    // Initializa SparkContext
    val sc = new SparkContext(conf)

    // Load data
    val path = "/Users/Galle/Dekd/novel-hour.csv"
    val data = sc.textFile(path)

    // comma separator split
    val allData = data.map(_.split(','))
      .map(p => Prop(p(0).toString,
                     Vectors.dense(p(1).toInt, p(2).toInt, p(3).toInt, p(4).toInt,
                                   p(5).toInt, p(6).toInt, p(7).toInt, p(8).toInt,
                                   p(9).toInt, p(10).toInt, p(11).toInt, p(12).toInt,
                                   p(13).toInt, p(14).toInt, p(15).toInt, p(16).toInt,
                                   p(17).toInt, p(18).toInt, p(19).toInt, p(20).toInt,
                                   p(21).toInt, p(22).toInt, p(23).toInt, p(24).toInt)))


    val vectors = allData.map(r => r.feature).cache()

    //KMeans model with 6 clusters and 20 iterations
    val kMeansModel = KMeans.train(vectors, 6, 20)
    val center      = kMeansModel.clusterCenters

    // Get the prediction from the model with the ID so we can link them back to other information
    val predictions = allData.map(r => (r.id, kMeansModel.predict(r.feature)))
    val output = center.map(s => s.toArray.mkString(","))

    // Evaluate Model
    val WSSSE = kMeansModel.computeCost(vectors)
    println("Within Set Sum of Squared Errors = " + WSSSE)
  }
}
