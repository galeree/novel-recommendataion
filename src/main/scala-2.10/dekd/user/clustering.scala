package dekd.user

/**
  * Created by Galle on 5/9/2016 AD.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

// Define container class
case class Prop(id: String, f1: F1, f2: F2)
case class F1(p1: Int, p2: Int, p3: Int, p4: Int, p5: Int, p6: Int,
              p7: Int, p8: Int, p9: Int, p10: Int, p11: Int, p12: Int)

case class F2(p1: Int, p2: Int, p3: Int, p4: Int, p5: Int, p6: Int,
              p7: Int, p8: Int, p9: Int, p10: Int, p11: Int, p12: Int)

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
    val path = "/Users/Galle/Dekd/user-hour.csv"
    val data = sc.textFile(path)

    // comma separator split
    val allData = data.map(_.split(','))
                       .map(p => Prop(p(0).toString,
                                      F1(p(1).toInt, p(2).toInt, p(3).toInt, p(4).toInt,
                                        p(5).toInt, p(6).toInt, p(7).toInt, p(8).toInt,
                                        p(9).toInt, p(10).toInt, p(11).toInt, p(12).toInt),
                                      F2(p(13).toInt, p(14).toInt, p(15).toInt, p(16).toInt,
                                        p(17).toInt, p(18).toInt, p(19).toInt, p(20).toInt,
                                        p(21).toInt, p(22).toInt, p(23).toInt, p(24).toInt)))


    val vectors = allData.map(r => Vectors.dense(r.f1.p1, r.f1.p2, r.f1.p3, r.f1.p4, r.f1.p5, r.f1.p6,
                                                 r.f1.p7, r.f1.p8, r.f1.p9, r.f1.p10, r.f1.p11, r.f1.p12,
                                                 r.f2.p1, r.f2.p2, r.f2.p3, r.f2.p4, r.f2.p5, r.f2.p6,
                                                 r.f2.p7, r.f2.p8, r.f2.p9, r.f2.p10, r.f2.p11, r.f2.p12)).cache()


    //KMeans model with 10 clusters and 20 iterations
    val kMeansModel = KMeans.train(vectors, 10, 20)
    val center      = kMeansModel.clusterCenters

    // Get the prediction from the model with the ID so we can link them back to other information
    val predictions = allData.map(r => (r.id,
                                        kMeansModel.predict(Vectors.dense(r.f1.p1, r.f1.p2, r.f1.p3, r.f1.p4, r.f1.p5, r.f1.p6,
                                          r.f1.p7, r.f1.p8, r.f1.p9, r.f1.p10, r.f1.p11, r.f1.p12,
                                          r.f2.p1, r.f2.p2, r.f2.p3, r.f2.p4, r.f2.p5, r.f2.p6,
                                          r.f2.p7, r.f2.p8, r.f2.p9, r.f2.p10, r.f2.p11, r.f2.p12))))


    val output = center.map(s => s.toArray.mkString(","))

    val WSSSE = kMeansModel.computeCost(vectors)
    println("Within Set Sum of Squared Errors = " + WSSSE)
  }
}
