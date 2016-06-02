/**
  * Created by Galle on 5/3/2016 AD.
  */
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

object recommend {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // Set up environment
    val conf = new SparkConf()
      .setAppName("RecommendDekd")
      .setMaster("local[2]")

    // Initializa SparkContext
    val sc = new SparkContext(conf)

    // Load and parse the data
    val path_train = "/Users/Galle/Dekd/fav_story_train.csv"
    val path_test  = "/Users/Galle/Dekd/test_new.csv"

    /*val train = sc.textFile(path_train)
                  .mapPartitions(_.drop(1))
                  .map(_.split(',') match { case Array(user, item, date, rate) => (user, item, rate) })*/
    val test  = sc.textFile(path_test)
                  .map(_.split(',') match { case Array(user, num_item) => (user.toInt, num_item.toInt)})

    /*val ratings = train.map { case (user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble) }

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val lambda = 0.01
    val alpha = 200
    val model = ALS.trainImplicit(
      ratings = ratings,
      rank = rank,
      iterations = numIterations,
      lambda = lambda,
      alpha = alpha
    )*/


    val model = MatrixFactorizationModel.load(sc, "/Users/Galle/Dekd/model")
    //model.recommendProductsForUsers(10)

    val result = test.take(100).map { case (user, num_item) => (user, model.recommendProducts(user, num_item).map(r => r.product))}
    val lastresult = sc.parallelize(result)
    val file = "/Users/Galle/Dekd/Output"

    val outputfile = lastresult.repartition(1)
                                .map { case r => r._1.toString + "," + r._2.mkString(",") }

    outputfile.saveAsTextFile(file)


  }
}
