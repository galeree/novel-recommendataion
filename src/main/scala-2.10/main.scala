/**
  * Created by Galle on 5/1/2016 AD.
  */
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

object Recommend {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // Set up environment
    val conf = new SparkConf()
      .setAppName("RecommendDekd")
      .set("spark.executor.memory", "2g")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    // Load and parse the data
    val path = "/Users/Galle/Dekd/fav_story_train.csv"
    val data = sc.textFile(path).mapPartitions(_.drop(1))
    val ratings = data.map(_.split(',') match { case Array(user, item, date, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // Evaluate the model on rating data
    val userProducts = ratings.map { case Rating(user, product, rate) =>
      (user,product)
    }

    val predictions =
      model.predict(userProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    val MSE = ratesAndPreds.map {case ((user, product), (r1,r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    println("Mean Squared Error = " + MSE)
  }
}