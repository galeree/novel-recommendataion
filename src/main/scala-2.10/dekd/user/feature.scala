package dekd.user

/**
  * Created by Galle on 5/8/2016 AD.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object feature {
  // Convert string to datetime
  def getHour(s: String) : Int = {
    val hour = s.substring(10,13).trim()
    hour match {
      case t if t.matches("^0") => t.substring(1).toInt
      case _  => hour.toInt
    }
  }

  def getFeature(l: List[(Int, Int)]) : List[(Int, Int)] = {
    var count = 0
    val output = new ListBuffer[(Int,Int)]()
    val map = l.toMap
    for ( count <- 0 to 23) {
        try {
          val r = map(count)
          output .+= ((count,r))
        } catch {
          case _ : Throwable => {
            output .+= ((count,0))
          }
        }
    }
    output.toList
  }

  def flatList(l: List[(Int, Int)]) : List[Int] = {
    l.map { case (hour, num_like) => num_like }
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // Set up environment
    val conf = new SparkConf()
      .setAppName("ClusterDekd")
      .setMaster("local[2]")

    // Initializa SparkContext
    val sc = new SparkContext(conf)

    // Load data
    val path  = "/Users/Galle/Dekd/fav_story_train.csv"
    val data  = sc.textFile(path)
                  .mapPartitions(_.drop(1))
                  .map(_.split(',') match { case Array(user, item, date, count) => ((user.toInt, getHour(date)), count.toInt)})

    val grp  = data.reduceByKey((x,y) => x+y)
                    .sortByKey()
                    .map { case ((user,hour), count) => (user, (hour, count))}
                    .groupByKey
                    .mapValues(_.toList)

    val filter = grp.map { case (user, item) => (user, flatList(getFeature(item))) }


    val file = "/Users/Galle/Dekd/Output"
    val outputfile = filter.repartition(1)
                            .map { case (key, value) => Array(key, value.mkString(",")).mkString(",") }

    outputfile.saveAsTextFile(file)
  }
}
