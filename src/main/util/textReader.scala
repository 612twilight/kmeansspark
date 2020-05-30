package util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object textReader {
  def readText(): RDD[String] = {
    val sparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val rdd = sparkSession.sparkContext.textFile("./src/main/resources/rawdata")
    rdd.take(1).foreach(println)
    rdd
  }

  def main(args: Array[String]): Unit = {
    readText().foreach(println)
  }

}
