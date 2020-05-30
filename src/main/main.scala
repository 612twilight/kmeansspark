import org.apache.spark.sql.{DataFrame, SparkSession}
import texthandler.WordSplit.jiebaCut
import texthandler.JsonReader.generateFromJson
import featureutil.TFIDFextractor.generateTfidfFromRDD
import util.textReader.readText
import cluster.KmeansCluster.kmeanscluster

object main {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val rdd = sparkSession.sparkContext.textFile("./src/main/resources/rawdata")
    rdd.take(1).foreach(println)
    val tmp = rdd.map(generateFromJson)
    val feature: DataFrame = generateTfidfFromRDD(sparkSession, tmp)
    val model = kmeanscluster(feature)
    model.save(sparkSession.sparkContext, "./src/main/resources/modelpath")
  }
}
