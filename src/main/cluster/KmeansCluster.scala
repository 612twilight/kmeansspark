package cluster

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

object KmeansCluster {
  def kmeanscluster(dataFrame: DataFrame): KMeansModel = {
    val vectors = dataFrame.rdd.map(item => org.apache.spark.mllib.linalg.Vectors.fromML(
      item.getAs[org.apache.spark.ml.linalg.SparseVector]("idfFeature")))
    val numClass = 2
    val numIterations = 200
    val cluster = KMeans.train(vectors, numClass, numIterations)
    vectors.take(1).foreach(v => println("Vectors " + v.toString + "belong to cluster " + cluster.predict(v)))
    cluster
  }
}
