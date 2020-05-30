package featureutil

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import util.UDFUtil.changeType

object TFIDFextractor {
  def generateTfidfFromRDD(ss: SparkSession, rdd: RDD[(String, (String, String, String))]): DataFrame = {
    val colNames = Array("id", "cut_words", "resource", "raw_text")
    val schemas = StructType(colNames.map(fieldName => StructField(fieldName, StringType)))
    val rowRdd = rdd.map(item => Seq(item._1, item._2._1, item._2._2, item._2._3)).map(p => Row(p: _*))
    val dataFrame = ss.createDataFrame(rowRdd, schemas)
    generateTfidfFromDataFrame(dataFrame)
  }

  def generateTfidfFromDataFrame(dataFrame: DataFrame): DataFrame = {
    val tokenizer = new Tokenizer().setInputCol("cut_words").setOutputCol("words")
    val wordsData = tokenizer.transform(dataFrame)
    val hashTF = new HashingTF().setInputCol("words").setOutputCol("TFfeature").setNumFeatures(Math.pow(2, 12).toInt)
    val tfData = hashTF.transform(wordsData)
    val idf = new IDF().setInputCol("TFfeature").setOutputCol("idfFeature")
    val idfModel = idf.fit(tfData)
    idfModel.transform(tfData)
  }

  def serializeDataFrame(dataFrame: DataFrame): Unit = {
    changeType(dataFrame).repartition(1).write
      .format("csv")
      .option("header", true)
      .option("delimiter", "\t")
      .save("file:///tfidf")
  }
}
