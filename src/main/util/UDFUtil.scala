package util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType}

object UDFUtil {
  def changeType(df: DataFrame): DataFrame = {
    var df2 = df
    val colNames = df2.columns
    for (colName <- colNames) {
      df2 = df2.withColumn(colName, col(colName).cast(StringType))
    }
    df2
  }
}
