package FlattenDataFrame

import FlattenDataFrame.FlattenDataframe.flattenDataFrame
import org.apache.spark.sql.SparkSession

object JsontoParquet {
    def main(args: Array[String]) = {
      val spark = SparkSession.builder
        .master("local[*]")
        .appName("Sample App")
        .getOrCreate()

      val dataFrame = spark.read.option("multiLine", true).json("C:/FlattenDataFrameF/data/phil.coutinho-1.json")


      flattenDataFrame(dataFrame).show()
      flattenDataFrame(dataFrame).printSchema()
    }

}
