package hackathon.dbs.problem.bigdata.utils

import hackathon.dbs.problem.bigdata.utils.Constants._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkUtils {
  private  def initiateSparkConf(): SparkConf = {
    new SparkConf()
      .setAppName(SPARK_APP_NAME)
      .setMaster(MASTER)
  }

  def initiateSparkSession(): SparkSession = {
    val conf = initiateSparkConf()
    SparkSession.builder().config(conf).getOrCreate()
  }

  def readTable(spark: SparkSession, filePath: String, fileName: String, schema: StructType): DataFrame = {
    spark.read.format(INPUT_FILE_FORMAT)
      .schema(schema)
      .option("delimiter", INPUT_FILE_DELIM)
      .option("mode", FILE_READ_MODE)
      .option("inferSchema", true)
      .load(filePath + "/" + fileName)
      .toDF()
  }

  def writeTable(dataFrame: DataFrame, fileName: String, filePath: String): Unit = {
    dataFrame.show(10)
    println(filePath + "/"+fileName)
    dataFrame.write.format(OUTPUT_FILE_FORMAT).mode("overwrite")
      .save(filePath + "/" + fileName)

    //dataFrame.write.parquet(filePath + "/"+fileName)
  }
}
