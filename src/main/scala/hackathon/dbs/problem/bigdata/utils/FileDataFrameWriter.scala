package hackathon.dbs.problem.bigdata.utils

import hackathon.dbs.problem.bigdata.utils.Constants._
import hackathon.dbs.problem.bigdata.utils.SparkUtils._
import org.apache.spark.sql._

object FileDataFrameWriter {

  def saveQuery1Data(dataframe: DataFrame) = {
    writeTable(dataframe, "Query1", OUTPUT_FILE_PATH)
  }

  def saveQuery2Data(dataframe: DataFrame): Unit = {
    writeTable(dataframe, "Query2", OUTPUT_FILE_PATH)
  }

  def saveQuery3Data(dataframe: DataFrame): Unit = {
    writeTable(dataframe, "Query3", OUTPUT_FILE_PATH)
  }

  def saveQuery4Data(dataframe: DataFrame): Unit = {
    writeTable(dataframe, "Query4", OUTPUT_FILE_PATH)
  }

  def saveQuery5Data(dataframe: DataFrame): Unit = {
    writeTable(dataframe, "Query5", OUTPUT_FILE_PATH)
  }

  def saveErrorData(dataframe: DataFrame): Unit = {
    writeTable(dataframe, "ERROR", OUTPUT_FILE_PATH)
  }
}
