package hackathon.dbs.problem.bigdata.utils

import hackathon.dbs.problem.bigdata.schema.TablesSchema
import hackathon.dbs.problem.bigdata.utils.Constants._
import hackathon.dbs.problem.bigdata.utils.SparkUtils._
import org.apache.spark.sql._

object FileDataFrameExtractor {
  def getProductData(spark: SparkSession): DataFrame = {
    readTable(spark, INPUT_FILE_PATH, PRODUCT_FILE, TablesSchema.productSchema)
  }

  def getCustomerData(spark: SparkSession): DataFrame = {
    readTable(spark, INPUT_FILE_PATH, CUSTOMER_FILE, TablesSchema.cusomterSchema)
  }

  def getRefundData(spark: SparkSession): DataFrame = {
    readTable(spark, INPUT_FILE_PATH, REFUND_FILE, TablesSchema.refundSchema)
  }

  def getSalesData(spark: SparkSession): DataFrame = {
    readTable(spark, INPUT_FILE_PATH, SALES_FILE, TablesSchema.salesSchema)
  }
}