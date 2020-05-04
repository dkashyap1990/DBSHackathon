package hackathon.dbs.problem.bigdata.driver

import hackathon.dbs.problem.bigdata.queries.QueryGenerator
import hackathon.dbs.problem.bigdata.transformations.DataQualityVerification
import hackathon.dbs.problem.bigdata.utils.{FileDataFrameExtractor, FileDataFrameWriter, SparkUtils}

object Driver {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.initiateSparkSession()

    spark.sparkContext.setLogLevel("ERROR")

    val productDF = FileDataFrameExtractor.getProductData(spark)
    val customerDF = FileDataFrameExtractor.getCustomerData(spark)
    val refundDF = FileDataFrameExtractor.getRefundData(spark)
    val salesDF = FileDataFrameExtractor.getSalesData(spark)

    val errorNullValues = DataQualityVerification.verify(productDF, customerDF, salesDF, refundDF)
    errorNullValues.show(10)

    val query1 = QueryGenerator.getQuery1(spark, productDF, salesDF)
    val query2 = QueryGenerator.getQuery2(spark, refundDF, salesDF, productDF)
    val query3 = QueryGenerator.getQuery3(spark, salesDF, productDF)


    val query4 = QueryGenerator.getQuery4(spark, salesDF, customerDF, refundDF)
    val query5 = QueryGenerator.getQuery5(spark, salesDF, customerDF, refundDF)

    salesDF.show(10)

    query1.show(10)
    query2.show(10)
    query3.show(10)
    query4.show(10)
    query5.show(10)


    FileDataFrameWriter.saveErrorData(errorNullValues)
    FileDataFrameWriter.saveQuery1Data(query1)
    FileDataFrameWriter.saveQuery1Data(query2)
    FileDataFrameWriter.saveQuery1Data(query3)
    FileDataFrameWriter.saveQuery1Data(query4)
    FileDataFrameWriter.saveQuery1Data(query5)
  }
}
