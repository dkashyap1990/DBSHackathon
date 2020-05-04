package hackathon.dbs.problem.bigdata.checks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object NullValueChecker {
  val errrorTable=Seq("file","row_no","error")

  def checkForProductTable(dataFrame: DataFrame): DataFrame = {
    val error_df=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("product_id").isNull)
      .select(lit("Product.txt"),col("row_no"),lit("null value in product_id"))
      .toDF("file","row_no","error")
    error_df
  }

  def checkForCustomerTable(dataFrame: DataFrame): DataFrame = {
    val error_df1=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("customer_id").isNull)
      .select(lit("Customer.txt"),col("row_no"),lit("null value in customer_id"))
      .toDF("file","row_no","error")

    val error_df2=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("customer_first_name").isNull)
      .select(lit("Customer.txt"),col("row_no"),lit("null value in customer_first_name"))
      .toDF("file","row_no","error")
    error_df1.union(error_df2)
  }

  def checkForSalesTable(dataFrame: DataFrame): DataFrame = {
    val error_df1=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("transaction_id").isNull)
      .select(lit("Sales.txt"),col("row_no"),lit("null value in transaction_id"))
      .toDF("file","row_no","error")

    val error_df2=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("customer_id").isNull)
      .select(lit("Sales.txt"),col("row_no"),lit("null value in customer_id"))
      .toDF("file","row_no","error")

    val error_df3=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("timestamp").isNull)
      .select(lit("Sales.txt"),col("row_no"),lit("null value in timestamp"))
      .toDF("file","row_no","error")

    val error_df4=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("total_amount").isNull)
      .select(lit("Sales.txt"),col("row_no"),lit("null value in total_amount"))
      .toDF("file","row_no","error")

    val error_df5=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("total_quantity").isNull)
      .select(lit("Sales.txt"),col("row_no"),lit("null value in total_quantity"))
      .toDF("file","row_no","error")

    val error_df6=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("product_id").isNull)
      .select(lit("Sales.txt"),col("row_no"),lit("null value in product_id"))
      .toDF("file","row_no","error")


    error_df1.union(error_df2).union(error_df3).union(error_df4).union(error_df5)
  }

  def checkForRefundTable(dataFrame: DataFrame): DataFrame = {

    val error_df1=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("refund_id").isNull)
      .select(lit("Refund.txt"),col("row_no"),lit("null value in refund_id"))
      .toDF("file","row_no","error")

    val error_df2=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("original_transaction_id").isNull)
      .select(lit("Refund.txt"),col("row_no"),lit("null value in original_transaction_id"))
      .toDF("file","row_no","error")
    val error_df3=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("customer_id").isNull)
      .select(lit("Refund.txt"),col("row_no"),lit("null value in customer_id"))
      .toDF("file","row_no","error")
    val error_df4=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("Product_id").isNull)
      .select(lit("Refund.txt"),col("row_no"),lit("null value in Product_id"))
      .toDF("file","row_no","error")

    val error_df5=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("timestamp").isNull)
      .select(lit("Refund.txt"),col("row_no"),lit("null value in timestamp"))
      .toDF("file","row_no","error")

    val error_df6=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("refund_amount").isNull)
      .select(lit("Refund.txt"),col("row_no"),lit("null value in refund_amount"))
      .toDF("file","row_no","error")

    val error_df7=dataFrame.withColumn("row_no",monotonically_increasing_id())
      .filter(col("refund_quantity").isNull)
      .select(lit("Refund.txt"),col("row_no"),lit("null value in refund_quantity"))
      .toDF("file","row_no","error")

    error_df1
    .union(error_df1)
    .union(error_df2)
    .union(error_df3)
    .union(error_df4)
    .union(error_df5)
    .union(error_df6)
  }


}
