package hackathon.dbs.problem.bigdata.schema

import org.apache.spark.sql.types.{StructField, _}

object TablesSchema {
  val productSchema = StructType(
    List(
      StructField("product_id", IntegerType, false),
      StructField("product_name", StringType, true),
      StructField("product_type", StringType, true),
      StructField("product_version", StringType, false),
      StructField("product_price", StringType, true)
    )
  )

  val cusomterSchema = StructType(
    List(
      StructField("customer_id", IntegerType, false),
      StructField("customer_first_name", StringType, true),
      StructField("customer_last_name", StringType, true),
      StructField("phone_number", LongType, false)
    )
  )

  val refundSchema = StructType(
    List(
      StructField("refund_id", IntegerType, false),
      StructField("original_transaction_id", IntegerType, true),
      StructField("customer_id", IntegerType, true),
      StructField("product_id", IntegerType, false),
      StructField("timestamp", StringType, true),
      StructField("refund_amount", StringType, true),
      StructField("refund_quantity", IntegerType, true)
    )
  )

  val salesSchema = StructType(
    List(
      StructField("transaction_id", IntegerType, false),
      StructField("customer_id", IntegerType, true),
      StructField("product_id", IntegerType, true),
      StructField("timestamp", StringType, false),
      StructField("total_amount", StringType, false),
      StructField("total_quantity", IntegerType, false)
    )
  )
}





