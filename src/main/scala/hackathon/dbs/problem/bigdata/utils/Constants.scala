package hackathon.dbs.problem.bigdata.utils

object Constants {
  //spark specific
  val SPARK_APP_NAME = "Dbs Hackathon Bigdata Driver"
  val SPARK_EXECUTOR_CORE = 4
  val SPARK_EXECUTOR_MEMORY = "1G"
  val MASTER = "local" // local/yarn
  val MODE = "cluster"
  val DEFAULT_PARTITON_NUM = 2


  // file specific
  val INPUT_FILE_FORMAT = "csv"
  val INPUT_FILE_DELIM = "|"
  val INPUT_FILE_PATH = "/Users/xenon/Desktop/dbs_hackathon"
  val OUTPUT_FILE_PATH = "/Users/xenon/output"
  val PRODUCT_FILE = "Product.txt"
  val CUSTOMER_FILE = "Customer.txt"
  val REFUND_FILE = "Refund.txt"
  val SALES_FILE = "Sales.txt"


  val OUTPUT_FILE_FORMAT = "parquet"
  val FILE_READ_MODE = "failfast"
  val FILE_WRITE_MODE = "overwrite"

}
