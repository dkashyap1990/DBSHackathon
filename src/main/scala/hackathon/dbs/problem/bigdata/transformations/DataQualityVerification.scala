package hackathon.dbs.problem.bigdata.transformations

import hackathon.dbs.problem.bigdata.checks.NullValueChecker
import org.apache.spark.sql.DataFrame

object DataQualityVerification {
  def verify(product: DataFrame, customer: DataFrame, sales: DataFrame, refund: DataFrame): DataFrame = {
    val df1 = NullValueChecker.checkForProductTable(product)
    val df2 = NullValueChecker.checkForCustomerTable(customer)
    val df3 = NullValueChecker.checkForSalesTable(sales)
    val df4 = NullValueChecker.checkForRefundTable(refund)

    df1.union(df2).union(df3).union(df4)
  }
}
