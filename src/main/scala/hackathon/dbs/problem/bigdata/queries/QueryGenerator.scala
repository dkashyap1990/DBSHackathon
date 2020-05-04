package hackathon.dbs.problem.bigdata.queries

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import hackathon.dbs.problem.bigdata.utils.TableColumns._
import org.apache.spark.sql.expressions.Window


object QueryGenerator {
    def getQuery1(spark:SparkSession, product:DataFrame, sales:DataFrame): DataFrame ={
        val productDF=product.select(PRODUCT_ID,PRODUCT_NAME,PRODUCT_TYPE)
        val salesDF=sales.select(col(TIMESTAMP).substr(0,2).as("Month"),col(TIMESTAMP).substr(7,4).as("Year"),
            col(TOTAL_AMOUNT).substr(2,10).as(TOTAL_AMOUNT),col(TOTAL_QUANTITY),col(PRODUCT_ID))

        productDF.join(salesDF,Seq(PRODUCT_ID))
          .withColumn("Sum_total_amount",sum(col(TOTAL_AMOUNT)).over(Window.partitionBy(PRODUCT_ID,PRODUCT_NAME,"Month","Year")))
          .withColumn("Sum_total_quantity",sum(col(TOTAL_QUANTITY)).over(Window.partitionBy(PRODUCT_ID,PRODUCT_NAME,"Month","Year")))
    }

    //Display the distribution of sales by product name and product type
    def getQuery2(spark:SparkSession, refundDf:DataFrame, salesDf:DataFrame,productDf:DataFrame): DataFrame ={
        val df1 = salesDf.join(refundDf, Seq(CUSTOMER_ID,PRODUCT_ID),"left_anti").filter(col(TIMESTAMP).substr(7,4)==="2013")
          .select(PRODUCT_ID)

        productDf.join(df1,Seq(PRODUCT_ID)).select(sum(col(PRODUCT_PRICE).substr(2,10)).as("Total_amount"))
    }

    def getQuery3(spark:SparkSession,salesDf:DataFrame,productDf:DataFrame): DataFrame ={
        val df = productDf.join(salesDf,Seq(PRODUCT_ID) , "left_anti").select(PRODUCT_ID)
        df
    }


    def getQuery4(spark:SparkSession,salesDF:DataFrame,customerDF:DataFrame,refundDF:DataFrame): DataFrame ={
        import spark.implicits._

        val salesTable=salesDF.createOrReplaceTempView("sales")
        val customerTable=customerDF.createOrReplaceTempView("customer")
        val refundTable=refundDF.createOrReplaceTempView("refund")

        val query="select count(*) from (select t.*,count(*) over(partition by grp, customer_id, product_id) as cnt from (select s.*, (row_number() over(partition by s.customer_id order by trans_date))- row_number() over(partition by s.customer_id,s.product_id order by trans_date) as grp from(select s1.*,cast(to_date(from_unixtime(unix_timestamp(timestamp, 'MM/dd/yyyy'))) as date) trans_date from sales s1) s ) t ) t where t.cnt>=2"
       // spark.sql(query)
        spark.sql("select * from sales")
    }


    def getQuery5(spark:SparkSession,salesDF:DataFrame,customerDF:DataFrame,refundDF:DataFrame): DataFrame ={
        import spark.implicits._
        val salesTable=salesDF.createOrReplaceTempView("sales")
        val customerTable=customerDF.createOrReplaceTempView("customer")
        val refundTable=refundDF.createOrReplaceTempView("refund")

        val query="select c.customer_id,c.customer_first_name,c.customer_last_name from (select t.customer_id,dense_rank() over(order by t.purchases) rn from (select s.customer_id, count(*) as purchases from sales s where s.transaction_id not in (select r.original_transaction_id from refund r) and month(to_date(from_unixtime(unix_timestamp(s.timestamp, 'MM/dd/yyyy')))) = 5 and year(to_date(from_unixtime(unix_timestamp(s.timestamp, 'MM/dd/yyyy')))) = 2013 group by s.customer_id) t) t1 join customer c on (t1.customer_id = c.customer_id) where t1.rn=2"
        spark.sql(query)
    }
}
