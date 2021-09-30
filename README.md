# code-hunt

package org.simplilearn.project2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, date_format, datediff, sum, to_date, to_timestamp, weekofyear}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object simpliproj2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Ecommerce project Application").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val simpleSchema = StructType(Array(
      StructField("Id", StringType, true),
      StructField("Order_status", StringType, true),
      StructField("Order_products_value", DoubleType, true),
      StructField("Order_freight_value", DoubleType, true),
      StructField("Order_items_qty", IntegerType, true),
      StructField("Customer_city", StringType, true),
      StructField("Customer_state", StringType, true),
      StructField("Customer_zip_code_prefix", IntegerType, true),
      StructField("Product_name_length", StringType, true),
      StructField("Product_description_length", StringType, true),
      StructField("Product_photos_qty", StringType, true),
      StructField("Review_score", DoubleType, true),
      StructField("Order_purchase_timestamp", StringType, true),
      StructField("Order_approved_at", StringType, true),
      StructField("Order_delivered_customer_date", StringType, true)
    ))

    val actual_file_df = spark.read.format("csv").schema(simpleSchema).option("header", "true").option("delimiter", ",")
      .load(args(0));

    //val actual_file_df = spark.read.format("csv").schema(simpleSchema).option("header", "true").option("delimiter", ",")
    //  .load(inputfile)

    //change to timestamp
    val timestamp_file_df = actual_file_df.withColumn("Order_purchase_timestamp", to_timestamp(col("Order_purchase_timestamp"), "dd/MM/yy H:mm"))
      .withColumn("Order_approved_at", to_timestamp(col("Order_approved_at"), "MM/dd/yy H:mm"))
      .withColumn("Order_delivered_customer_date", to_timestamp(col("Order_delivered_customer_date"), "dd/MM/yy H:mm"))
    timestamp_file_df.select("*").show()


    // Get the week Of year and day of week
    val weekday_file_df = timestamp_file_df.withColumn("week_of_year", weekofyear(col("Order_purchase_timestamp")))
      .withColumn("week_day_abb", date_format(col("Order_purchase_timestamp"), "E"))
      .withColumn("year", date_format(col("Order_purchase_timestamp"), "yyyy"))

    //Get the total sales for day and week for each customer city
    val total_sales_city_day = weekday_file_df.select("year", "week_of_year", "week_day_abb", "Customer_city", "Order_items_qty")
      .groupBy(col("year"), col("week_of_year"), col("week_day_abb"), col("Customer_city"))
      .agg(sum("Order_items_qty"))
      .orderBy("year", "week_of_year", "week_day_abb")

    val total_sales_city_week = weekday_file_df.select("year", "week_of_year", "Customer_city", "Order_items_qty")
      .groupBy(col("year"), col("week_of_year"), col("Customer_city"))
      .agg(sum("Order_items_qty"))
      .orderBy("year", "week_of_year")

    //Get the total sales for day and week for each customer state
    val total_sales_state_day = weekday_file_df.select("year", "week_of_year", "week_day_abb", "Customer_state", "Order_items_qty")
      .groupBy(col("year"), col("week_of_year"), col("week_day_abb"), col("Customer_state"))
      .agg(sum("Order_items_qty"))
      .orderBy("year", "week_of_year", "week_day_abb")

    val total_sales_state_week = weekday_file_df.select("year", "week_of_year", "Customer_state", "Order_items_qty")
      .groupBy(col("year"), col("week_of_year"), col("Customer_state"))
      .agg(sum("Order_items_qty"))
      .orderBy("year", "week_of_year")

    //Avg of Review score, Freight value, Order approval, Order Delivery time
    val avg_review_score = weekday_file_df.agg(avg("Review_score"))
    val avg_freight_value = weekday_file_df.agg(avg("Order_freight_value"))
    val avg_approval = weekday_file_df.select(datediff(to_date(col("Order_approved_at")), to_date(col("Order_purchase_timestamp"))).alias("Order approval"))
      .filter(col("Order approval").isNotNull)
      .agg(avg("Order approval"))
    val avg_delivery = weekday_file_df.select(datediff(to_date(col("Order_delivered_customer_date")), to_date(col("Order_purchase_timestamp"))).alias("Order delivery"))
      .filter(col("Order delivery").isNotNull)
      .agg(avg("Order delivery"))

    total_sales_state_day.show()
    total_sales_state_week.show()
    total_sales_city_day.show()
    total_sales_city_week.show()
    avg_review_score.show()
    avg_freight_value.show()
    avg_approval.show()
    avg_delivery.show()


    total_sales_state_day.coalesce(1).write.option("header", "true").csv(args(1) + "daytotalsalescity")
    total_sales_city_day.coalesce(1).write.option("header", "true").csv(args(1) + "daytotalsalesstate")
    total_sales_state_week.coalesce(1).write.option("header", "true").csv(args(1) + "weektotalsalescity")
    total_sales_city_week.coalesce(1).write.option("header", "true").csv(args(1) + "weektotalsalesstate")
    avg_review_score.write.option("header", "true").csv(args(1) + "avgreviewscore")
    avg_freight_value.write.option("header", "true").csv(args(1) + "avgfreightvalue")
    avg_approval.write.option("header", "true").csv(args(1) + "avgapproval")
    avg_delivery.write.option("header", "true").csv(args(1) + "avgdelivery")

    total_sales_state_day.coalesce(1).write.option("header", "true").csv(args(2) + "daytotalsalescity")
    total_sales_city_day.coalesce(1).write.option("header", "true").csv(args(2) + "daytotalsalesstate")
    total_sales_state_week.coalesce(1).write.option("header", "true").csv(args(2) + "weektotalsalescity")
    total_sales_city_week.coalesce(1).write.option("header", "true").csv(args(2) + "weektotalsalesstate")
    avg_review_score.write.option("header", "true").csv(args(2) + "avgreviewscore")
    avg_freight_value.write.option("header", "true").csv(args(2) + "avgfreightvalue")
    avg_approval.write.option("header", "true").csv(args(2) + "avgapproval")
    avg_delivery.write.option("header", "true").csv(args(2) + "avgdelivery")

    spark.stop()
  }


  }
