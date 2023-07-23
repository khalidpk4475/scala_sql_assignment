
package org.itc.com

import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.mapred
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.itc.com.sql_Demo.spark.sqlContext

import java.sql.{Connection, DriverManager}
object sql_Demo extends App{
  System.setProperty("hadoop.home.dir", "C:\\hadoop")

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample.com")
    .getOrCreate()

  // stored in a MySQL database.
  val url = "jdbc:mysql://localhost:3306/mydatabase?user=root&password=pass"
  val conn = sqlContext.read.format("jdbc").option("url",url)





  val members_df = conn.option("dbtable", "members").load()
  val menu_df = conn.option("dbtable", "menu").load()
  val sales_df = conn.option("dbtable", "sales").load()

  //members_df.show()
  //menu_df.show()
  //sales_df.show()

  // 1. What is the total amount each customer spent at the restaurant?
  val df1 = sales_df.join(menu_df, sales_df.col("product_id") === menu_df.col("product_id"))
  df1.groupBy("customer_id").sum("price").show(false)

  // 2. How many days has each customer visited the restaurant?
  //val df2 = members_df.join(sales_df, members_df.col("customer_id") === sales_df.col("customer_id"))
  val df2 = members_df.join(sales_df,Seq("customer_id"),"inner")
  val df3 = df2.select("customer_id","order_date").distinct()
  df3.groupBy("customer_id").agg(count("customer_id").alias("Number of Visits")).show()

  // 3. What was the first item from the menu purchased by each customer?
  val windowSpec = Window.partitionBy("customer_id").orderBy(asc("order_date"))
  val df4 = menu_df.join(sales_df, Seq("product_id"), "inner")
  val df5 = df4.groupBy("customer_id","product_name").agg(min("order_date").alias("order_date"))
  val df6 = df5.withColumn("rank", dense_rank().over(windowSpec))
  df6.select("customer_id", "product_name", "order_date").where(col("rank") === 1).show()

  // 4. What is the most purchased item on the menu and how many times was it purchased by all customers?
  val df7 = menu_df.join(sales_df, Seq("product_id"), "inner")
  val df8 = df7.groupBy( "product_name").agg(count("product_id").alias("Number of Time"))
  val df9 = df8.withColumn("rank", dense_rank().over(Window.orderBy(desc("Number of Time"))))
  df9.select("product_name", "Number of Time").where(col("rank") === 1).show()

  //  Which item was the most popular for each customer?
  val windowSpec1 = Window.partitionBy("customer_id").orderBy(desc("top_product"))
  val df10 = menu_df.join(sales_df, Seq("product_id"), "inner")
  val df11 = df10.groupBy("customer_id", "product_name").agg(count("customer_id").alias("top_product"))
  val df12 = df11.withColumn("rank", dense_rank().over(windowSpec1))
  df12.select("customer_id", "product_name").where(col("rank") === 1).show()

  //val resultDF: DataFrame = spark.sql("SELECT * FROM members")
  members_df.createTempView("members")
  menu_df.createTempView("menu")
  sales_df.createTempView("sales")

  // Which item was purchased first by the customer after they became a member?
  val sqlQuery: String = "select customer_id, product_name, order_date " +
    "from( select sales.customer_id, sales.product_id, menu.product_name, members.join_date, sales.order_date," +
    " DENSE_RANK() OVER (PARTITION BY members.customer_id ORDER BY sales.order_date) AS dense from members" +
    "  inner join sales  on members.customer_id = sales.customer_id  " +
    "inner join menu  on sales.product_id = menu.product_id  where sales.order_date >= members.join_date" +
    "  order by members.customer_id, sales.order_date) subtable  where dense = 1"

  val resultDF: DataFrame = spark.sql(sqlQuery) // Execute the SQL query using spark.sql method
  resultDF.show() // Show the results

  // 7. Which item was purchased just before the customer became a member?
  val sqlQuery1: String = "select customer_id, product_name, order_date from" +
    "(select sales.customer_id, sales.product_id, menu.product_name, members.join_date, sales.order_date," +
    " DENSE_RANK() OVER (PARTITION BY members.customer_id ORDER BY sales.order_date DESC) AS dense from members" +
    "  inner join sales  on members.customer_id = sales.customer_id  inner join menu  on " +
    "sales.product_id = menu.product_id  where sales.order_date < members.join_date  order by members.customer_id, " +
    "sales.order_date) subtable  where dense = 1"

  val resultDF1: DataFrame = spark.sql(sqlQuery1) // Execute the SQL query using spark.sql method
  resultDF1.show() // Show the results

  //8. What is the total items and amount spent for each member before they became a member?
  val sqlQuery2: String = " select sales.customer_id, count(sales.product_id) as Total_Item, sum(menu.price) as Total_Amount " +
    "from members  inner join sales  on members.customer_id = sales.customer_id  inner join menu  on " +
    "sales.product_id = menu.product_id  where sales.order_date < members.join_date  group by 1"

  val resultDF2: DataFrame = spark.sql(sqlQuery2) // Execute the SQL query using spark.sql method
  resultDF2.show() // Show the results

  // -- 9. If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?
  val sqlQuery3: String = "select customer_id, sum(Total_Points) as total_points from( select sales.customer_id," +
    " case  when menu.product_name = 'sushi'  then menu.price * 20 else menu.price * 10 end as Total_Points from sales" +
    "  inner join menu  on sales.product_id = menu.product_id) subtable  group by 1"

  val resultDF3: DataFrame = spark.sql(sqlQuery3) // Execute the SQL query using spark.sql method
  resultDF3.show() // Show the results

  //  10. In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?
  val sqlQuery4: String = " select sales.customer_id, count(sales.product_id) as Total_Item, sum(menu.price * 20) as Total_Points " +
    "from members  inner join sales  on members.customer_id = sales.customer_id  inner join menu  on " +
    "sales.product_id = menu.product_id  where sales.order_date between members.join_date and " +
    "DATE_ADD(members.join_date, INTERVAL 7 DAY)  group by 1"

  val resultDF4: DataFrame = spark.sql(sqlQuery3) // Execute the SQL query using spark.sql method
  resultDF4.show() // Show the results
}
