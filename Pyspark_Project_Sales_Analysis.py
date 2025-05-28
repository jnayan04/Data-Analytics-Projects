# Databricks notebook source
# Location for sales analysis:-  /FileStore/tables/sales_csv.txt

#Location for menu analysis:-  /FileStore/tables/menu_csv.txt


# COMMAND ----------

# MAGIC %md
# MAGIC #Sales DataFrame:-
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

schema= StructType([

    StructField("product_id",IntegerType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("location",StringType(),True),
    StructField("source_order",StringType(),True),
])

# COMMAND ----------

sales_df=spark.read.format("csv").option("inferSchema",True).schema(schema).load("/FileStore/tables/sales_csv.txt")

display(sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Adding Year,Month,Quarter:-

# COMMAND ----------

from pyspark.sql.functions import month,year,quarter

sales_df=sales_df.withColumn("order_year",year(sales_df.order_date)).withColumn("order_quarter",quarter(sales_df.order_date)).withColumn("order_month",month(sales_df.order_date))

display(sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Menu DataFrame:-

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

schema= StructType([

    StructField("product_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("price",StringType(),True)
])

menu_df=spark.read.format("csv").option("inferSchema",True).schema(schema).load("/FileStore/tables/menu_csv.txt")

# COMMAND ----------

display(menu_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Total Amount Spent By Each Customer:-

# COMMAND ----------

total_amount_spent= (sales_df.join(menu_df,'product_id').groupBy('customer_id').agg({'price':'sum'}).orderBy('customer_id'))

# COMMAND ----------

display(total_amount_spent)

# COMMAND ----------

# MAGIC %md
# MAGIC #Total amount spent on each Food Category:-

# COMMAND ----------

total_amount_spent_fc= (sales_df.join(menu_df,'product_id').groupBy('product_name').agg({'price':'sum'}).orderBy('sum(price)'))

# COMMAND ----------

display(total_amount_spent_fc)

# COMMAND ----------

# MAGIC %md
# MAGIC #Total amount of sales in each month:-

# COMMAND ----------

total_sales_month= (sales_df.join(menu_df,'product_id').groupBy('order_month').agg({'price':'sum'}).orderBy("order_month"))

# COMMAND ----------

display(total_sales_month)

# COMMAND ----------

# MAGIC %md
# MAGIC #Yearly sale:-

# COMMAND ----------

total_sales_year= (sales_df.join(menu_df,'product_id').groupBy('order_year').agg({'price':'sum'}).orderBy("order_year"))

# COMMAND ----------

display(total_sales_year)

# COMMAND ----------

# MAGIC %md
# MAGIC #Quarterly Sales:-

# COMMAND ----------

total_sales_quarter= (sales_df.join(menu_df,'product_id').groupBy('order_quarter').agg({'price':'sum'}).orderBy("order_quarter"))

# COMMAND ----------

display(total_sales_quarter)

# COMMAND ----------

# MAGIC %md
# MAGIC #How many times each product is purchased:-

# COMMAND ----------

from pyspark.sql.functions import count

count_prod= (sales_df.join(menu_df,'product_id').groupBy('product_id','product_name').agg(count('product_id').alias('product_count'))).orderBy('product_count',ascending=0).drop('product_id')

# COMMAND ----------

display(count_prod)

# COMMAND ----------

# MAGIC %md
# MAGIC #Top ordered item:-

# COMMAND ----------

from pyspark.sql.functions import count

count_prod= (sales_df.join(menu_df,'product_id').groupBy('product_id','product_name').agg(count('product_id').alias('product_count'))).orderBy('product_count',ascending=0).drop('product_id').limit(1)

# COMMAND ----------

display(count_prod)

# COMMAND ----------

# MAGIC %md
# MAGIC #Frequency of customer visiting restaurant:-

# COMMAND ----------

from pyspark.sql.functions import countDistinct

sales_filtered=(sales_df.filter(sales_df.source_order=='Restaurant').groupBy('customer_id').agg(countDistinct('order_date')))

# COMMAND ----------

display(sales_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC #Total sales country wise:-

# COMMAND ----------

display(sales_df)

# COMMAND ----------

from pyspark.sql.functions import sum

sales_country= (sales_df.join(menu_df,'product_id').groupBy('location').agg(sum('price').alias('total_sales'))).orderBy('total_sales',ascending=0)

# COMMAND ----------

display(sales_country)

# COMMAND ----------

# MAGIC %md
# MAGIC #Total sales by order_source:-

# COMMAND ----------

sales_source= (sales_df.join(menu_df,'product_id').groupBy('source_order').agg(sum('price').alias('total_sales'))).orderBy('total_sales',ascending=0)

# COMMAND ----------

display(sales_source)