# Databricks notebook source
# MAGIC %md
# MAGIC # Geospatial demo - house price data
# MAGIC
# MAGIC Notebook downloads and processes data from HMLR price-paid sales, combines it with House Price Index data for further analysis.
# MAGIC The intention is to spot suspicious sales, where the house was sold for significantly more or less than expected, given the general trend of prices over time. 
# MAGIC
# MAGIC Dataset is used in the Azure Geocode Address notebook for visualisation

# COMMAND ----------

# MAGIC %md
# MAGIC Download House Price-paid data from HMLR

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog jgcatalog;
# MAGIC use schema geo;

# COMMAND ----------

# MAGIC %sh
# MAGIC if [ ! -f /Volumes/databricks_dev/geo/raw-price-paid/pp-complete.csv ]; then
# MAGIC     wget "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv" -P /tmp/raw-price-paid
# MAGIC else
# MAGIC     echo "File already downloaded and copied into Databricks volume"
# MAGIC fi

# COMMAND ----------

# Copy downloaded file to Databricks volume
import os

temp_file_path = "/tmp/raw-price-paid/pp-complete.csv"
final_file_path = "/Volumes/jgcatalog/geo/geovolume/raw-price-paid/pp-complete.csv"

# Check if the file exists
if os.path.isfile(temp_file_path):
    dbutils.fs.mv(f"file://{temp_file_path}", final_file_path)
else:
    print("File hasn't been downloaded recently so skipping the copy step")
    

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import concat_ws, col, lit, when, expr

# Define the schema
schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Price_paid", IntegerType(), True),
    StructField("Date_of_transfer", StringType(), True),
    StructField("Postcode", StringType(), True),
    StructField("Property_type", StringType(), True),
    StructField("Old_or_new", StringType(), True),
    StructField("Tenure", StringType(), True),
    StructField("House_number_or_name", StringType(), True),
    StructField("Flat", StringType(), True),
    StructField("Street", StringType(), True),
    StructField("Locality", StringType(), True),
    StructField("Town_or_city", StringType(), True),
    StructField("District", StringType(), True),
    StructField("County", StringType(), True),
    StructField("Transaction_category", StringType(), True),
    StructField("Record_status", StringType(), True)
])

price_paid_df = spark.read.csv('/Volumes/jgcatalog/geo/geovolume/raw-price-paid/pp-complete.csv', header=False, schema=schema)

price_paid_df = price_paid_df.withColumn("Price_paid", price_paid_df["Price_paid"].cast(IntegerType()))

price_paid_df = price_paid_df.withColumn("Date_of_transfer", price_paid_df["Date_of_transfer"].cast(DateType()))

# Create a new column called 'Address' by concatenating all the address-related fields
price_paid_df = price_paid_df.withColumn(
    'Address',
    concat_ws(', ',
        col('Flat'),
        concat_ws(' ',
            when(expr("try_cast(House_number_or_name as INT)").isNotNull(),
                 concat_ws(' ',
                           col('House_number_or_name'),
                           col('Street')
                           )
                 ).otherwise(concat_ws(', ',
                                       col('House_number_or_name'),
                                       col('Street')
                                       )
                             )
            ),
            col('Town_or_city'),
            col('Postcode')
        )
    )

price_paid_df.display()

# COMMAND ----------

# Register price_paid_df as a SQL view
price_paid_df.createOrReplaceTempView("price_paid")

# COMMAND ----------

price_paid_df.write.mode("overwrite").saveAsTable("jgcatalog.geo.price_paid_uk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get HPI data. See for initial ingestion: [HPI ingestion notebook](https://adb-1786926766267612.12.azuredatabricks.net/?o=1786926766267612#notebook/3363992674905507)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import concat_ws, col, lit, when, to_date

hpi_df = spark.read.csv('/Volumes/jgcatalog/geo/geovolume/raw-hpi/UK-HPI-full-file-2023-06.csv', header=True, inferSchema=True)

# Convert from string to actual data types where needed
hpi_df = hpi_df.withColumn("Date", to_date(hpi_df["Date"], "dd/MM/yyyy"))

hpi_df.display()

# COMMAND ----------

hpi_df.createOrReplaceTempView("hpi")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join price paid and HPI. Attempt to match on Price paid's town/city first, otherwise on district, otherwise on county

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- town or city matches
# MAGIC create or replace temporary view price_paid_matched_to_hpi_town_or_city as 
# MAGIC select price_paid.*, hpi.regionName as hpi_region_name, hpi.date as hpi_date, hpi.index as hpi_index
# MAGIC from price_paid
# MAGIC inner join hpi on 
# MAGIC lower(trim(price_paid.town_or_city)) = lower(trim(hpi.regionName))
# MAGIC AND
# MAGIC hpi.date = date_format(date_trunc('MONTH', price_paid.date_of_transfer), 'yyyy-MM-01')  -- HPI date is always first of the month

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- district matches
# MAGIC create or replace temporary view price_paid_matched_to_hpi_district as 
# MAGIC select price_paid.*, hpi.regionName as hpi_region_name, hpi.date as hpi_date, hpi.index as hpi_index
# MAGIC from price_paid
# MAGIC inner join hpi on 
# MAGIC lower(trim(price_paid.district)) = lower(trim(hpi.regionName))
# MAGIC AND
# MAGIC hpi.date = date_format(date_trunc('MONTH', price_paid.date_of_transfer), 'yyyy-MM-01')  -- HPI date is always first of the month

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- county matches
# MAGIC create or replace temporary view price_paid_matched_to_hpi_county as 
# MAGIC select price_paid.*, hpi.regionName as hpi_region_name, hpi.date as hpi_date, hpi.index as hpi_index
# MAGIC from price_paid
# MAGIC inner join hpi on 
# MAGIC lower(trim(price_paid.county)) = lower(trim(hpi.regionName))
# MAGIC AND
# MAGIC hpi.date = date_format(date_trunc('MONTH', price_paid.date_of_transfer), 'yyyy-MM-01')  -- HPI date is always first of the month

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- combine all matches, we only want the most detailed match available
# MAGIC create or replace table jgcatalog.geo.price_paid_matched_to_hpi_unique as
# MAGIC select *, 'TOWN_OR_CITY' as match_type from price_paid_matched_to_hpi_town_or_city 
# MAGIC union
# MAGIC select *, 'DISTRICT' as match_type from price_paid_matched_to_hpi_district where id not in (select id from price_paid_matched_to_hpi_town_or_city)
# MAGIC union
# MAGIC select *, "COUNTY" as match_type from price_paid_matched_to_hpi_county where id not in (select id from price_paid_matched_to_hpi_town_or_city) and id not in (select id from price_paid_matched_to_hpi_district)
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from jgcatalog.geo.price_paid_matched_to_hpi_unique

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find earliest and latest sales in 2 given time periods. 

# COMMAND ----------

# MAGIC %md
# MAGIC Get the earliest sale from 2015-2020 and the most recent sale from 2020-present for each property. For properties that had at least one sale in each time period, Subtract the prices to get the difference. Then take the median difference for each postcode for the map. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace table jgcatalog.geo.temp_price_paid_earliest_sale as
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY Address
# MAGIC         ORDER BY
# MAGIC           Date_of_transfer asc
# MAGIC       ) AS rn
# MAGIC     FROM
# MAGIC       jgcatalog.geo.price_paid_matched_to_hpi_unique
# MAGIC     where
# MAGIC       Date_of_transfer >= '2015-01-01'
# MAGIC       and Date_of_transfer <= '2020-12-31'
# MAGIC       and Transaction_category = 'A' -- normal sale only
# MAGIC   ) sub
# MAGIC WHERE
# MAGIC   rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jgcatalog.geo.temp_price_paid_earliest_sale

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC or replace table jgcatalog.geo.temp_price_paid_latest_sale as
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY Address
# MAGIC         ORDER BY
# MAGIC           Date_of_transfer desc -- get the most recent sale in the time period being compared, not the earliest as before
# MAGIC       ) AS rn
# MAGIC     FROM
# MAGIC       price_paid_matched_to_hpi_unique
# MAGIC     where Date_of_transfer > '2020-12-31' -- second period to compare to
# MAGIC     and Transaction_category = 'A' -- normal sale only
# MAGIC     and Address in (select Address from jgcatalog.geo.temp_price_paid_earliest_sale) -- must exist in previously created dataset. Note: Differences in how the address was recorded could throw this off. 
# MAGIC   ) sub
# MAGIC WHERE
# MAGIC   rn = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC to figure out difference between actual and HPI...
# MAGIC
# MAGIC actual latest sale price - expected sale price based on initial sale price and HPI change
# MAGIC
# MAGIC i.e.
# MAGIC
# MAGIC latest sale price paid - (earliest sale price paid * (latest sale hpi - earliest sale hpi))

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table jgcatalog.geo.price_paid_diffs as
# MAGIC select
# MAGIC   temp_price_paid_latest_sale.*,
# MAGIC   temp_price_paid_latest_sale.Date_of_transfer as Latest_sale_date_of_transfer, temp_price_paid_latest_sale.price_paid as Latest_sale_price_paid, temp_price_paid_earliest_sale.Date_of_transfer as Earliest_sale_date_of_transfer, temp_price_paid_earliest_sale.Price_paid as Earliest_sale_price_paid, temp_price_paid_earliest_sale.hpi_index as earliest_sale_hpi_index,
# MAGIC   (
# MAGIC     temp_price_paid_latest_sale.Price_paid - temp_price_paid_earliest_sale.Price_paid
# MAGIC   ) as price_diff_latest_to_earliest_sale,
# MAGIC   (temp_price_paid_earliest_sale.Price_paid * (temp_price_paid_latest_sale.hpi_index / temp_price_paid_earliest_sale.hpi_index)) as expected_price_given_hpi,
# MAGIC   temp_price_paid_latest_sale.Price_paid - (temp_price_paid_earliest_sale.Price_paid * (temp_price_paid_latest_sale.hpi_index / temp_price_paid_earliest_sale.hpi_index)) as diff_actual_price_vs_expected
# MAGIC from
# MAGIC   jgcatalog.geo.temp_price_paid_latest_sale
# MAGIC   inner join jgcatalog.geo.temp_price_paid_earliest_sale on temp_price_paid_latest_sale.Address = temp_price_paid_earliest_sale.Address

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jgcatalog.geo.price_paid_diffs