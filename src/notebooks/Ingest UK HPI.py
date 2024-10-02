# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog jgcatalog;
# MAGIC use schema geo;

# COMMAND ----------

# MAGIC %sh
# MAGIC if [ ! -f /Volumes/databricks_dev/geo/raw-hpi/UK-HPI-full-file-2023-06.csv ]; then
# MAGIC     wget "http://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-2023-06.csv" -P /tmp/raw-hpi --no-check-certificate
# MAGIC else
# MAGIC     echo "File already downloaded and copied into Databricks volume"
# MAGIC fi

# COMMAND ----------

# Copy downloaded file to Databricks volume
import os

temp_file_path = "/tmp/raw-hpi/UK-HPI-full-file-2023-06.csv"
final_file_path = "/Volumes/jgcatalog/geo/geovolume/raw-hpi/UK-HPI-full-file-2023-06.csv"

# Check if the file exists
if os.path.isfile(temp_file_path):
    dbutils.fs.mv(f"file://{temp_file_path}", final_file_path)
else:
    print("File hasn't been downloaded recently so skipping the copy step")
    

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import concat_ws, col, lit, when, to_date

hpi_df = spark.read.csv('/Volumes/jgcatalog/geo/geovolume/raw-hpi/UK-HPI-full-file-2023-06.csv', header=True, inferSchema=True)

# Convert from string to actual data types where needed
hpi_df = hpi_df.withColumn("Date", to_date(hpi_df["Date"], "dd/MM/yyyy"))

hpi_df.printSchema()

hpi_df.display()

# COMMAND ----------

hpi_df.createOrReplaceTempView("hpi")

# COMMAND ----------

hpi_df.write.mode("overwrite").saveAsTable("house_price_index")

# COMMAND ----------

# MAGIC %sql
# MAGIC select regionName, count(*) from hpi.index
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC create temporary view price_paid_matched_to_hpi_town_or_city as 
# MAGIC select price_paid_diffs.*, hpi.regionName as hpi_region_name, hpi.date as hpi_date, hpi.index as hpi_index,
# MAGIC case 
# MAGIC   when (lower(trim(price_paid_diffs.town_or_city)) = lower(trim(hpi.regionName))) then 'TOWN_OR_CITY' 
# MAGIC   when (lower(trim(price_paid_diffs.district)) = lower(trim(hpi.regionName))) then 'DISTRICT'
# MAGIC   when (lower(trim(price_paid_diffs.county)) = lower(trim(hpi.regionName))) then 'COUNTY'
# MAGIC   else 'NONE'
# MAGIC end as match_type
# MAGIC from price_paid_diffs
# MAGIC left join hpi on 
# MAGIC ((lower(trim(price_paid_diffs.town_or_city)) = lower(trim(hpi.regionName)))
# MAGIC or
# MAGIC (lower(trim(price_paid_diffs.district)) = lower(trim(hpi.regionName)))
# MAGIC or 
# MAGIC (lower(trim(price_paid_diffs.county)) = lower(trim(hpi.regionName))))
# MAGIC and
# MAGIC hpi.date = date_format(date_trunc('MONTH', price_paid_diffs.date_of_transfer), 'yyyy-MM-01')  -- HPI date is always first of the month
# MAGIC
# MAGIC where hpi.regionName is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TOWN_OR_CITY match but no others
# MAGIC select count(*) from price_paid_matched_to_hpi where match_type = 'TOWN_OR_CITY'
# MAGIC and id not in (select id from price_paid_matched_to_hpi where match_type = 'DISTRICT')
# MAGIC and id not in (select id from price_paid_matched_to_hpi where match_type = 'COUNTY')

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table price_paid_matched_to_hpi_deduped as
# MAGIC select * from price_paid_matched_to_hpi where match_type = 'TOWN_OR_CITY'
# MAGIC union all
# MAGIC select * from price_paid_matched_to_hpi where match_type = 'DISTRICT' and id not in (select id from price_paid_matched_to_hpi where match_type = 'TOWN_OR_CITY')
# MAGIC union all
# MAGIC select * from price_paid_matched_to_hpi where match_type = 'COUNTY' and id not in (select id from price_paid_matched_to_hpi where match_type = 'TOWN_OR_CITY' or match_type = 'DISTRICT')
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select match_type, count(*) from price_paid_matched_to_hpi_deduped
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from price_paid_matched_to_hpi_deduped

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DISTRICT match but no others
# MAGIC select a.* from 
# MAGIC (select * from price_paid_matched_to_hpi where match_type = 'TOWN_OR_CITY') as town_or_city_query
# MAGIC (select id from price_paid_matched_to_hpi where match_type = 'DISTRICT')
# MAGIC (select id from price_paid_matched_to_hpi where match_type = 'COUNTY')

# COMMAND ----------

# MAGIC %sql
# MAGIC select match_type, count(*) from price_paid_matched_to_hpi
# MAGIC group by 1
# MAGIC