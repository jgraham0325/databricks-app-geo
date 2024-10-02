# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM jgcatalog.geo.price_paid_diffs ORDER BY ABS(diff_actual_price_vs_expected) DESC LIMIT 10

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import functions as F
import requests
from pyspark.sql.types import StructType, StructField, FloatType

# Read data from databricks_dev.geo.price_paid_diffs
df = spark.sql(
    "SELECT * FROM jgcatalog.geo.price_paid_diffs ORDER BY ABS(diff_actual_price_vs_expected) DESC LIMIT 50"
)

# Define Azure Maps API endpoint and API key
url_prefix = "https://atlas.microsoft.com/geocode?api-version=2023-06-01"
api_key = "<<fill in>>"

# Function to call Azure Maps API and get latitude and longitude
def get_coordinates(address):
    # Prepare the request parameters
    params = {"subscription-key": api_key}

    url = f"{url_prefix}&addressLine={address}&countryRegion=GB" 
    # Make the API request
    response = requests.get(url, params=params)

    # Parse the response and extract latitude and longitude
    data = response.json()
    # print(data)
    # Extract latitude and longitude from geocodePoints
    latitude = data['features'][0]['properties']['geocodePoints'][0]['geometry']['coordinates'][1]
    longitude = data['features'][0]['properties']['geocodePoints'][0]['geometry']['coordinates'][0]

    # Return latitude and longitude
    return latitude, longitude


# Define UDF to call get_coordinates function
get_coordinates_udf = F.udf(
    get_coordinates,
    StructType(
        [StructField("latitude", FloatType()), StructField("longitude", FloatType())]
    ),
)

# Add latitude and longitude columns using the UDF
df_with_coordinates = df.withColumn(
    "latitude", get_coordinates_udf(df.Address).getField("latitude")
).withColumn("longitude", get_coordinates_udf(df.Address).getField("longitude"))

df_with_coordinates.display()
# Save the dataframe with latitude and longitude into a new table
df_with_coordinates.write.mode("overwrite").saveAsTable("jgcatalog.geo.price_paid_diffs_with_coordinates")

# COMMAND ----------

# MAGIC %md
# MAGIC Custom format for marker popup in map visualisation:
# MAGIC {{Address}} ({{latitude}},{{longitude}})<br/>
# MAGIC <br/>
# MAGIC <b>Latest sale</b><br/>
# MAGIC Date: {{Latest_sale_date_of_transfer}}<br/>
# MAGIC Price paid: £{{Latest_sale_price_paid}}<br/>
# MAGIC HPI: {{hpi_index}}<br/>
# MAGIC <br/>
# MAGIC <b>Earliest Sale</b><br/>
# MAGIC Date: {{Earliest_sale_date_of_transfer}}<br/>
# MAGIC Price paid: £{{Earliest_sale_price_paid}}<br/>
# MAGIC HPI: {{earliest_sale_hpi_index}}<br/>
# MAGIC <br/>
# MAGIC <b>Calculations</b>:<br/>
# MAGIC price_diff_latest_to_earliest_sale: £{{price_diff_latest_to_earliest_sale}}<br/>
# MAGIC expected_price_given_hpi: £{{expected_price_given_hpi}}<br/>
# MAGIC diff_actual_price_vs_expected: £{{diff_actual_price_vs_expected}}<br/>
# MAGIC percentage_diff_actual_from_expected: {{percentage_diff_actual_from_expected}}
# MAGIC <br/>
# MAGIC <A target="_blank" HREF="http://maps.google.com/maps?q=&layer=c&cbll={{latitude}},{{longitude}}">Street view<A>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW jgcatalog.geo.price_paid_diffs_with_coordinates_view AS
# MAGIC SELECT
# MAGIC   ID,
# MAGIC   TO_CHAR(Price_paid, '999,999,999,999') AS Price_paid,
# MAGIC   DATE_FORMAT(Date_of_transfer, 'dd/MM/yyyy') AS Date_of_transfer,
# MAGIC   Postcode,
# MAGIC   Property_type,
# MAGIC   Old_or_new,
# MAGIC   Tenure,
# MAGIC   House_number_or_name,
# MAGIC   Flat,
# MAGIC   Street,
# MAGIC   Locality,
# MAGIC   Town_or_city,
# MAGIC   District,
# MAGIC   County,
# MAGIC   Transaction_category,
# MAGIC   Record_status,
# MAGIC   Address,
# MAGIC   hpi_region_name,
# MAGIC   DATE_FORMAT(hpi_date, 'dd/MM/yyyy') AS hpi_date,
# MAGIC   hpi_index,
# MAGIC   match_type,
# MAGIC   rn,
# MAGIC   DATE_FORMAT(Latest_sale_date_of_transfer, 'dd/MM/yyyy') AS Latest_sale_date_of_transfer,
# MAGIC   TO_CHAR(Latest_sale_price_paid, '999,999,999,999') AS Latest_sale_price_paid,
# MAGIC   DATE_FORMAT(Earliest_sale_date_of_transfer, 'dd/MM/yyyy') AS Earliest_sale_date_of_transfer,
# MAGIC   TO_CHAR(Earliest_sale_price_paid, '999,999,999,999') AS Earliest_sale_price_paid,
# MAGIC   earliest_sale_hpi_index,
# MAGIC   TO_CHAR(price_diff_latest_to_earliest_sale :: bigint, '999,999,999,999') AS price_diff_latest_to_earliest_sale,
# MAGIC   TO_CHAR(expected_price_given_hpi :: bigint, '999,999,999,999') AS expected_price_given_hpi,
# MAGIC   TO_CHAR(diff_actual_price_vs_expected :: bigint, '999,999,999,999') AS diff_actual_price_vs_expected,
# MAGIC   ROUND(((Latest_sale_price_paid - expected_price_given_hpi) / Latest_sale_price_paid) * 100, 2) as percentage_diff_actual_from_expected,  
# MAGIC   CASE 
# MAGIC     WHEN percentage_diff_actual_from_expected > 0 and percentage_diff_actual_from_expected <= 49 THEN '0% to 49%' 
# MAGIC     WHEN percentage_diff_actual_from_expected > 49 and percentage_diff_actual_from_expected <= 100 THEN '50% to 100%' 
# MAGIC     WHEN percentage_diff_actual_from_expected > 100 and percentage_diff_actual_from_expected <= 200 THEN '100% to 200%' 
# MAGIC     WHEN percentage_diff_actual_from_expected > 200  THEN 'more than 200%' 
# MAGIC     WHEN percentage_diff_actual_from_expected < 0 and percentage_diff_actual_from_expected >= -49 THEN '-49% to -1%'
# MAGIC     WHEN percentage_diff_actual_from_expected < -49 and percentage_diff_actual_from_expected >= -100 THEN '-100% to -49%'
# MAGIC     WHEN percentage_diff_actual_from_expected < -100 and percentage_diff_actual_from_expected >= -200 THEN '-200% to -100%'
# MAGIC     WHEN percentage_diff_actual_from_expected < -200 THEN 'less than -200%'
# MAGIC     ELSE 'unknown' END as percentage_diff_expected_actual,
# MAGIC   latitude,
# MAGIC   longitude
# MAGIC FROM
# MAGIC   jgcatalog.geo.price_paid_diffs_with_coordinates;