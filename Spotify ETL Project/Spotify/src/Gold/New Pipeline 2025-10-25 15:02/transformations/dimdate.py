import dlt
from pyspark.sql.functions import col

@dlt.table()
def dimdate_stg():
  df = spark.readStream.table("spotify_data.silver.dimdate")
  return df

dlt.create_streaming_table("spotify_data.gold.dimdate")

dlt.create_auto_cdc_flow(
    target="dimdate",
    source="dimdate_stg",
    keys=["date_key"],
    sequence_by=col("date"),
    ignore_null_updates=False,
    stored_as_scd_type=2,
    name="dimdate_cdc_flow"
)