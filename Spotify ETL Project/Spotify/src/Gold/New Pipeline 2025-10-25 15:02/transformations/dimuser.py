import dlt
from pyspark.sql.functions import col

@dlt.table()
def dimuser_stg():
  df = spark.readStream.table("spotify_data.silver.dimuser")
  return df

dlt.create_streaming_table("spotify_data.gold.dimuser")

dlt.create_auto_cdc_flow(
    target="dimuser",
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by=col("updated_at"),
    ignore_null_updates=False,
    stored_as_scd_type=2,
    name="dimuser_cdc_flow"
)