import dlt
from pyspark.sql.functions import col

@dlt.table()
def factstream_stg():
  df = spark.readStream.table("spotify_data.silver.factstream")
  return df

dlt.create_streaming_table("spotify_data.gold.factstream")

dlt.create_auto_cdc_flow(
    target="factstream",
    source="factstream_stg",
    keys=["stream_id"],
    sequence_by=col("stream_timestamp"),
    ignore_null_updates=False,
    stored_as_scd_type=1,
    name="factstream_cdc_flow"
)