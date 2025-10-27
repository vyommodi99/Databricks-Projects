import dlt
from pyspark.sql.functions import col

@dlt.table()
def dimartist_stg():
  df = spark.readStream.table("spotify_data.silver.dimartist")
  return df

dlt.create_streaming_table("spotify_data.gold.dimartist")

dlt.create_auto_cdc_flow(
    target="dimartist",
    source="dimartist_stg",
    keys=["artist_id"],
    sequence_by=col("updated_at"),
    ignore_null_updates=False,
    stored_as_scd_type=2,
    name="dimartist_cdc_flow"
)