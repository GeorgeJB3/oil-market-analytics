from pyspark.sql.functions import *
from pyspark.sql.window import Window


SCHEMA_NAME = "oil_analytics"
GOLD_TABLE_NAME = "gold_oil_market_features"


def prepare_index_features(df, name: str, date_col="Date"):
    """
    Prepare daily index features: price and 1-day return.

    Args:
        df (DataFrame): Raw index DataFrame with date and close columns
        name (str): Index short name, used for column prefix (e.g., 'sp500', 'dxy')
        date_col (str): Name of the date column in df
        price_col (str): Name of the price column in df

    Returns:
        DataFrame: DataFrame with columns: date, <name>_close, <name>_return
    """
    window_spec = Window.orderBy(f"{name}_date")

    df_clean = (
        df
        .select(col(date_col).alias(f"{name}_date"), col("Close").cast("double").alias(f"{name}_close"))
        .withColumn(f"{name}_prev_close", lag(col(f"{name}_close")).over(window_spec))
        .withColumn(f"{name}_return", ((col(f"{name}_close") - col(f"{name}_prev_close")) / col(f"{name}_prev_close")) * 100)
        .drop(f"{name}_prev_close")
        .orderBy(desc(f"{name}_date"))
    )
    return df_clean



energy_df = spark.table("oil_analytics.silver_energy_prices")
sp500_df = spark.table("oil_analytics.silver_sp500")
dxy_df = spark.table("oil_analytics.silver_dollar_index")

pivot_df = energy_df.groupBy("date").pivot("code").agg(first("spot_price").alias("price"))

window_spec = Window.orderBy("date")

pivot_df = pivot_df \
    .withColumnRenamed("BRENT_CRUDE_USD", "brent_crude_usd") \
    .withColumnRenamed("WTI_USD", "wti_usd") \
    .withColumnRenamed("NATURAL_GAS_USD", "natural_gas_usd")

commodities = {
  "brent": "brent_crude_usd",
  "wti": "wti_usd",
  "natgas": "natural_gas_usd"
}

for commodity, column in commodities.items():
    pivot_df = (
        pivot_df
        # Add column to show previous days price and percent change
        .withColumn(f"prev_{commodity}_price", lag(col(column)).over(window_spec)) 
        .withColumn(f"{commodity}_return", ((col(column) - col(f"prev_{commodity}_price")) / col(f"prev_{commodity}_price")) * 100) 
        # Create columns for 7 day and 30 day average price
        .withColumn(f"{commodity}_7d_avg", avg(column).over(window_spec.rowsBetween(-6, 0))) 
        .withColumn(f"{commodity}_30d_avg", avg(column).over(window_spec.rowsBetween(-29, 0)))
        .drop(f"prev_{commodity}_price")
    )

pivot_df = pivot_df \
    .withColumn("brent_wti_spread", col("brent_crude_usd").cast("double") - col("wti_usd").cast("double")) \
    .orderBy(desc("date"))
   

sp500_df = prepare_index_features(sp500_df, "sp500")
dxy_df = prepare_index_features(dxy_df, "dxy")

joined_df = pivot_df.join(sp500_df, pivot_df.date == sp500_df.sp500_date, "left")
joined_df = joined_df.join(dxy_df, joined_df.date == dxy_df.dxy_date, "left")
joined_df = joined_df.drop("sp500_date", "dxy_date")

commodities = ["brent_return", "wti_return", "natgas_return"]
indices = ["sp500_return", "dxy_return"]

rolling_30d = Window.orderBy("date").rowsBetween(-29, 0)

gold_om_feat_df = joined_df

# Loop over commodities and indices to add rolling correlations
for commodity in commodities:
    for index in indices:
        col_name = f"{commodity.split('_')[0]}_vs_{index.split('_')[0]}_corr_30d"
        gold_om_feat_df = gold_om_feat_df.withColumn(col_name, corr(commodity, index).over(rolling_30d))

gold_om_feat_df = gold_om_feat_df.orderBy(desc("date"))

gold_columns = [
    "date",
    "brent_crude_usd", "brent_return", "brent_7d_avg", "brent_30d_avg",
    "wti_usd", "wti_return", "wti_7d_avg", "wti_30d_avg",
    "natural_gas_usd", "natgas_return", "natgas_7d_avg", "natgas_30d_avg",
    "brent_wti_spread",
    "sp500_close", "sp500_return",
    "dxy_close", "dxy_return",
    "brent_vs_sp500_corr_30d", "wti_vs_sp500_corr_30d",
    "brent_vs_dxy_corr_30d", "wti_vs_dxy_corr_30d"
]

gold_om_feat_df = gold_om_feat_df.select(*gold_columns)

try:
    gold_om_feat_df.write.mode("append").saveAsTable(f"{SCHEMA_NAME}.{GOLD_TABLE_NAME}")
    print(f"Created gold table {SCHEMA_NAME}.{GOLD_TABLE_NAME}")
except AnalysisException as ae:
    print(f"Analysis error when saving gold table: {ae}")
except Exception as e:
    print(f"Unexpected error saving gold table {SCHEMA_NAME}.{GOLD_TABLE_NAME}: {e}")

