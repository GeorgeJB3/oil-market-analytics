from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException


SCHEMA_NAME = "oil_analytics"

GOLD_GDP_TABLE = "macro_gdp_daily"
GOLD_CPI_TABLE = "macro_inflation_daily"
GOLD_UNEM_TABLE = "macro_unemployment_daily"
GOLD_INTEREST_TABLE = "macro_interest_daily"

# START_DATE = "2020-01-01"
# END_DATE   = "2026-12-31" # change to current day

def build_calendar_df(spark, start_dt="2020-01-01", end_dt="2026-12-31"):
    """
    Generate a Spark DataFrame containing a continuous range of calendar dates.

    This function creates a single-column DataFrame with one row per date
    between the specified start and end dates (inclusive).

    Args:
        spark (SparkSession): Active Spark session used to create the DataFrame.
        start_dt: Start date of the calendar range in 'YYYY-MM-DD' format (default is '2020-01-01').
        end_dt: End date of the calendar range in 'YYYY-MM-DD' format (default is '2026-12-31').

    Returns:
        DataFrame with a single column: calendar_date (date)
            Continuous sequence of dates from start_dt to end_dt.
    """
    df = (
        spark
        .createDataFrame([(start_dt, end_dt)], ["start_date", "end_date"])
        .withColumn("calendar_date", explode(sequence(to_date(lit(start_dt)),to_date(lit(end_dt)))))
        .select("calendar_date")
    )
    return df


def create_gold_daily(spark, df, calendar_df, col_name:str, date_col="date"):
    """
    Aligns source dataframe to a daily calendar and forward-fills values.

    df: source Silver dataframe
    calendar_df: calendar dataframe with column calendar_date
    col_name: column in dataframe to forward-fill
    date_col: silver dataframe date column default= date
    """

    df = (
    calendar_df
    .join(df, calendar_df.calendar_date == df[date_col], "left")
    .withColumn(col_name, last(col_name, ignorenulls=True)
        .over(Window.orderBy("calendar_date")))
    .select(calendar_df.calendar_date.alias("date"), col_name)
    )
    return df


def create_gold_interest(df, window, threshold=0.05):
    """
    Create a gold-layer interest rate feature table with rate movement flags.

    This function derives month-over-month interest rate changes for UK and US rates, and classifies them into hike, cut, or hold signals based on a configurable threshold.
    The resulting DataFrame is written to a gold table.

    Args:
        df (DataFrame): Input Spark DataFrame containing interest rate columns.
        window (WindowSpec): Spark window specification used to calculate lagged values.
        threshold (float, optional): Minimum change required to classify a rate move
            as a hike or cut (default is 0.05).

    Returns:
        None: Writes the transformed DataFrame to a persistent gold table.

    Raises:
        AnalysisException: If there is an error related to the Spark SQL execution
            or table write operation.
        Exception: For any unexpected errors encountered during the write process.
    """

    gold_df = (
        df
        .withColumn("uk_prev_month", lag("uk_interest_rate_3m", 31).over(window)) 
        .withColumn("us_prev_month", lag("us_interest_rate_1m", 31).over(window)) 
        .withColumn("uk_rate_hike_flag", when(col("uk_interest_rate_3m") > col("uk_prev_month") + threshold, 1).otherwise(0)) 
        .withColumn("us_rate_hike_flag", when(col("us_interest_rate_1m") > col("us_prev_month") + threshold, 1).otherwise(0)) 
        .withColumn("uk_rate_cut_flag", when(col("uk_interest_rate_3m") < col("uk_prev_month") - threshold, 1).otherwise(0)) 
        .withColumn("us_rate_cut_flag", when(col("us_interest_rate_1m") < col("us_prev_month") - threshold, 1).otherwise(0)) 
        .withColumn("uk_rate_hold_flag", when(abs(col("uk_interest_rate_3m") - col("uk_prev_month")) <= threshold, 1).otherwise(0)) 
        .withColumn("us_rate_hold_flag", when(abs(col("us_interest_rate_1m") - col("us_prev_month")) <= threshold, 1).otherwise(0)) 
        .drop("uk_prev_month") 
        .drop("us_prev_month")
        )

    try:
        gold_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{GOLD_INTEREST_TABLE}")
        print(f"Created silver table {SCHEMA_NAME}.{GOLD_INTEREST_TABLE}")
    except AnalysisException as ae:
        print(f"Analysis error when saving gold table {SCHEMA_NAME}.{GOLD_INTEREST_TABLE}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving gold table {SCHEMA_NAME}.{GOLD_INTEREST_TABLE}: {e}")


def create_gold_unemployment(df, window_12, window_dt):
    """
    """
    
    gold_df = (
        df 
        .withColumn("uk_unemployment_12m_avg", avg("uk_unemployment_rate_1m").over(window_12)) 
        .withColumn("us_unemployment_12m_avg", avg("us_unemployment_rate_3m").over(window_12)) 
        .withColumn("uk_unemployment_1y_ago", lag("uk_unemployment_rate_1m", 365).over(window_dt))
        .withColumn("us_unemployment_1y_ago", lag("us_unemployment_rate_3m", 365).over(window_dt))
        .withColumn("uk_weak_labour_market_flag",
        when(col("uk_unemployment_1y_ago").isNotNull() & (col("uk_unemployment_rate_1m") > col("uk_unemployment_1y_ago") + 0.3), 1)
            .otherwise(0))
        .withColumn("us_weak_labour_market_flag",
        when(col("us_unemployment_1y_ago").isNotNull() & (col("us_unemployment_rate_3m") > col("us_unemployment_1y_ago") + 0.3), 1)
            .otherwise(0))
        .drop("uk_unemployment_1y_ago", "us_unemployment_1y_ago")
        )

    try:
        gold_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{GOLD_UNEM_TABLE}")
        print(f"Created silver table {SCHEMA_NAME}.{GOLD_UNEM_TABLE}")
    except AnalysisException as ae:
        print(f"Analysis error when saving gold table {SCHEMA_NAME}.{GOLD_UNEM_TABLE}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving gold table {SCHEMA_NAME}.{GOLD_UNEM_TABLE}: {e}")


def create_gold_cpi(df, window):
    """
    """

    gold_df = (
        df 
        .withColumn("us_inflation_12m_avg", avg("us_inflation_rate_1m").over(window)) 
        .withColumn("uk_inflation_12m_avg", avg("uk_inflation_rate_1m").over(window)) 
        .withColumn("us_high_inflation_regime", when(col("us_inflation_12m_avg") > 3, 1).otherwise(0)) 
        .withColumn("uk_high_inflation_regime", when(col("uk_inflation_12m_avg") > 3, 1).otherwise(0))
        )

    try:
        gold_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{GOLD_CPI_TABLE}")
        print(f"Created silver table {SCHEMA_NAME}.{GOLD_CPI_TABLE}")
    except AnalysisException as ae:
        print(f"Analysis error when saving gold table {SCHEMA_NAME}.{GOLD_CPI_TABLE}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving gold table {SCHEMA_NAME}.{GOLD_CPI_TABLE}: {e}")


def create_gold_gdp(df):
    """
    """
    gold_df = (
        df
        .withColumn("uk_weak_growth_regime", when(col("uk_gdp_rate_yoy") < 0, 1).otherwise(0))
        .withColumn("us_weak_growth_regime", when(col("us_gdp_rate_yoy") < 0, 1).otherwise(0))
        )

    try:
        gold_df.write.mode("overwrite").saveAsTable(f"{SCHEMA_NAME}.{GOLD_GDP_TABLE}")
        print(f"Created silver table {SCHEMA_NAME}.{GOLD_GDP_TABLE}")
    except AnalysisException as ae:
        print(f"Analysis error when saving gold table {SCHEMA_NAME}.{GOLD_GDP_TABLE}: {ae}")
    except Exception as e:
        print(f"Unexpected error saving gold table {SCHEMA_NAME}.{GOLD_GDP_TABLE}: {e}")
