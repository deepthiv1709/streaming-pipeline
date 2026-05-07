from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def check_nulls(df: DataFrame, columns: list) -> DataFrame:
    """Drop rows with nulls in critical fields and log count."""
    original = df.count()
    clean    = df.dropna(subset=columns)
    dropped  = original - clean.count()
    if dropped > 0:
        print(f"[QUALITY] Dropped {dropped} rows with nulls in {columns}")
    return clean

def check_amount_range(df: DataFrame, min_val=0.0, max_val=50000.0) -> DataFrame:
    """Filter out transactions with implausible amounts."""
    valid = df.filter((col("amount") >= min_val) & (col("amount") <= max_val))
    removed = df.count() - valid.count()
    if removed > 0:
        print(f"[QUALITY] Removed {removed} rows outside amount range")
    return valid

def check_valid_status(df: DataFrame) -> DataFrame:
    """Keep only known status values."""
    valid_statuses = ["completed", "pending", "failed"]
    return df.filter(col("status").isin(valid_statuses))

def run_all_checks(df: DataFrame) -> DataFrame:
    df = check_nulls(df, ["transaction_id", "user_id", "amount", "timestamp"])
    df = check_amount_range(df)
    df = check_valid_status(df)
    return df