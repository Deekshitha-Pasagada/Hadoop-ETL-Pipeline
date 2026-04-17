import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
import re

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transforms raw RDBMS data for loading into
    Hadoop/Hive columnar storage (Parquet/ORC).
    Applies data quality checks, type casting,
    and schema normalization.
    """

    def __init__(self, config: Dict):
        self.config = config

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply data cleansing — handle nulls, duplicates, outliers."""
        logger.info(f"Cleaning dataframe: {df.shape}")

        # Drop full duplicates
        before = len(df)
        df = df.drop_duplicates()
        logger.info(f"Removed {before - len(df)} duplicate rows")

        # Fill numeric nulls with median
        num_cols = df.select_dtypes(include=[np.number]).columns
        for col in num_cols:
            median = df[col].median()
            nulls = df[col].isna().sum()
            if nulls > 0:
                df[col] = df[col].fillna(median)
                logger.info(
                    f"Filled {nulls} nulls in '{col}' with median {median:.4f}")

        # Fill string nulls with empty string
        str_cols = df.select_dtypes(include=['object']).columns
        for col in str_cols:
            df[col] = df[col].fillna('').str.strip()

        return df

    def normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names for Hive compatibility."""
        df.columns = [
            re.sub(r'[^a-z0-9_]', '_', col.lower().strip())
            for col in df.columns
        ]
        return df

    def cast_types(self, df: pd.DataFrame,
                   schema: Dict[str, str]) -> pd.DataFrame:
        """Cast columns to specified types for Parquet schema."""
        for col, dtype in schema.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                    logger.info(f"Cast '{col}' to {dtype}")
                except Exception as e:
                    logger.warning(f"Failed to cast '{col}': {e}")
        return df

    def remove_outliers(self, df: pd.DataFrame,
                        columns: List[str],
                        z_threshold: float = 3.0) -> pd.DataFrame:
        """Remove statistical outliers using Z-score method."""
        for col in columns:
            if col in df.columns:
                mean = df[col].mean()
                std = df[col].std()
                if std > 0:
                    z_scores = (df[col] - mean) / std
                    before = len(df)
                    df = df[z_scores.abs() <= z_threshold]
                    removed = before - len(df)
                    if removed > 0:
                        logger.info(
                            f"Removed {removed} outliers from '{col}'")
        return df

    def add_metadata(self, df: pd.DataFrame,
                     source_table: str) -> pd.DataFrame:
        """Add ETL metadata columns for lineage tracking."""
        df['_etl_source'] = source_table
        df['_etl_timestamp'] = pd.Timestamp.utcnow()
        df['_etl_version'] = self.config.get('version', '1.0.0')
        return df

    def transform(self, df: pd.DataFrame,
                  source_table: str,
                  schema: Optional[Dict] = None,
                  outlier_cols: Optional[List] = None) -> pd.DataFrame:
        """Full transformation pipeline."""
        df = self.normalize_column_names(df)
        df = self.clean(df)
        if schema:
            df = self.cast_types(df, schema)
        if outlier_cols:
            df = self.remove_outliers(df, outlier_cols)
        df = self.add_metadata(df, source_table)
        logger.info(
            f"Transformation complete: {df.shape[0]} rows, "
            f"{df.shape[1]} columns")
        return df