import logging
import os
from typing import Optional
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


class HadoopLoader:
    """
    Loads transformed data into the Hadoop ecosystem.
    Supports local Parquet (HDFS-compatible), Hive-ready
    partitioned output, and S3/cloud storage targets.
    """

    def __init__(self, output_path: str, partition_col: Optional[str] = None):
        self.output_path = Path(output_path)
        self.partition_col = partition_col
        self.output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Loader initialized: output={output_path}")

    def load_parquet(self, df: pd.DataFrame,
                     table_name: str,
                     compression: str = 'snappy') -> str:
        """Write dataframe to Parquet format (columnar, HDFS-compatible)."""
        if self.partition_col and self.partition_col in df.columns:
            return self._load_partitioned(df, table_name, compression)

        output_file = self.output_path / f"{table_name}.parquet"
        df.to_parquet(output_file, compression=compression, index=False)
        size_mb = output_file.stat().st_size / (1024 * 1024)
        logger.info(
            f"Loaded {len(df)} rows to {output_file} "
            f"({size_mb:.2f} MB, {compression} compression)")
        return str(output_file)

    def _load_partitioned(self, df: pd.DataFrame,
                          table_name: str,
                          compression: str) -> str:
        """Write partitioned Parquet for Hive partition pruning."""
        table_path = self.output_path / table_name
        table_path.mkdir(parents=True, exist_ok=True)
        partitions = df[self.partition_col].unique()
        total_rows = 0
        for partition_val in partitions:
            partition_df = df[df[self.partition_col] == partition_val]
            partition_path = table_path / f"{self.partition_col}={partition_val}"
            partition_path.mkdir(parents=True, exist_ok=True)
            output_file = partition_path / "data.parquet"
            partition_df.to_parquet(
                output_file, compression=compression, index=False)
            total_rows += len(partition_df)
        logger.info(
            f"Partitioned load complete: {total_rows} rows, "
            f"{len(partitions)} partitions by '{self.partition_col}'")
        return str(table_path)

    def load_csv(self, df: pd.DataFrame, table_name: str) -> str:
        """Write dataframe to CSV for Hive external table compatibility."""
        output_file = self.output_path / f"{table_name}.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Loaded {len(df)} rows to {output_file}")
        return str(output_file)

    def validate_load(self, source_df: pd.DataFrame,
                      output_path: str) -> bool:
        """Validate loaded Parquet matches source row count."""
        try:
            loaded = pd.read_parquet(output_path)
            match = len(loaded) == len(source_df)
            logger.info(
                f"Validation: source={len(source_df)}, "
                f"loaded={len(loaded)}, match={match}")
            return match
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False