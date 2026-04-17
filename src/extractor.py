import logging
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RDBMSExtractor:
    """
    Extracts data from relational database systems (RDBMS)
    for ingestion into the Hadoop ecosystem via HDFS/Hive.
    """

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        logger.info("RDBMS extractor initialized")

    def extract_table(self, table_name: str,
                      batch_size: int = 10000) -> pd.DataFrame:
        """Extract full table from RDBMS."""
        logger.info(f"Extracting table: {table_name}")
        query = f"SELECT * FROM {table_name}"
        chunks = []
        with self.engine.connect() as conn:
            for chunk in pd.read_sql(
                text(query), conn, chunksize=batch_size
            ):
                chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True)
        logger.info(f"Extracted {len(df)} rows from {table_name}")
        return df

    def extract_query(self, query: str) -> pd.DataFrame:
        """Extract data using a custom SQL query."""
        logger.info(f"Executing custom query: {query[:80]}...")
        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        logger.info(f"Query returned {len(df)} rows")
        return df

    def extract_incremental(self, table_name: str,
                             timestamp_col: str,
                             last_run: str) -> pd.DataFrame:
        """Extract only new/updated records since last run."""
        query = f"""
            SELECT * FROM {table_name}
            WHERE {timestamp_col} > '{last_run}'
        """
        logger.info(
            f"Incremental extract from {table_name} since {last_run}")
        return self.extract_query(query)