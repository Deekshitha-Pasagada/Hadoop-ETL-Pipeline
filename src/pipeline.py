import logging
import yaml
import time
from pathlib import Path
from extractor import RDBMSExtractor
from transformer import DataTransformer
from loader import HadoopLoader

logger = logging.getLogger(__name__)


class HadoopETLPipeline:
    """
    End-to-end ETL pipeline integrating RDBMS with the Hadoop
    ecosystem. Extracts from MySQL/PostgreSQL, transforms with
    data quality checks, and loads to HDFS-compatible Parquet
    storage with performance benchmarking.
    """

    def __init__(self, config_path: str = 'config/pipeline_config.yaml'):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        logger.info(f"Pipeline initialized: {self.config['pipeline']['name']}")

    def run(self, table_name: str) -> dict:
        """Execute full ETL pipeline for a given table."""
        start_time = time.time()
        logger.info(f"Starting ETL pipeline for table: {table_name}")

        # --- EXTRACT ---
        extractor = RDBMSExtractor(
            self.config['source']['connection_string'])
        df = extractor.extract_table(
            table_name,
            batch_size=self.config['pipeline'].get('batch_size', 10000)
        )

        extract_time = time.time() - start_time
        logger.info(f"Extract complete in {extract_time:.2f}s")

        # --- TRANSFORM ---
        transformer = DataTransformer(self.config['pipeline'])
        table_config = self.config.get('tables', {}).get(table_name, {})
        df = transformer.transform(
            df,
            source_table=table_name,
            schema=table_config.get('schema'),
            outlier_cols=table_config.get('outlier_columns')
        )

        transform_time = time.time() - start_time - extract_time
        logger.info(f"Transform complete in {transform_time:.2f}s")

        # --- LOAD ---
        loader = HadoopLoader(
            output_path=self.config['target']['output_path'],
            partition_col=table_config.get('partition_col')
        )
        output_path = loader.load_parquet(
            df, table_name,
            compression=self.config['target'].get('compression', 'snappy')
        )

        load_time = time.time() - start_time - extract_time - transform_time
        total_time = time.time() - start_time

        # --- BENCHMARK REPORT ---
        report = {
            'table': table_name,
            'rows_extracted': len(df),
            'columns': len(df.columns),
            'output_path': output_path,
            'extract_time_s': round(extract_time, 2),
            'transform_time_s': round(transform_time, 2),
            'load_time_s': round(load_time, 2),
            'total_time_s': round(total_time, 2),
            'throughput_rows_per_sec': round(
                len(df) / total_time, 0) if total_time > 0 else 0
        }

        logger.info(f"Pipeline complete: {report}")
        return report


if __name__ == '__main__':
    pipeline = HadoopETLPipeline()
    tables = pipeline.config['pipeline'].get('tables', [])
    for table in tables:
        report = pipeline.run(table)
        print(f"\nETL Report for {table}:")
        for k, v in report.items():
            print(f"  {k}: {v}")