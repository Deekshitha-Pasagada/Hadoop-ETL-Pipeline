import pytest
import pandas as pd
import numpy as np
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from transformer import DataTransformer
from loader import HadoopLoader
import tempfile


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'Customer ID': [1, 2, 2, 3, 4],
        'Age': [25.0, 30.0, 30.0, np.nan, 200.0],
        'Name': ['Alice', 'Bob', 'Bob', None, 'Dave'],
        'Amount': [100.0, 200.0, 200.0, 150.0, 99999.0],
        'Country': ['US', 'UK', 'UK', 'US', 'UK']
    })


@pytest.fixture
def transformer():
    return DataTransformer({'version': '1.0.0'})


def test_normalize_columns(transformer, sample_df):
    df = transformer.normalize_column_names(sample_df)
    assert 'customer_id' in df.columns
    assert 'Customer ID' not in df.columns


def test_remove_duplicates(transformer, sample_df):
    df = transformer.normalize_column_names(sample_df)
    df = transformer.clean(df)
    assert len(df) == 4


def test_fill_nulls(transformer, sample_df):
    df = transformer.normalize_column_names(sample_df)
    df = transformer.clean(df)
    assert df['age'].isna().sum() == 0
    assert df['name'].isna().sum() == 0


def test_remove_outliers(transformer, sample_df):
    df = transformer.normalize_column_names(sample_df)
    df = transformer.clean(df)
    df = transformer.remove_outliers(df, ['amount'], z_threshold=2.0)
    assert 99999.0 not in df['amount'].values


def test_add_metadata(transformer, sample_df):
    df = transformer.normalize_column_names(sample_df)
    df = transformer.add_metadata(df, 'customers')
    assert '_etl_source' in df.columns
    assert '_etl_timestamp' in df.columns
    assert df['_etl_source'].iloc[0] == 'customers'


def test_load_parquet(sample_df):
    with tempfile.TemporaryDirectory() as tmpdir:
        loader = HadoopLoader(output_path=tmpdir)
        path = loader.load_parquet(sample_df, 'test_table')
        assert os.path.exists(path)
        loaded = pd.read_parquet(path)
        assert len(loaded) == len(sample_df)


def test_partitioned_load(sample_df):
    with tempfile.TemporaryDirectory() as tmpdir:
        loader = HadoopLoader(output_path=tmpdir, partition_col='Country')
        path = loader.load_parquet(sample_df, 'test_partitioned')
        assert os.path.exists(path)


def test_validate_load(sample_df):
    with tempfile.TemporaryDirectory() as tmpdir:
        loader = HadoopLoader(output_path=tmpdir)
        path = loader.load_parquet(sample_df, 'test_validate')
        assert loader.validate_load(sample_df, path) is True