
import boto3
import pandas as pd
import numpy as np
import multiprocessing as mp
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========== Configuration ==========
AWS_CONFIG = {
    'aws_access_key_id': 'AKIAXXXXXXXXXXXXXXXX',
    'aws_secret_access_key': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
    'region_name': 'us-east-1'
}

S3_BUCKETS = {
    'raw': 'ecom-raw-data',
    'archived': 'ecom-archived'
}

DATABASE_CONFIG = {
    'dialect': 'redshift+psycopg2',
    'user': 'admin',
    'password': 'SecurePass123!',
    'host': 'analytics-cluster.123456.us-east-1.redshift.amazonaws.com',
    'port': '5439',
    'dbname': 'ecom_warehouse'
}

# ========== Helper Functions ==========
def create_db_connection():
    """Create Redshift connection with connection pooling"""
    return create_engine(
        f"{DATABASE_CONFIG['dialect']}://"
        f"{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@"
        f"{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/"
        f"{DATABASE_CONFIG['dbname']}"
    )

# ========== Core ETL Functions ==========
def process_chunk(chunk_path):
    """
    Parallel processing of data chunks
    Args:
        chunk_path (str): S3 path to CSV chunk
    Returns:
        pd.DataFrame: Processed DataFrame
    """
    try:
        # Initialize S3 client per process
        s3 = boto3.client('s3', **AWS_CONFIG)
        
        # Download chunk
        bucket, key = chunk_path
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj['Body'])
        
        # Transformation pipeline
        df = (df
              .drop_duplicates(subset=['transaction_id'])
              .assign(
                  transaction_date=lambda x: pd.to_datetime(
                      x['transaction_date'], errors='coerce'),
                  amount=lambda x: pd.to_numeric(
                      x['amount'], errors='coerce')
              )
              .query("amount > 0 & transaction_date >= '2023-01-01'")
              .assign(
                  price_category=lambda x: np.where(
                      x['amount'] > 100, 'premium', 'standard')
              )
        )
        
        # Validate chunk quality
        if df.isnull().sum().sum() > 0:
            raise ValueError("Null values in processed chunk")
            
        return df
    
    except Exception as e:
        logger.error(f"Failed processing {chunk_path}: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on failure

def run_etl():
    """Main ETL execution flow with parallel processing"""
    # === Extraction Phase ===
    logger.info("Starting S3 data extraction")
    s3 = boto3.client('s3', **AWS_CONFIG)
    
    # List all raw files (10M+ records)
    raw_objects = s3.list_objects_v2(
        Bucket=S3_BUCKETS['raw'],
        Prefix='daily/'
    )['Contents']
    
    # Create chunk paths for parallel processing
    chunk_paths = [(S3_BUCKETS['raw'], obj['Key']) 
                  for obj in raw_objects]
    
    # === Transformation Phase ===
    logger.info(f"Processing {len(chunk_paths)} chunks with {mp.cpu_count()} workers")
    
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(process_chunk, chunk_paths)
    
    # Combine results while preserving memory
    final_df = pd.concat(results, ignore_index=True)
    
    # Final validation
    if len(final_df) < 0.95 * 10_000_000:  # 95% data quality threshold
        raise ValueError("Excessive data loss during processing")
    
    logger.info(f"Processed {len(final_df):,} records with {len(final_df)/10_000_000:.1%} retention")
    
    # === Loading Phase ===
    logger.info("Starting data loading")
    engine = create_db_connection()
    
    # Add partitioning columns
    final_df['year'] = final_df['transaction_date'].dt.year
    final_df['month'] = final_df['transaction_date'].dt.month
    
    # === Load to Redshift with Batch Insert ===
    final_df.to_sql(
        name='sales',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=10000,
        schema='analytics'
    )
    
    # === Archive to S3 with Compression ===
    logger.info("Archiving processed data")
    table = pa.Table.from_pandas(final_df)
    pq.write_to_dataset(
        table,
        root_path=f"s3://{S3_BUCKETS['archived']}/processed/",
        partition_cols=['year', 'month'],
        compression='snappy',
        filesystem=pa.fs.S3FileSystem(
            region=AWS_CONFIG['region_name'],
            access_key=AWS_CONFIG['aws_access_key_id'],
            secret_key=AWS_CONFIG['aws_secret_access_key']
        )
    )
    
    logger.info("ETL completed successfully")

# ========== Execution & Monitoring ==========
if __name__ == "__main__":
    start_time = datetime.now()
    
    try:
        run_etl()
        duration = datetime.now() - start_time
        logger.info(f"ETL completed in {duration.total_seconds()/60:.2f} minutes")
        
    except Exception as e:
        logger.error(f"ETL failed: {str(e)}")
        # Implement retry logic or alerting here
        raise
