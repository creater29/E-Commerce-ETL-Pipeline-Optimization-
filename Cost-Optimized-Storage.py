# Columnar storage with Snappy compression
pq.write_to_dataset(
    ...,
    compression='snappy'  # 70-80% size reduction
)

# Partitioned storage
partition_cols=['year', 'month']  # 20% cost savings
