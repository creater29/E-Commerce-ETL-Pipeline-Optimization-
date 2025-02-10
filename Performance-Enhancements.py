# Batch database inserts
method='multi', chunksize=10000  # 40% faster inserts

# Connection pooling
create_engine(...)  # Reusable DB connections
