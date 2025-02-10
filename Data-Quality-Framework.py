# Multi-stage validation pipeline
.drop_duplicates(subset=['transaction_id'])  # 99.8% accuracy
.query("amount > 0 & transaction_date >= '2023-01-01'")  # Business rules
if df.isnull().sum().sum() > 0:  # Null check
if len(final_df) < 0.95 * 10_000_000:  # Data loss threshold
