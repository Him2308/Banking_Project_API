import dlt
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col, to_timestamp

@dlt.table(
    name="bronze_transactions"
)
def bronze_transactions():
    return spark.read.format("delta") \
        .load("/Volumes/banking_api/default/users_banking_volume/transactions")

@dlt.table(
    name="bronze_users"
)
def bronze_users():
    return (spark.read.format("delta") \
        .load("/Volumes/banking_api/default/users_banking_volume/users")
        .withColumn("ingestion_time", current_timestamp())
    )

@dlt.table(name="silver_transactions")
@dlt.expect("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect("valid_account_id", "account_id IS NOT NULL")
@dlt.expect("valid_amount", "amount > 0")
def silver_transactions():

    txn = dlt.read_stream("bronze_transactions")
    users = dlt.read("bronze_users")

    txn_clean = txn \
        .withColumn("amount", col("amount").cast("double")) \
        .withColumn("timestamp", to_timestamp("timestamp"))

    return txn_clean.join(
        users,
        txn_clean.account_id == users.id,
        "inner"
    )



# Step 1: Create target table
dlt.create_streaming_table("silver_transactions_cdc")

# Step 2: Apply CDC
dlt.apply_changes(
    target="silver_transactions_cdc",   # 👈 target table name
    source="silver_transactions",
    keys=["transaction_id"],
    sequence_by=col("timestamp"),
    stored_as_scd_type=1
)

from pyspark.sql.functions import col

dlt.create_streaming_table("gold_users_scd2")

dlt.apply_changes(
    target="gold_users_scd2",
    source="bronze_users",
    keys=["id"],
    sequence_by=col("ingestion_time"),
    stored_as_scd_type=2
)

from pyspark.sql.functions import sum

@dlt.table(name="gold_customer_spending")
def gold_customer_spending():

    df = dlt.read("silver_transactions")

    return df.groupBy("account_id") \
             .agg(sum("amount").alias("total_spent"))

from pyspark.sql.functions import when

@dlt.table(name="gold_fraud_detection")
def gold_fraud_detection():

    df = dlt.read("silver_transactions")

    return df.withColumn(
        "is_fraud",
        when(col("amount") > 8000, 1).otherwise(0)
    )