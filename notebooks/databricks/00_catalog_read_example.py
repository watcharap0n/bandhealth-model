# Databricks notebook source
# MAGIC %md
# MAGIC # Catalog Read Example (App-ID Filtered)
# MAGIC
# MAGIC This notebook shows the direct `spark.table(...)` read pattern by `app_id` for c-vit and see-chan.

# COMMAND ----------

from pyspark.sql import functions as F

# Define app_ids
app_id_cvit = "1993744540760190"
app_id_seechan = "838315041537793"

# Activity Transaction
activity_transaction_cvit = spark.table("projects_prd.datacleansing.activity_transaction").filter(F.col("app_id").cast("string") == app_id_cvit)
activity_transaction_seechan = spark.table("projects_prd.datacleansing.activity_transaction").filter(F.col("app_id").cast("string") == app_id_seechan)

# Purchase Transaction
purchase_transaction_cvit = spark.table("projects_prd.datacleansing.purchase_transaction").filter(F.col("app_id").cast("string") == app_id_cvit)
purchase_transaction_seechan = spark.table("projects_prd.datacleansing.purchase_transaction").filter(F.col("app_id").cast("string") == app_id_seechan)

# Purchase Transaction Items
purchase_transactionitems_cvit = spark.table("projects_prd.datacleansing.purchase_transactionitems").filter(F.col("app_id").cast("string") == app_id_cvit)
purchase_transactionitems_seechan = spark.table("projects_prd.datacleansing.purchase_transactionitems").filter(F.col("app_id").cast("string") == app_id_seechan)

# User Device
user_device_cvit = spark.table("projects_prd.datacleansing.user_device").filter(F.col("app_id").cast("string") == app_id_cvit)
user_device_seechan = spark.table("projects_prd.datacleansing.user_device").filter(F.col("app_id").cast("string") == app_id_seechan)

# User Identity
user_identity_cvit = spark.table("projects_prd.datacleansing.user_identity").filter(F.col("app_id").cast("string") == app_id_cvit)
user_identity_seechan = spark.table("projects_prd.datacleansing.user_identity").filter(F.col("app_id").cast("string") == app_id_seechan)

# User View
user_view_cvit = spark.table("projects_prd.datacleansing.user_view").filter(F.col("app_id").cast("string") == app_id_cvit)
user_view_seechan = spark.table("projects_prd.datacleansing.user_view").filter(F.col("app_id").cast("string") == app_id_seechan)

# User Visitor
user_visitor_cvit = spark.table("projects_prd.datacleansing.user_visitor").filter(F.col("app_id").cast("string") == app_id_cvit)
user_visitor_seechan = spark.table("projects_prd.datacleansing.user_visitor").filter(F.col("app_id").cast("string") == app_id_seechan)

# User Info
userinfo_cvit = spark.table("projects_prd.datacleansing.userinfo").filter(F.col("app_id").cast("string") == app_id_cvit)
userinfo_seechan = spark.table("projects_prd.datacleansing.userinfo").filter(F.col("app_id").cast("string") == app_id_seechan)

print("✓ All datasets loaded and filtered by app_id")
print(f"  - c-vit app_id: {app_id_cvit}")
print(f"  - see-chan app_id: {app_id_seechan}")

# Optional quick sanity counts
for name, sdf in [
    ("activity_transaction_cvit", activity_transaction_cvit),
    ("activity_transaction_seechan", activity_transaction_seechan),
    ("purchase_transaction_cvit", purchase_transaction_cvit),
    ("purchase_transaction_seechan", purchase_transaction_seechan),
]:
    print(name, sdf.count())
