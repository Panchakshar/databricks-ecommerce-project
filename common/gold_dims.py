"""
Gold layer dimensions.

dim_product, dim_seller, dim_date are Type 1 (current-state only,
fully rebuilt each run) -- there's no requirement to track history on
product attributes or the calendar, and rebuilding them from scratch
each run keeps them trivially consistent with Silver.

dim_customer is Type 2 (history-tracked): a customer's city/state is
allowed to change over time, and fact_order_items needs to resolve a
customer "as of" the order date, not just to whoever they are today.
That means dim_customer can NOT be blindly overwritten each run -- it's
merged incrementally against its own previous state.

Surrogate keys use Delta's GENERATED ALWAYS AS IDENTITY columns rather
than a hash or monotonically_increasing_id(), which is the standard
approach for surrogate keys in Databricks: FK joins get an integer key
that's stable for the life of the row, and identity generation is
handled by Delta itself instead of hand-rolled key logic.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from common.config import DIM_DATE_START, DIM_DATE_END


# ---------------------------------------------------------------------------
# Type 1 dimensions
# ---------------------------------------------------------------------------

def build_dim_date(spark, catalog, start_date=DIM_DATE_START, end_date=DIM_DATE_END):
    """Standard calendar dimension. date_sk is YYYYMMDD as an int (no identity
    needed -- the date itself is already a natural, stable integer key)."""
    return (
        spark.sql(
            f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS date"
        )
        .withColumn('date_sk', F.date_format('date', 'yyyyMMdd').cast('int'))
        .withColumn('year', F.year('date'))
        .withColumn('quarter', F.quarter('date'))
        .withColumn('month', F.month('date'))
        .withColumn('month_name', F.date_format('date', 'MMMM'))
        .withColumn('day', F.dayofmonth('date'))
        .withColumn('day_of_week', F.dayofweek('date'))
        .withColumn('day_name', F.date_format('date', 'EEEE'))
        .withColumn('week_of_year', F.weekofyear('date'))
        .withColumn('is_weekend', F.dayofweek('date').isin(1, 7))
        .select(
            'date_sk', 'date', 'year', 'quarter', 'month', 'month_name',
            'day', 'day_of_week', 'day_name', 'week_of_year', 'is_weekend',
        )
    )


# DDL for every Type 1 identity-key dimension lives here, next to the build
# function that populates it -- the surrogate key column is always visible
# at the definition site instead of being synthesized later from a
# DataFrame's inferred schema. (dim_customer's DDL above follows the same
# rule; these two are no longer a special case relative to it.)

DIM_PRODUCT_DDL = """
CREATE OR REPLACE TABLE {table} (
    product_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    product_id STRING,
    product_category STRING,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    product_photos_qty INT
) USING DELTA
"""

DIM_SELLER_DDL = """
CREATE OR REPLACE TABLE {table} (
    seller_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    seller_id STRING,
    seller_city STRING,
    seller_state STRING,
    seller_zip_code_prefix INT
) USING DELTA
"""


def build_dim_product(spark, catalog):
    products = spark.table(f"{catalog}.silver.products")
    return products.select(
        'product_id',
        F.col('product_category_name_final').alias('product_category'),
        'product_weight_g', 'product_length_cm', 'product_height_cm',
        'product_width_cm', 'product_photos_qty',
    )


def build_dim_seller(spark, catalog):
    sellers = spark.table(f"{catalog}.silver.sellers")
    return sellers.select('seller_id', 'seller_city', 'seller_state', 'seller_zip_code_prefix')


# ---------------------------------------------------------------------------
# Type 2 dimension: dim_customer
# ---------------------------------------------------------------------------

DIM_CUSTOMER_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_unique_id STRING,
    customer_city STRING,
    customer_state STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
) USING DELTA
"""


def _latest_customer_state(spark, catalog):
    """One row per customer_unique_id: their most recently-seen city/state.

    silver.customers is at customer_id grain (Olist mints a fresh customer_id
    per order), so this collapses to the durable customer_unique_id grain by
    keeping the most recent row per _load_timestamp.
    """
    customers = spark.table(f"{catalog}.silver.customers")
    window_spec = Window.partitionBy('customer_unique_id').orderBy(F.col('_load_timestamp').desc())
    return (
        customers
        .withColumn('rn', F.row_number().over(window_spec))
        .filter(F.col('rn') == 1)
        .select('customer_unique_id', 'customer_city', 'customer_state')
    )


def scd2_merge_dim_customer(spark, catalog):
    """Apply new/changed customer records to dim_customer, preserving history.

    Split into two separate, independently-correct merges rather than one
    combined "union trick" merge:
      1. Brand-new customers (no existing row at all) -> plain insert-only
         merge on the natural key. Nothing to close, so no ambiguity.
      2. Existing customers whose city/state changed -> the standard SCD2
         "two rows per change" merge: one row closes the old version
         (is_current=false, valid_to=now), a second row (forced to miss
         the match via a NULL merge key) inserts the new version.
    Keeping these separate avoids a subtle double-insert bug that the
    naive single-merge version of this pattern has for brand-new records
    (both of the unioned rows come back NOT MATCHED and both would insert).
    """
    table_name = f"{catalog}.gold.dim_customer"
    spark.sql(DIM_CUSTOMER_DDL.format(table=table_name))

    staged = _latest_customer_state(spark, catalog).withColumn('valid_from', F.current_timestamp())
    dim = DeltaTable.forName(spark, table_name)
    current = dim.toDF().filter(F.col('is_current'))

    joined = staged.alias('s').join(
        current.alias('c'), on='customer_unique_id', how='left'
    )

    brand_new = (
        joined.filter(F.col('c.customer_unique_id').isNull())
        .select('s.customer_unique_id', 's.customer_city', 's.customer_state', 's.valid_from')
    )

    changed_existing = (
        joined.filter(
            F.col('c.customer_unique_id').isNotNull()
            & (
                (F.col('s.customer_city') != F.col('c.customer_city'))
                | (F.col('s.customer_state') != F.col('c.customer_state'))
            )
        )
        .select('s.customer_unique_id', 's.customer_city', 's.customer_state', 's.valid_from')
    )

    new_count, changed_count = brand_new.count(), changed_existing.count()

    if new_count > 0:
        (
            dim.alias('tgt')
            .merge(brand_new.alias('src'), 'tgt.customer_unique_id = src.customer_unique_id')
            .whenNotMatchedInsert(values={
                'customer_unique_id': F.col('src.customer_unique_id'),
                'customer_city': F.col('src.customer_city'),
                'customer_state': F.col('src.customer_state'),
                'valid_from': F.col('src.valid_from'),
                'valid_to': F.lit(None).cast('timestamp'),
                'is_current': F.lit(True),
            })
            .execute()
        )

    if changed_count > 0:
        staged_merge = changed_existing.withColumn(
            'merge_key', F.col('customer_unique_id')
        ).unionByName(
            changed_existing.withColumn('merge_key', F.lit(None).cast('string'))
        )

        (
            dim.alias('tgt')
            .merge(
                staged_merge.alias('src'),
                'tgt.customer_unique_id = src.merge_key AND tgt.is_current = true',
            )
            .whenMatchedUpdate(set={
                'is_current': F.lit(False),
                'valid_to': F.col('src.valid_from'),
            })
            .whenNotMatchedInsert(values={
                'customer_unique_id': F.col('src.customer_unique_id'),
                'customer_city': F.col('src.customer_city'),
                'customer_state': F.col('src.customer_state'),
                'valid_from': F.col('src.valid_from'),
                'valid_to': F.lit(None).cast('timestamp'),
                'is_current': F.lit(True),
            })
            .execute()
        )

    print(f"dim_customer: {new_count} new customers, {changed_count} changed (new version inserted)")
    return new_count, changed_count