"""
Generic Gold layer write engine.

Three distinct write patterns, because the three kinds of table need
genuinely different handling -- collapsing them into one "write_table()"
would hide real differences, not simplify anything:

  write_identity_dimension()  Type 1 dim with a generated surrogate key
                               (dim_product, dim_seller). Table is dropped
                               and recreated each run; identity sequence
                               resets, which is fine since facts referencing
                               it are rebuilt in the same run.

  write_date_dimension()      dim_date. Plain overwrite -- it already has
                               a natural stable key (date_sk), no identity
                               column, no history to preserve.

  write_fact_table()          fact_order_items/payments/reviews. Plain
                               overwrite, validated against gold_schema.py
                               before writing.

dim_customer isn't written through this module at all -- it's a MERGE,
not a table write, and lives in gold_dims.scd2_merge_dim_customer().
"""

from common.config import DEFAULT_CATALOG
from common.gold_schema import GOLD_FACT_SCHEMAS


def write_identity_dimension(spark, df, table_name, ddl_template, catalog=DEFAULT_CATALOG):
    """Recreate a Type 1 dimension with a Delta-generated surrogate key.

    ddl_template is the CREATE OR REPLACE TABLE statement for this specific
    dimension (defined in gold_dims.py, next to its build function) with a
    `{table}` placeholder for the fully-qualified table name -- this
    function doesn't infer or generate DDL itself, it just executes it and
    loads the DataFrame into the table it declares. Uses INSERT INTO with
    an explicit column list (excluding the identity column) rather than
    DataFrameWriter, since saveAsTable can't target a pre-declared identity
    column.
    """
    table_full_name = f"{catalog}.gold.{table_name}"
    spark.sql(ddl_template.format(table=table_full_name))

    staging_view = f"__staging_{table_name}"
    df.createOrReplaceTempView(staging_view)
    col_list = ", ".join(df.columns)
    spark.sql(f"INSERT INTO {table_full_name} ({col_list}) SELECT {col_list} FROM {staging_view}")

    row_count = df.count()
    print(f"{table_name}: {row_count} rows written to {table_full_name} (surrogate keys regenerated)")
    return row_count


def write_date_dimension(df, table_name, catalog=DEFAULT_CATALOG):
    table_full_name = f"{catalog}.gold.{table_name}"
    (df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true')
       .saveAsTable(table_full_name))
    row_count = df.count()
    print(f"{table_name}: {row_count} rows written to {table_full_name}")
    return row_count


def validate_fact_schema(df, table_name):
    expected_schema = GOLD_FACT_SCHEMAS.get(table_name)
    if expected_schema is None:
        return

    expected = {f.name: f.dataType for f in expected_schema.fields}
    actual = {f.name: f.dataType for f in df.schema.fields}

    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    if missing or extra:
        raise ValueError(
            f"[{table_name}] output columns don't match gold_schema.py contract. "
            f"Missing: {sorted(missing) or 'none'}, unexpected: {sorted(extra) or 'none'}"
        )

    for col_name, expected_type in expected.items():
        actual_type = actual[col_name]
        if actual_type != expected_type:
            print(
                f"[{table_name}] WARNING: column '{col_name}' is {actual_type}, "
                f"expected {expected_type} per gold_schema.py"
            )


def write_fact_table(df, table_name, catalog=DEFAULT_CATALOG):
    validate_fact_schema(df, table_name)
    table_full_name = f"{catalog}.gold.{table_name}"
    (df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true')
       .saveAsTable(table_full_name))
    row_count = df.count()
    print(f"{table_name}: {row_count} rows written to {table_full_name}")
    return row_count