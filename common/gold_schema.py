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


DIM_CUSTOMER_DDL = """
CREATE TABLE IF NOT EXIST {table}(
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_unique_id STRING,
    customer_city STRING,
    customer_state STRING,
    eff_start_date TIMESTAMP,
    eff_end_date TIMESTAMP,
    is_active BOOLEAN
) USING DELTA
    )
"""