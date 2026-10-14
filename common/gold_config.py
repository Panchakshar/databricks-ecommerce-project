"""
Gold layer registry.

Build order matters and is NOT left to dict iteration order: dimensions
must exist before facts (facts resolve surrogate keys by joining out to
them), and dim_customer's SCD2 merge must run before fact_order_items
(which does an as-of join against it). See Gold Notebook.ipynb, which
runs GOLD_TYPE1_DIMENSIONS -> GOLD_SCD2_DIMENSIONS -> GOLD_FACTS in that
explicit order rather than looping one merged dict.
"""

from common.gold_dims import (
    build_dim_date, build_dim_product, build_dim_seller,
    DIM_PRODUCT_DDL, DIM_SELLER_DDL,
)
from common.gold_facts import (
    build_fact_order_items,
    build_fact_order_payments,
    build_fact_order_reviews,
)

# dim_date already has a natural, stable integer key (date_sk = YYYYMMDD),
# so it's just a plain overwrite -- no identity column needed. Handled as
# its own case in the Gold notebook rather than lumped in below.
GOLD_DATE_DIMENSION = {'build_fn': build_dim_date}

# Type 1 dimensions that need a generated surrogate key (no natural
# integer key of their own). `ddl` is that dimension's CREATE OR REPLACE
# TABLE statement, defined in gold_dims.py next to its build function --
# same pattern dim_customer's DDL follows, just Type 1 instead of Type 2.
GOLD_TYPE1_DIMENSIONS = {
    'dim_product': {'build_fn': build_dim_product, 'ddl': DIM_PRODUCT_DDL},
    'dim_seller': {'build_fn': build_dim_seller, 'ddl': DIM_SELLER_DDL},
}

# dim_customer is handled separately (common/gold_dims.scd2_merge_dim_customer)
# since it's an incremental MERGE against its own history, not a rebuild.

GOLD_FACTS = {
    'fact_order_items': {'build_fn': build_fact_order_items},
    'fact_order_payments': {'build_fn': build_fact_order_payments},
    'fact_order_reviews': {'build_fn': build_fact_order_reviews},
}