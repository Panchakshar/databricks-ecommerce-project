from pyspark.sql.functions import StructType, StructField, StringType, IntegerType, DoubleType

customer_schema = StructType([
    StructField('customer_id',StringType(),False),
    StructField('customer_unique_id',StringType(),False),
    StructField('customer_zip_code_prefix',IntergerType(),True),
    StructField('customer_city',StringType(),True),
    StructField('customer_state',StringType(),True)
])