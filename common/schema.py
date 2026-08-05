from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType

customer_schema = StructType([
    StructField('customer_id',StringType(),False),
    StructField('customer_unique_id',StringType(),True),
    StructField('customer_zip_code_prefix',IntegerType(),True),
    StructField('customer_city',StringType(),True),
    StructField('customer_state',StringType(),True)
])

geolocation_schema = StructType([
    StructField('geolocation_zip_code_prefix',IntegerType(),False),
    StructField('geolocation_lat',DoubleType(),True),
    StructField('geolocation_lng',DoubleType(),True),
    StructField('geolacation_city',StringType(),True),
    StructField('geolocation_state',StringType(),True)
])

order_items_schema = StructType([
    StructField('order_id',StringType(),False),
    StructField('order_item_id',IntegerType(),False),
    StructField('product_id',StringType(),True),
    StructField('seller_id',StringType(),True),
    StructField('shipping_limit_date',TimestampType(),True),
    StructField('price',FloatType(),True),
    StructField('freight_value',FloatType(),True)
])

order_payments_schema = StructType([
    StructField('order_id',StringType(),False),
    StructField('payment_sequential',IntegerType(),True),
    StructField('payment_type',StringType(),True),
    StructField('payment_installments',IntegerType(),True),
    StructField('payment_value',FloatType(),True)
])

order_reviews_schema = StructType([
    StructField('review_id',StringType(),False),
    StructField('order_id',StringType(),True),
    StructField('review_score',IntegerType(),True),
    StructField('review_comment_title',StringType(),True),
    StructField('review_comment_message',StringType(),True),
    StructField('review_creation_date',TimestampType(),True),
    StructField('review_answer_timestamp',TimestampType(),True)
])

orders_schema = StructType([
    StructField('order_id',StringType(),False),
    StructField('customer_id',StringType(),True),
    StructField('order_status',StringType(),True),
    StructField('order_purchase_timestamp',TimestampType(),True),
    StructField('order_approved_at',TimestampType(),True),
    StructField('order_delivered_carrier_date',TimestampType(),True),
    StructField('order_delivered_customer_date',TimestampType(),True),
    StructField('order_estimated_delivery_date',TimestampType(),True)
])

products_schema = StructType([
    StructField('product_id',StringType(),False),
    StructField('product_category_name',StringType(),True),
    StructField('product_name_lenght',IntegerType(),True),
    StructField('product_description_length',IntegerType(),True),
    StructField('product_photos_qty',IntegerType(),True),
    StructField('product_weight_q',IntegerType(),True),
    StructField('product_length_cm',IntegerType(),True),
    StructField('product_height_cm',IntegerType(),True),
    StructField('product_width_cm',IntegerType(),True),
])

sellers_schema = StructType([
    StructField('seller_id',StringType(),False),
    StructField('seller_zip_code_prefix',IntegerType(),True),
    StructField('seller_city',StringType(),True),
    StructField('seller_state',StringType(),True)
])

product_category_name_translation_schema = StructType([
    StructField('product_category_name',StringType(),True),
    StructField('product_category_name_english',StringType(),True)
])
