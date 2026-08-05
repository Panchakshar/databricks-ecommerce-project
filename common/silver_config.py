from pyspark.sql import functions as F

def translate_product_category(df, spark):
    #Translate Portuguese category names to English
    products_translations = spark.table('ecommerce_lakehouse.bronze.product_category_name_translation')

    return (df.alias('a').join(products_translations.alias('b'), on='product_category_name', how = 'left')
            .withColumn('product_category_name_final',F.coalesce(F.col('product_category_name_english'),F.col('product_category_name'),F.lit('Unknown')))
            .drop(F.col("product_category_name"),F.col("product_category_name_english"),F.col("b._file_name"),F.col("b._load_timestamp")))
    
def agg_geolocations(df, spark):
    # collapses heavy zip-level duplication instead of just deduping
    return (df.groupBy('geolocation_zip_code_prefix').agg(F.round(F.avg('geolocation_lat'),5).alias('geolocation_lat'),F.round(F.avg('geolocation_lng'),5).alias('geolocation_lng'),F.first('geolocation_city').alias('geolocation_city'),F.first('geolocation_state').alias('geolocation_state'),F.max('_load_timestamp').alias('_load_timestamp'),F.first('_file_name').alias('_file_name')))


SILVER_RULES = {
    'customers' : {
        'dedup_keys' : ['customer_id'],
        'pre_transform' : [
            lambda df, spark: df.withColumn('customer_city',F.trim(F.lower(F.col('customer_city'))))
        ],
        'rules' : [
            (F.col('customer_unique_id').isNull(),'missing_customer_unique_id'),
            (F.col('customer_zip_code_prefix').isNull(),'missing_customer_zip_code_prefix')
        ]
    },

    'geolocation' : {
        'dedup_keys' : ['geolocation_zip_code_prefix'],
        'pre_transform' : [
            agg_geolocations,
            lambda df,spark : df.withColumn('geolocation_city',F.trim(F.lower(F.col('geolocation_city'))))
        ],
        'rules' : []
    },

    'order_items' : {
        'dedup_keys' : ['order_id','order_item_id'],
        'rules' : [
            (F.col('price') <= 0, 'non_positivea_price'),
            (F.col('product_id').isNull(),'missing_product_id'),
            (F.col('seller_id').isNull(),'missing_seller_id')
        ]
    },

    'order_payments' : {
        'dedup_keys' : ['order_id','payment_sequential'],
        'pre_transform' : [
            lambda df, spark: df.withColumn('payment_type',F.trim(F.lower(F.col('payment_type'))))
        ],
        'rules' : [
            (F.col('payment_value') <= 0, 'non_positive_payment_value')
        ]
    },

    'order_reviews' : {
        'dedup_keys' : ['review_id'],
        'pre_transform' : [
            lambda df,spark : df.withColumn('review_comment_message',F.trim(F.lower(F.col('review_comment_message')))).withColumn('review_comment_title',F.trim(F.lower(F.col('review_comment_title'))))
        ],
        'rules' : [
            (F.col('order_id').isNull(),'missing_oder_id'),
            (~F.col('review_score').between(1,5),'invalid_review_score')
        ]
    },

    'orders' : {
        'dedup_keys' : ['order_id'],
        'pre_transform' : [
            lambda df,spark : df.withColumn('order_status',F.trim(F.lower(F.col('order_status'))))
        ],
        'rules' : [
            (F.col('order_purchase_timestamp').isNull(),'missing_order_purchase_timestamp'),
            (F.col('order_delivered_customer_date') < F.col('order_purchase_timestamp'), 'delivery_before_purchase')
        ]
    },

    'products' : {
        'dedup_keys' : ['product_id'],
        'pre_transform' : [
            lambda df,spark : df.withColumn('product_category_name',F.trim(F.lower(F.col('product_category_name')))),
            translate_product_category
        ],
        'rules': []
    },

    'sellers' : {
        'dedup_keys' : ['seller_id'],
        'pre_transform' : [
            lambda df,spark : df.withColumn('seller_city',F.trim(F.lower(F.col('seller_city'))))
        ],
        'rules' : [
            (F.col('seller_zip_code_prefix').isNull(),'missing_seller_zip_code_prefix')
        ]
    }
}