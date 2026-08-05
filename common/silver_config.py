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
        'pre_transform' : lambda df, spark: df.withColumn('customer_city',F.trim(F.lower(F.col('customer_city')))),
        'rules' : [
            (F.col('customer_id').isNull(),'missing_customer_id'),
            (F.col('customer_unique_id').isNull(),'missing_customer_unique_id'),
            (F.col('customer_zip_code_prefix').isNull(),'missing_customer_zip_code_prefix')
        ]
    },

    'geolocation' : {
        'dedup_keys' : ['geolocation_zip_code_prefix'],
        'pre_transform' : [
            agg_geolocations,
            lambda df,spark : df.withColumn('geolacation_city',F.trim(F.lower(F.col('geolocation_city'))))
        ],
        'rules' : [
            (F.col('geolocation_zip_code_prefix').isNull(),'missing_zip_code')
        ]
    },

    'order_items' : {
        'dedup_keys' : ['order_id','order_item_id'],
        'rules' : [
            (F.col('order_id').isNull(),'missing_order_id'),
            (F.col('order_item_id').isNull(),'missing_order_item_id'),
            (F.col('price') <= 0, 'non_positivea_price'),
            (F.col('product_id').isNull(),'missing_product_id'),
            (F.col('seller_id').isNull(),'missing_seller_id')
        ]
    },

    'order_payments' : {
        'dedup_keys' : ['order_id','payment_sequntial'],
        'pre_transform' : lambda df, spark: df.withColumn('payment_type',F.trim(F.lower(F.col('payment_type')))),
        'rules' : [
            (F.col('order_id').isNull(),'missing_order_id'),
            (F.col('payment_value') <= 0, 'non_positive_payment_value')
        ]
    },

    'order_reviews' : {
        'dedup_keys' : ['review_id'],
        'pre_transform' : lambda df,spark : df.withColumn('review_comment_message',F.trim(F.lower(F.col('review_comment_message')))).withColumn('review_comment_title',F.trim(F.lower(F.col('review_comment_title')))),
        'rules' : [
            (F.col('review_id').isNull(),'missing_review_id'),
            (F.col('order_id').isNull(),'missing_oder_id'),
            (~F.col('review_score').between(1,5),'invalid_review_score')
        ]
    },

    'orders' 
}




