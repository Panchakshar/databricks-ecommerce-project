from pyspark.sql import functions as F
from pyspark.sql.window import Window

def split_valid_invalid(df):
    return df.filter(F.col("_is_valid")), df.filter(~F.col("_is_valid"))

def write_rejects(invalid_df, table_name, catalog="ecommerce_lakehouse"):
    (invalid_df
        .withColumn("_quarantined_at", F.current_timestamp())
        .write.format("delta").mode("append")
        .saveAsTable(f"{catalog}.silver.{table_name}_rejects"))

def clean_silver_table(spark,table_name,config, catalog="ecommerce_lakehouse"):
    df = spark.table(f"{catalog}.bronze.{table_name}")
    
    pre_transform = config.get('pre_transform',[])
    for transfrom in pre_transform:
        df = transform(df,spark)

    window_spec = Window.partitionBy(*config['dedup_keys']).orderBy(F.col('_load_timestamp').desc())
    df = (df.withColumn('rn',F.row_number().over(window_spec))
          .filter(F.col('rn')==1).drop('rn'))

    reject_expr = F.lit(None).cast('string')
    for condition,reason in reversed(config['rules']):
        reject_expr = F.when(condtion,F.lit(reason)).otherwise(reject_expr)

    df_checked = (df.withColumn('_reject_reason',reject_expr).withColumn('_is_valid',F.col('_reject_reason').isNull())
    valid_df,invalid_df = split_valid_invalid(df_checked))
    
    valid_df,invalid_df = split_valid_invalid(df_checked)

    (valid.drop("_reject_reason", "_is_valid").write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver.{table_name}"))

    valid_count, invalid_count = valid.count(), invalid.count()
    print(f"{table_name}: {valid_count} valid, {invalid_count} rejected")
    return valid_count, invalid_count