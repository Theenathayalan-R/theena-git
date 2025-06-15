from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import hashlib

# Initialize Spark with Iceberg support
spark = SparkSession.builder \
    .appName("SCD_Type2_Iceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "path/to/warehouse") \
    .getOrCreate()

def create_hash_column(df, columns_to_hash, hash_column_name="row_hash"):
    """
    Create a hash column from specified columns for change detection
    """
    # Concatenate specified columns and create hash
    concat_expr = concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in columns_to_hash])
    return df.withColumn(hash_column_name, sha2(concat_expr, 256))

def prepare_source_data(source_df, business_key_cols, attribute_cols, current_timestamp):
    """
    Prepare source data with hash and SCD columns
    """
    # Create hash from attribute columns (excluding business keys and metadata)
    source_with_hash = create_hash_column(source_df, attribute_cols, "source_hash")
    
    # Add SCD Type 2 columns
    return source_with_hash \
        .withColumn("effective_date", lit(current_timestamp)) \
        .withColumn("end_date", lit(None).cast(TimestampType())) \
        .withColumn("is_current", lit(True)) \
        .withColumn("version", lit(1))

def scd_type2_merge_iceberg(source_df, target_table_name, business_key_cols, attribute_cols):
    """
    Implement SCD Type 2 using MERGE with hash comparison
    """
    current_timestamp = current_timestamp()
    
    # Prepare source data
    source_prepared = prepare_source_data(source_df, business_key_cols, attribute_cols, current_timestamp)
    
    # Create a temporary view for the source data
    source_prepared.createOrReplaceTempView("source_data")
    
    # Step 1: Identify changed records using hash comparison
    # Get current active records from target
    current_target_sql = f"""
    SELECT {', '.join(business_key_cols)}, row_hash as target_hash
    FROM {target_table_name}
    WHERE is_current = true
    """
    
    # Find records that have changed (different hash) or are new
    changed_records_sql = f"""
    SELECT s.*
    FROM source_data s
    LEFT JOIN ({current_target_sql}) t
    ON {' AND '.join([f's.{col} = t.{col}' for col in business_key_cols])}
    WHERE t.target_hash IS NULL OR s.source_hash != t.target_hash
    """
    
    changed_records = spark.sql(changed_records_sql)
    changed_records.createOrReplaceTempView("changed_records")
    
    # Step 2: MERGE operation for SCD Type 2
    merge_sql = f"""
    MERGE INTO {target_table_name} AS target
    USING (
        -- Union of records to expire and new records to insert
        SELECT 
            {', '.join(business_key_cols)},
            {', '.join([f'NULL as {col}' for col in attribute_cols])},
            NULL as source_hash,
            NULL as effective_date,
            current_timestamp() as end_date,
            false as is_current,
            version,
            'EXPIRE' as merge_action
        FROM {target_table_name}
        WHERE is_current = true
        AND ({' OR '.join([f'{col} IN (SELECT {col} FROM changed_records)' for col in business_key_cols])})
        
        UNION ALL
        
        SELECT 
            {', '.join(business_key_cols)},
            {', '.join(attribute_cols)},
            source_hash,
            effective_date,
            end_date,
            is_current,
            COALESCE((
                SELECT MAX(version) + 1 
                FROM {target_table_name} t2 
                WHERE {' AND '.join([f't2.{col} = c.{col}' for col in business_key_cols])}
            ), 1) as version,
            'INSERT' as merge_action
        FROM changed_records c
    ) AS source
    ON {' AND '.join([f'target.{col} = source.{col}' for col in business_key_cols])}
    AND target.is_current = true
    AND source.merge_action = 'EXPIRE'
    
    WHEN MATCHED AND source.merge_action = 'EXPIRE' THEN
        UPDATE SET
            end_date = source.end_date,
            is_current = source.is_current
    
    WHEN NOT MATCHED AND source.merge_action = 'INSERT' THEN
        INSERT ({', '.join(business_key_cols + attribute_cols + ['source_hash', 'effective_date', 'end_date', 'is_current', 'version'])})
        VALUES ({', '.join([f'source.{col}' for col in business_key_cols + attribute_cols + ['source_hash', 'effective_date', 'end_date', 'is_current', 'version']])})
    """
    
    # Execute the merge
    spark.sql(merge_sql)

# Performance optimization tips
def optimization_tips():
    """
    Key optimization strategies:
    
    1. Partitioning:
       - Partition by is_current flag for efficient querying
       - Consider date-based partitioning for large datasets
    
    2. Clustering:
       - Cluster by business key columns
    
    3. Hash Function Selection:
       - SHA2-256 for security vs MD5 for performance
       - Consider xxHash for ultra-high performance scenarios
    
    4. Batch Processing:
       - Process changes in batches to avoid memory issues
       - Use repartition() before merge operations
    
    5. Predicate Pushdown:
       - Use partition pruning in WHERE clauses
       - Leverage Iceberg's metadata for efficient filtering
    """
    pass
