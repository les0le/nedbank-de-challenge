"""
Data Quality Rules: 
Extract rules from the dq_rules.yaml for Process.
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,to_date, to_timestamp, concat, coalesce, when, row_number, concat
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window 
import yaml


# ********************Load config file containing the rules **************
def dq_rules_config():
    dq_rules_path = os.environ.get("PIPELINE_CONFIG", "/data/config/dq_rules.yaml")
    try:
        with open(dq_rules_path) as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

#*****************Run rule NULL_REQUIRED ******************
def dq_null_checks(df, rules, table):
    cols = rules.get("null_checks", {}).get(table, [])
    
    if "dq_flag" not in df.columns:
        df = df.withColumn("dq_flag", lit(""))
        
    null_condition = None
    for col_name in cols:
        condition = col(col_name).isNull()
        null_condition = condition if null_condition is None else (null_condition | condition)
    
    #print(type(df))
    df = df.withColumn("dq_flag", when(null_condition, lit("NULL_REQUIRED")).otherwise(lit(None))) 
 
    return df 

    

#***************** Run rule type_checks
def dq_type_checks(df, rules, table):
    cols_to_check = rules.get("type_checks", {}).get(table, [])
    
    if "dq_flag" not in df.columns:
        df = df.withColumn("dq_flag", lit(""))
        
    for column_name, expected_type in cols_to_check.items():
        if column_name in df.columns:
            df = df.withColumn(
                "dq_flag",
                when(
                    col(column_name).isNotNull() & col(column_name).cast(expected_type).isNull(), lit(f"TYPE_MISMATCH")
                ).otherwise(col("dq_flag"))
            )

    return df

#*****************Run rule domain_checks
def dq_domain_checks(df, rules, table):
    domain_checks = rules.get("domain_checks", {})#.get(table, [])
    
    if "dq_flag" not in df.columns:
        df = df.withColumn("dq_flag", lit(""))
    
    for column_name, rule in domain_checks.items():
        if column_name in df.columns:
            allowed_values = rule.get("allowed", [])
            dq_flag_value = rule.get("dq_flag", [])
            
            if dq_flag_value is None:
                flag_value = "TYPE_MISMATCH"
            else:
                flag_value = dq_flag_value
            
            df = df.withColumn(
                "dq_flag",
                when(
                    col(column_name).isNotNull() & (~col(column_name).isin(allowed_values)), lit(f"{flag_value}")
                ).otherwise(col("dq_flag"))
            )
    #print(domain_checks)
    return df
    
#*****************Run rule currency_normalisation
def dq_currency_normalisation(df, rules, table):
    currency_checks = rules.get("currency_normalisation", {})
    
    target_value = currency_checks['target_value']
    if "dq_flag" not in df.columns:
        df = df.withColumn("dq_flag", lit(""))
    
    df = df.withColumn("dq_flag",when(col("currency").isNotNull() & (~col("currency").isin(target_value)), lit(f"CURRENCY_VARIANT")
        ).otherwise(col("dq_flag"))
    )

    return df

#***************** Run rule date_format_checks
def dq_date_format_checks(df, rules, table):
    iso_format = "yyyy-MM-dd"
    date_columns = rules.get("date_format_checks", {})
    
    if "dq_flag" not in df.columns:
        df = df.withColumn("dq_flag", lit(""))
        
    for date_col in date_columns:
        if date_col in df.columns:
            df = df.withColumn("dq_flag",when(col(date_col).isNotNull() & to_date(col(date_col),iso_format).isNull(), lit(f"DATE_FORMAT")
                ).otherwise(col("dq_flag"))
            )
    
    return df

#*****************Run rule referential_integrity
def dq_referential_integrity(df_list, rules, table):
    ref_integrity_rules = rules.get("referential_integrity", {}).get(table, [])
    
    # map dataframe names 
    df_map = {name: df for name, df in df_list}
    
    for ref_integrity_rule in ref_integrity_rules:
        field = ref_integrity_rule["field"]
        ref_table_column = ref_integrity_rule["references"]
        ref_table = ref_table_column.split('.')[0]
        ref_column = ref_table_column.split('.')[1]
        flag = ref_integrity_rule["dq_flag"]
        
        df_main = df_map[table]
        df_ref = df_map[ref_table]
        
        #df_ref = df_ref.filter(col("account_id") !='60d314de-eff7-d59f-465c-a643b0688fc6')
                
        if "dq_flag" not in df_main.columns:
            df_main = df_main.withColumn("dq_flag", lit(""))
        
        # Get all the records from main table that are not in referenced table
        orphans = df_main.join(df_ref, df_main[field] == df_ref[ref_column], "left_anti").select(field).withColumn("is_orphan", lit(1))
        
        # Join them back to main table marking them as orphans
        df = df_main.join(orphans, field,"left"
        ).withColumn("dq_flag",  when(orphans["is_orphan"].isNotNull(),  lit(flag)).otherwise(col("dq_flag"))).drop("is_orphan")
        
    return df

