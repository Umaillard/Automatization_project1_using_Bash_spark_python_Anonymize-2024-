import sys
import os
import datetime
import openpyxl
from typing import List, Tuple, Optional
import io
import pandas as pd
import unicodedata
import re
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType


HiveDatabase = "your_database"

try:
    spark = SparkSession.builder \
        .appName("ProcessExcelAndAppendHistory") \
        .config("spark.sql.session.timeZone", "UTC") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession initialized successfully.")
except Exception as e:
    print(f"FATAL: Failed to initialize SparkSession: {e}", file=sys.stderr)
    sys.exit(1)

def normalize_text_for_matching(text: str) -> str:
    if not text:
        return ""
    nfkd_form = unicodedata.normalize('NFKD', text)
    only_ascii = nfkd_form.encode('ascii', 'ignore').decode('utf-8', 'ignore')
    lower_case = only_ascii.lower()
    no_whitespace = re.sub(r'\s+', '', lower_case)
    return no_whitespace

def find_resilient_sheet_name(
    spark_context,
    hdfs_excel_path: str,
    target_prefix_raw: str
) -> str:
    normalized_target_prefix = normalize_text_for_matching(target_prefix_raw)
    print(f"Normalized target prefix for sheet search: '{normalized_target_prefix}'")
    try:
        file_content_rdd = spark_context.binaryFiles(hdfs_excel_path, minPartitions=1).take(1)
        if not file_content_rdd:
            raise FileNotFoundError(f"Excel file not found or empty at HDFS path: {hdfs_excel_path}")
        excel_bytes = file_content_rdd[0][1]
        with io.BytesIO(excel_bytes) as excel_buffer:
            workbook = openpyxl.load_workbook(excel_buffer, read_only=True)
            available_sheet_names = workbook.sheetnames
            workbook.close()

        print(f"Available sheet names in '{os.path.basename(hdfs_excel_path)}': {available_sheet_names}")
        for sheet_name_original in available_sheet_names:
            normalized_current_sheet = normalize_text_for_matching(sheet_name_original)
            if normalized_current_sheet.startswith(normalized_target_prefix):
                print(f"Matching sheet found: '{sheet_name_original}' (normalized: '{normalized_current_sheet}')")
                return sheet_name_original
        raise ValueError(
            f"No sheet found starting with '{target_prefix_raw}' (normalized: '{normalized_target_prefix}') "
            f"in Excel file '{hdfs_excel_path}'. Available sheets: {available_sheet_names}"
        )
    except Exception as e:
        print(f"Error while trying to list sheets from '{hdfs_excel_path}': {e}", file=sys.stderr)
        raise Exception(f"Could not list sheets from Excel file '{hdfs_excel_path}'. Original error: {e}")

def get_resilient_column_names(actual_columns: List[str], target_prefixes_raw: List[str]) -> List[str]:
    selected_original_cols = []
    normalized_target_prefixes = [normalize_text_for_matching(p) for p in target_prefixes_raw]
    for norm_prefix in normalized_target_prefixes:
        for actual_col_original in actual_columns:
            normalized_actual_col = normalize_text_for_matching(actual_col_original)
            if normalized_actual_col.startswith(norm_prefix):
                if actual_col_original not in selected_original_cols:
                    selected_original_cols.append(actual_col_original)

    if not selected_original_cols:
        print(f"Warning: No columns were selected by get_resilient_column_names for prefixes: {target_prefixes_raw} from {actual_columns}")
    return selected_original_cols

def quote_column_if_needed(col_name: str) -> str:
    if re.search(r'[\s\(\)-]', col_name) and not (col_name.startswith('`') and col_name.endswith('`')):
        return f"`{col_name}`"
    return col_name

def find_category_columns_for_aggregation(actual_columns: List[str]) -> Tuple[Optional[str], Optional[str]]:
    col_category_a_name = None
    col_category_b_name = None

    def normalize_for_substring_search(text: str) -> str:
        nfkd_form = unicodedata.normalize('NFKD', text)
        only_ascii = nfkd_form.encode('ascii', 'ignore').decode('utf-8', 'ignore')
        return only_ascii.lower().replace(" ", "")

    norm_marker_a = normalize_for_substring_search("(cat-a)")
    norm_marker_b = normalize_for_substring_search("(cat-b)")
    norm_eligibility_prefix = normalize_text_for_matching("eligibility")

    for col_name in actual_columns:
        norm_col_full = normalize_text_for_matching(col_name)
        norm_col_substring = normalize_for_substring_search(col_name)

        if norm_col_full.startswith(norm_eligibility_prefix):
            if norm_marker_a in norm_col_substring and not col_category_a_name:
                col_category_a_name = col_name
            elif norm_marker_b in norm_col_substring and not col_category_b_name:
                col_category_b_name = col_name


    return col_category_a_name, col_category_b_name


def process_excel(hdfs_excel_path: str) -> None:
    target_sheet_prefix_raw = "SheetToProcess"
    table_hive_inter = HiveDatabase + '.' + "intermediate_table"
    target_hive_table = HiveDatabase + '.' + "history_log"

    excel_schema_for_reader = StructType([
                                StructField("id_column", StringType(), True),
                                StructField("desc_column", StringType(), True),
                                StructField("Eligibility (cat-a)", StringType(), True),
                                StructField("Eligibility (cat-b)", StringType(), True)
                                ])

    print(f"Processing HDFS Excel file: {hdfs_excel_path}")

    try:
        excel_sheet_name = find_resilient_sheet_name(spark.sparkContext, hdfs_excel_path, target_sheet_prefix_raw)

        print(f"Reading Excel sheet '{excel_sheet_name}' from: {hdfs_excel_path} using pandas...")

        file_content_rdd = spark.sparkContext.binaryFiles(hdfs_excel_path, minPartitions=1).take(1)

        if not file_content_rdd:
            raise FileNotFoundError(f"Excel file not found or empty at HDFS path: {hdfs_excel_path}")

        excel_bytes = file_content_rdd[0][1]

        with io.BytesIO(excel_bytes) as excel_buffer:
            df_pandas = pd.read_excel(
                excel_buffer,
                sheet_name=excel_sheet_name,
                header=0,
            )

        print(f"Excel file read into Pandas DataFrame with {len(df_pandas)} rows.")

        df_spark_raw = spark.createDataFrame(df_pandas)

        print(f"Spark DataFrame created with {df_spark_raw.count()} rows and the following schema:")
        df_spark_raw.printSchema()

        raw_count = df_spark_raw.count()
        print(f"Number of rows initially read (before filtering): {raw_count}")

        id_col_candidates = get_resilient_column_names(df_spark_raw.columns, ["id_column"])
        if not id_col_candidates:
            raise ValueError(f"Column 'id_column' not found in headers: {df_spark_raw.columns}")
        actual_id_col_for_filter = id_col_candidates[0]

        df_spark = df_spark_raw.filter(
            F.col(quote_column_if_needed(actual_id_col_for_filter)).isNotNull() & \
            (F.trim(F.col(quote_column_if_needed(actual_id_col_for_filter))) != "")
        )

        initial_row_count = df_spark.count()
        print(f"Number of valid rows (after filtering on '{actual_id_col_for_filter}'): {initial_row_count}")

        actual_id_col_for_select = actual_id_col_for_filter

        actual_cat_a_col, actual_cat_b_col = find_category_columns_for_aggregation(df_spark.columns)

        if not actual_id_col_for_select:
            raise ValueError(f"Semantic column 'id_column' not found for selection.")
        if not actual_cat_a_col:
            print(f"Warning: Column for 'Eligibility (cat-a)' not found dynamically among {df_spark.columns}.")
        if not actual_cat_b_col:
            print(f"Warning: Column for 'Eligibility (cat-b)' not found dynamically among {df_spark.columns}.")

        select_expressions = []
        if actual_id_col_for_select:
            select_expressions.append(F.col(quote_column_if_needed(actual_id_col_for_select)).alias("id_column"))
        if actual_cat_a_col:
            select_expressions.append(F.col(quote_column_if_needed(actual_cat_a_col)).alias("eligibility_cat_a"))
        if actual_cat_b_col:
            select_expressions.append(F.col(quote_column_if_needed(actual_cat_b_col)).alias("eligibility_cat_b"))

        if not select_expressions:
             raise ValueError("No columns could be prepared for the intermediate table.")

        df_for_hive_intermediate = df_spark.select(*select_expressions)

        print(f"Columns for intermediate Hive table '{table_hive_inter}': {df_for_hive_intermediate.columns}")
        df_for_hive_intermediate.write.mode('overwrite').saveAsTable(table_hive_inter)
        print(f"Refreshing table metadata: {table_hive_inter}")
        spark.sql(f'REFRESH TABLE {table_hive_inter}')

        excel_file_name_only = os.path.basename(hdfs_excel_path)
        processing_timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

        count_cat_a = 0
        count_cat_b = 0

        if initial_row_count > 0:
            aggregations_list = []
            if actual_cat_a_col:
                aggregations_list.append(F.sum(F.col(quote_column_if_needed(actual_cat_a_col)).cast("long")).alias("sum_cat_a"))
            else:
                aggregations_list.append(F.lit(0).alias("sum_cat_a"))

            if actual_cat_b_col:
                aggregations_list.append(F.sum(F.col(quote_column_if_needed(actual_cat_b_col)).cast("long")).alias("sum_cat_b"))
            else:
                aggregations_list.append(F.lit(0).alias("sum_cat_b"))

            if aggregations_list:
                summary_stats_row = df_spark.agg(*aggregations_list).first()
                if summary_stats_row:
                    count_cat_a = summary_stats_row["sum_cat_a"] if summary_stats_row["sum_cat_a"] is not None else 0
                    count_cat_b = summary_stats_row["sum_cat_b"] if summary_stats_row["sum_cat_b"] is not None else 0

        print(f"Calculated stats: CategoryA={count_cat_a}, CategoryB={count_cat_b}")

        historical_data = [(
            excel_file_name_only,
            processing_timestamp_str,
            count_cat_a,
            count_cat_b
        )]
        historical_schema = StructType([
            StructField("filename", StringType(), True),
            StructField("processing_timestamp", StringType(), True),
            StructField("Count_Category_A", LongType(), True),
            StructField("Count_Category_B", LongType(), True)
        ])
        df_historical_spark = spark.createDataFrame(data=historical_data, schema=historical_schema)

        print(f"Appending record to Hive table: {target_hive_table}")
        df_historical_spark.write.mode('append').saveAsTable(target_hive_table)
        spark.sql(f'REFRESH TABLE {target_hive_table}')

    except Exception as e:
        print(f"An error occurred during Spark processing: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    script_name = os.path.basename(__file__)
    print (sys.argv)
    if len(sys.argv) != 2:
        print(f"Usage: spark-submit <options> {script_name} <hdfs_input_excel_path>", file=sys.stderr)
        sys.exit(1)

    excel_file_hdfs_arg = sys.argv[1]

    try:
        process_excel(excel_file_hdfs_arg)
        print(f"{script_name} finished successfully.")
    except Exception as e:
        print(f"FATAL: {script_name} failed. See error messages above.", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'spark' in globals() and spark:
            spark.stop()
            print("SparkSession stopped.")
