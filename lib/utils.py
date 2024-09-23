from pyspark.sql import *
from pyspark.sql.functions import *


def load_survey_df(spark, filename, schema=None):
    if schema:
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .schema(schema) \
            .load(filename)
        return df
    else:
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(filename)
        return df


def write_survey_df(data_df, path, mode="overwrite", maxRecords=10000):
    data_df.write \
        .option("path", path) \
        .mode(mode) \
        .option("maxRecordsPerFile", maxRecords) \
        .save()


def null_count_percent(data_df):
    total_count = data_df.count()
    null_count = data_df.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in data_df.columns])
    final_df = null_count.select(
        [((col(c)/total_count)*100).alias(f"{c} Null%") for c in data_df.columns])
    return final_df


def count_by_country(survey_df):
    return survey_df \
        .where("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()


def set_range(value):
    if value is None:
        return "Unknown"
    elif value < 10000:
        return "Low"
    elif value < 50000:
        return "Medium"
    else:
        return "High"


def create_range(data_df, col_name):
    data_df2 = data_df.withColumn(
        "Range",
        when(col(col_name).isNull(), "Unknown")
        .when(col(col_name) < 10000, "Low")
        .when(col(col_name) < 50000, "Medium")
        .otherwise("High")
    )
    return data_df2


def get_value_count(data_df, col_name):
    return data_df.groupBy(col_name).count()


def get_max_count(data_df, col_name):
    return data_df.groupBy(col_name).count().sort(desc("count"), col_name)


def get_max_exp_industry_name(data_df):
    max_value = data_df.agg({"Value": "max"}).collect()[0][0]
    return data_df.select("Industry_name_NZSIOC").where(data_df["Value"] == max_value)
