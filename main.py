from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.utils import load_survey_df, write_survey_df, null_count_percent, create_range, get_value_count, get_max_count, get_max_exp_industry_name
from lib.logger import Log4j

dataframe_schema = """
Year INT,
Industry_aggregation_NZSIOC STRING,
Industry_code_NZSIOC INT,
Industry_name_NZSIOC STRING,
Units STRING,
Variable_code STRING,
Variable_name STRING,
Variable_category STRING,
Value INT,
Industry_code_ANZSIC06 STRING
"""

spark = SparkSession \
    .builder \
    .appName("Hello Spark") \
    .config("spark.driver.extraJavaOptions",
            "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark") \
    .master("local[3]") \
    .getOrCreate()

logger = Log4j(spark)
logger.info("Starting Spark")

data_df = load_survey_df(
    spark=spark, filename="data/survey2023.csv", schema=dataframe_schema)
write_survey_df(data_df=data_df, path='dataSink/parquet', mode="overwrite")
data1 = null_count_percent(data_df=data_df)
data1.show()

data2 = create_range(data_df=data_df, col_name="Value")
data2.show()

data3 = get_value_count(data_df=data_df, col_name="variable_code")
data3.show()

data4 = get_max_count(data_df=data_df, col_name="variable_code")
data4.show()

data5 = get_max_exp_industry_name(data_df=data_df)
data5.show()
