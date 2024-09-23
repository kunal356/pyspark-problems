from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_survey_df, null_count_percent, create_range, get_value_count, get_max_count


class UtilsTestCase(TestCase):
    spark = None
    dataframe_schema = """
        `Employee Name` STRING,
        Age INT,
        Salary INT,
        City STRING
        """

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("PySparkTest") \
            .config("spark.driver.extraJavaOptions",
                    "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark") \
            .getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_survey_df(spark=self.spark, filename="data/test.csv", schema=self.dataframe_schema)
        result_count = sample_df.count()
        # Testing with sample file
        self.assertEqual(result_count, 10,
                         "Total number of records count should be 10")
        
    def test_null_percent(self):
        data_df = load_survey_df(spark=self.spark, filename="data/test.csv")
        result = null_count_percent(data_df=data_df).collect()
        
        result_dict = dict()
        for col,row in result[0].asDict().items():
            result_dict[col] = row
        self.assertEqual(result_dict["Employee Name Null%"],0,"Null percent should be 0")
        self.assertEqual(result_dict["Age Null%"],40,"Null`%` should be 40")
        self.assertEqual(result_dict["Salary Null%"],50,"Null`%` should be 50")
        self.assertEqual(result_dict["City Null%"],50,"Null`%` should be 50")

    def test_create_range(self):
        data_df = load_survey_df(spark=self.spark, filename="data/test.csv", schema=self.dataframe_schema)
        result = create_range(data_df=data_df, col_name="Salary").collect()
        result_dict = dict()
        for row in result:
            result_dict[row["Salary"]] = row["Range"]
        self.assertEqual(result_dict[19000], "Medium", "The value should be Medium")
        self.assertEqual(result_dict[None], "Unknown", "The value should be Unkown")
        self.assertEqual(result_dict[5000], "Low", "The value should be Low")
    
    def test_value_count(self):
        data_df = load_survey_df(spark=self.spark, filename="data/test.csv", schema=self.dataframe_schema)
        result = get_value_count(data_df=data_df, col_name="Salary").collect()
        result_dict = dict()
        for row in result:
            result_dict[row["Salary"]] = row["count"]
        self.assertEqual(result_dict[100000],1,"The value should be 1")
        self.assertEqual(result_dict[None],5,"The value should be 5")
        self.assertEqual(result_dict[60000],1,"The value should be 1")
        self.assertEqual(result_dict[19000],1,"The value should be 1")
    
    def test_max_count(self):
        data_df = load_survey_df(spark=self.spark, filename="data/test.csv", schema=self.dataframe_schema)
        result = get_max_count(data_df=data_df, col_name="Salary").collect()
        result_dict = dict()
        print(result)
        for row in result:
            result_dict[row["Salary"]] = row["count"]
        self.assertEqual(result_dict[None],5,"The value should be 5")
        


    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
