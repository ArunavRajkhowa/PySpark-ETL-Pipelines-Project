from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class ETLProcess:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.spark = SparkSession.builder \
            .appName("AdvancedETL") \
            .getOrCreate()

    def extract(self):
        try:
            df = self.spark.read.csv(self.input_path, header=True, inferSchema=True)
            return df
        except Exception as e:
            print("Error occurred during data extraction:", str(e))
            return None

    def transform(self, df):
        try:
            transformed_df = transformed_df = df.filter(col("GarageArea") > 500)
            # Add more transformations as needed
            return transformed_df
        except Exception as e:
            print("Error occurred during data transformation:", str(e))
            return None

    def load(self, transformed_df):
        try:
            transformed_df.write.mode("overwrite").csv(self.output_path)
            print("Data loaded successfully.")
        except Exception as e:
            print("Error occurred during data loading:", str(e))

    def run_etl(self):
        df = self.extract()
        if df is not None:
            transformed_df = self.transform(df)
            if transformed_df is not None:
                self.load(transformed_df)
        self.spark.stop()
