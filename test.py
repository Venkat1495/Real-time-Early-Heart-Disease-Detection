import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType


def main():
    spark = SparkSession.builder.appName('test').getOrCreate()

    # schema = StructType([
    #     StructField("age", IntegerType(), True),
    #     StructField("sex", BooleanType(), True),
    #     StructField("cp", IntegerType(), True),
    #     StructField("trestbps", IntegerType(), True),
    #     StructField("chol", IntegerType(), True),
    #     StructField("fbs", BooleanType(), True),
    #     StructField("restecg", IntegerType(), True),
    #     StructField("thalach", IntegerType(), True),
    #     StructField("exang", BooleanType(), True),
    #     StructField("oldpeak", FloatType(), True),
    #     StructField("slope", IntegerType(), True),
    #     StructField("ca", IntegerType(), True),
    #     StructField("thal", IntegerType(), True),
    #     StructField("target", BooleanType(), True)
    # ])

    new_df = spark.read.csv('C:/Users/Brijesh Prajapati/Downloads/archive/heart.csv',
                            header=True, inferSchema=True)
    # new_df.printSchema()
    # new_df = new_df.groupby("target").count()
    # new_df.show()


if __name__ == "__main__":
    print("Hello World")
    # print(pyspark.__version__)
    main()
