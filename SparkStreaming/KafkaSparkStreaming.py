import json
import uuid

from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.tree import RandomForestModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import col, round
from pyspark import SparkContext
sc = SparkContext("local", "First App")
KAFKA_TOPIC_NAME = "New_topic_4"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

if __name__ == "__main__":

    # creating spark session object
    spark = (
        SparkSession.builder.master("local[*]").
        appName("KafkaSparkStreaming").
        getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")


    # read data stream from kafka topic
    dataFrame = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    dataFrame.printSchema()

    
    #applying the data schema
    inputDataSchema = StructType([
        StructField("age", IntegerType(), False),
        StructField("sex", IntegerType(), False),
        StructField("cp", IntegerType(), False),
        StructField("trestbps", IntegerType(), False),
        StructField("chol", IntegerType(), False),
        StructField("fbs", IntegerType(), False),
        StructField("restecg", IntegerType(), False),
        StructField("thalach", IntegerType(), False),
        StructField("exang", IntegerType(), False),
        StructField("oldpeak", FloatType(), False),
        StructField("slope", IntegerType(), False),
        StructField("ca", IntegerType(), False),
        StructField("thal", IntegerType(), False),
    ])

    # Extracting the value alone from the kafka data and applying the schema
    clean_DataFrame = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .withColumn("value", F.from_json("value", inputDataSchema)) \
        .select(F.col('value.*'))

    print(clean_DataFrame)

    clean_DataFrame.printSchema()

    # Listing all the columns in a list
    required_features = ['age', 'sex', 'cp', 'trestbps', 'chol', 'fbs', 'restecg', 'thalach',
                         'exang', 'oldpeak', 'slope', 'ca', 'thal']

    # Adding the vector module to match the schema in the Machine Learning model
    assembler = VectorAssembler(inputCols=required_features, outputCol='features')
    transformed_data = assembler.transform(clean_DataFrame)
    # transformed_data.show()
    # transformed_data.printSchema()



    # Loadin the Trained Meachine Learning Model
    pipeline = PipelineModel.load("/Users/csuftitan/Repos/cpsc531-project/pySparkML/models/rfTrainedModel")

    # Getting the Predections of the data from kafka
    transformeddataset = pipeline.transform(transformed_data)

    # Adding the uuid to work as primary key in cassandra
    uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())
    df = transformeddataset.withColumn("id", uuidUdf())

    # Writing the data to cassandra table 
    def writeToCassandra(writeDF, epochId):
        writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="heart_prediction_data", keyspace="cas") \
        .save()

    # Droping some of the vector columns and connecting to cassandra sever and calling writing function
    posts_stream = df \
        .drop("features","rawPrediction","probability") \
        .writeStream \
        .option("spark.cassandra.connection.host", "localhost:9042") \
        .foreachBatch(writeToCassandra) \
        .outputMode("update") \
        .start()

    # waiting till we terminata the section 
    posts_stream.awaitTermination()
