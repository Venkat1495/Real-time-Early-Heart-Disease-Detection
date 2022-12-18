from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession


def main():

    # Creating Spark Connection
    spark = (
        SparkSession.builder.master("local[*]")
        .appName('RandomForestClassifier')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Reading Heart Data
    new_df = spark.read.csv('heart.csv',
                            header=True, inferSchema=True)

    # drops rows with any null value parameters
    new_df = new_df.na.drop(how='any')

    # counts the number of each target class
    new_df.groupby("target").count().show()

    # prints the first 20 rows from the dataframe
    new_df.show()
    new_df.printSchema()

    # Listing all the columns in a list
    required_features = ['age', 'sex', 'cp', 'trestbps', 'chol', 'fbs', 'restecg', 'thalach',
                         'exang', 'oldpeak', 'slope', 'ca', 'thal']

    # grouping all features into a single vector row
    assembler = VectorAssembler(inputCols=required_features, outputCol='features')
    transformed_data = assembler.transform(new_df)
    transformed_data.show()

    # splitting the data
    (training_data, test_data) = transformed_data.randomSplit([0.7, 0.3])

    transformed_data.printSchema()


    # Random Forest Classifier
    random_forest = RandomForestClassifier(labelCol='target', featuresCol='features', maxDepth=7,
                                           numTrees=10)
    stages = [random_forest]

    # Joining the stages
    pipeline = Pipeline(stages=stages)

    # training the model
    model = pipeline.fit(training_data)
    #random_forest_mdl = random_forest.fit(training_data)

    # testing with the model
    random_forest_prd = model.transform(test_data)
    multi_evaluator = MulticlassClassificationEvaluator(labelCol='target', metricName='accuracy')
    print('Random Forest Classifier Accuracy', multi_evaluator.evaluate(random_forest_prd))

    # saving the model
    path = "./models/rfTrainedModel"
    model.save(path=path)



if __name__ == "__main__":
    main()
