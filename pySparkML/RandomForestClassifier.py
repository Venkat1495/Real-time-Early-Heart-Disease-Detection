from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName('RandomForestClassifier')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    new_df = spark.read.csv('heart.csv',
                            header=True, inferSchema=True)

    # drops rows with any null value parameters
    new_df = new_df.na.drop(how='any')

    # counts the number of each target class
    new_df.groupby("target").count().show()

    # prints the first 20 rows from the dataframe
    new_df.show()

    required_features = ['age', 'sex', 'cp', 'trestbps', 'chol', 'fbs', 'restecg', 'thalach',
                         'exang', 'oldpeak', 'slope', 'ca', 'thal']

    # grouping all features into a single vector row
    assembler = VectorAssembler(inputCols=required_features, outputCol='features')
    transformed_data = assembler.transform(new_df)
    transformed_data.show()

    # splitting the data
    (training_data, test_data) = transformed_data.randomSplit([0.7, 0.3])

    # Random Forest Classifier

    random_forest = RandomForestClassifier(labelCol='target', featuresCol='features', maxDepth=7,
                                           numTrees=10)
    # training the model
    random_forest_mdl = random_forest.fit(training_data)
    # testing with the model
    random_forest_prd = random_forest_mdl.transform(test_data)
    multi_evaluator = MulticlassClassificationEvaluator(labelCol='target', metricName='accuracy')
    print('Random Forest Classifier Accuracy', multi_evaluator.evaluate(random_forest_prd))

    # saving the model
    path = "./models/rfTrainedModel"
    random_forest_mdl.save(path=path)


if __name__ == "__main__":
    main()
