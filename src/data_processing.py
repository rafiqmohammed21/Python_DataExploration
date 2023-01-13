import shutil
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline
from pyspark.sql.types import IntegerType, FloatType

# Spark Session Initiated
spark = SparkSession.builder.master("local[1]").appName('project1').getOrCreate()

# Input file path
file_path = "C:\\Users\\91790\\PycharmProjects\\project1-main\\dataset\\nyc-jobs.csv"


def data_processing(df):
    ''''
    Data Processing method using pyspark
    Input: Dataframe
    Output: Parquet file output
    '''
    # Cleaning
    df1 = df.fillna('') # Other way to clean
    print(df1)
    df2 = df.fillna(0)

    # column pre-processing
    df3 = df2.withColumn('Country', F.lit('United States of America'))
    df4 = df3.withColumnRenamed('Minimum Qual Requirements', 'Expectations')

    # data wrangling
    df5 = df4.drop('Country')
    df6 = df5[df5['# Of Positions']>1]
    df7 = df6.groupby('# Of Positions').mean()
    print(df7)

    # Transformation
    df8 = df5.withColumn("Salary Range To", df5["Salary Range To"].cast(FloatType())).withColumn("Salary Range From", df5["Salary Range From"].cast(FloatType()))\
        .withColumn("Job ID", df5["Job ID"].cast(IntegerType()))
    df9 = df8.withColumn('Work Location', F.trim(F.col('Work Location')))

    # Feature Engineering #

    # Vector Assembler
    inputCols = ['Salary Range From', 'Job ID', 'Salary Range To']
    outputCol = "features"
    df10 = VectorAssembler(inputCols = inputCols, outputCol = outputCol)
    df11 = df10.transform(df9)
    print(df11)

    # Bucketing
    spark.conf.get("spark.sql.sources.bucketing.enabled")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    df12 = spark.range(1, 16000, 1, 16).select(F.col("id").alias("key"), F.rand(12).alias("value"))
    shutil.rmtree("spark-warehouse")
    df12.write.saveAsTable("unbucketed", format="parquet", mode="overwrite")
    df12.write.bucketBy(16, "key").sortBy("value").saveAsTable("bucketed", format="parquet", mode="overwrite")

    # Scaler Function
    scaler = MinMaxScaler(inputCol="features", outputCol="x_scaled")
    pipeline = Pipeline(stages=[df10, scaler])
    scalerModel = pipeline.fit(df9)
    scaledData = scalerModel.transform(df9)
    scaledData.write.parquet('C:\\Users\\91790\\PycharmProjects\\project1-main\\dataset\\output')
    print("Success")
    return "Success"


if __name__ == "__main__":
    df = spark.read.option("multiline", "true").option("quote", '"').option("header", "true").option("escape","\\").option("escape", '"').csv(file_path, header=True)
    output = data_processing(df)
    print ("Executed when invoked directly")
    print(output)