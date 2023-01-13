from pyspark.sql.functions import avg
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, FloatType

# Spark Session
spark = SparkSession.builder.master("local[1]").appName('project1').getOrCreate()
file_path = "C:\\Users\\91790\\PycharmProjects\\project1-main\\dataset\\nyc-jobs.csv"


def data_exploration(df):
    ''''
    Data Processing method using pyspark
    Input: Dataframe
    Output: Parquet file output
    '''
    # Cleaning
    df = df.fillna(0)

    # Describe column
    for col in df.columns:
         df.describe([col]).show()

    # Drop column
    df = df.drop("Work Location 1")
    for col in df.columns:
         df.describe([col]).show()

    # check unique values for class
    df.select('# Of Positions').distinct().collect()

    # filter out those rows
    df.filter(df['Job Category'] == "Information Technology & Telecommunications").show()

    # Find the average of a column
    mean_value = df.select(avg(df['# Of Positions'])).show()
    print(mean_value)

    # Count the value of null in every column
    for col in df.numeric_cols:
        print(col, "\t", "with null values: ", df.filter(df[col].isNull()).count())

    # Changing datatype of the columns in dataframe
    df1 = df.withColumn("# Of Positions", df["# Of Positions"].cast(IntegerType())).withColumn("Salary Range To", df["Salary Range To"].cast(FloatType()))

    # Adding a new column to the dataframe with computations for KPI calculations
    df1 = df1.withColumn("Salary", F.when((df1["Salary Frequency"] == "Annual"), F.lit(df["Salary Range To"]/12)).when((df1["Salary Frequency"] == "Hourly"), F.lit(df["Salary Range To"]*8*30)).otherwise(F.lit(df["Salary Range To"]))).show()

    # List of KPIs to be resolved

    # What's the number of jobs posting per category (Top 10)
    df2 = df1.groupBy("Job Category").sum("# Of Positions")
    df2.show()

    # What's the salary distribution per job category?
    df3 = df1.groupBy("Job Category").sum("Salary").show()
    df3.show()

    # Is there any correlation between the higher degree and the salary?
    df4 = df1.stat.corr("Civil Service Title", "Salary").show()
    df4.show()

    # What's the job posting having the highest salary per agency?
    df5 = df1.groupBy("Agency").max("Salary").show()
    df5.show()

    # What's the job postings average salary per agency for the last 2 years?
    df6 = df1.groupBy("Agency").avg("Salary").filter(F.year("Posting Date").geq(F.lit(2021))).show()
    df6.show()

    # What are the highest paid skills in the US market?
    df7 = df1.agg({"x": "Salary"}).collect()[0]
    df7.show()

    print("Success")


if __name__ == "__main__":
    df = spark.read.option("multiline", "true").option("quote", '"').option("header", "true").option("escape","\\").option("escape", '"').csv(file_path, header=True)
    output = data_exploration(df)
    print ("Executed when invoked directly")
    print(output)