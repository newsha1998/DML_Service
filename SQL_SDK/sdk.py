from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit

#spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder \
    .master("local") \
    .appName("DML app") \
    .config("header", "true") \
    .getOrCreate() 

def run_sql(sqlcommand,dataframe):
    spark.sql(sqlcommand).show()

def importt(path,DataframName='Test',type='csv',header="true"):
    
    if type=="csv" : 
        df =spark.read.format("csv").option("header",header).load(path)  
        df.createOrReplaceTempView(DataframName)
        print("=== Print out schema ===")
        df.printSchema()
        return df
    elif type=="json":
        df =spark.read.format("json").option("header",header).load(path)  
        df.createOrReplaceTempView(DataframName)
        print("=== Print out schema ===")
        df.printSchema()
        return df

def ShowColumn(dataframe):   
    print("Number of the column:" + str(len(dataframe.columns)) )
    df_list=dataframe.dtypes 
    for i in df_list:
          print(i)


def ChangeColumntype(dataframe,columnName,newtype):
    if newtype=="Float":
        dataframe=dataframe.withColumn(columnName,dataframe[columnName].cast(FloatType()))
        print("done")
        return dataframe
    elif newtype=="String":
        dataframe=dataframe.withColumn(columnName,dataframe[columnName].cast(StringType()))
        print("done")
        return dataframe
    elif newtype=="Integer":
        dataframe=dataframe.withColumn(columnName,dataframe[columnName].cast(IntegerType()))
        print("done")
        return dataframe
    

def ChangeColumnName(dataframe,columnNameOld,ColumnNameNew):
    dataframe=dataframe.withColumnRenamed(columnNameOld,ColumnNameNew)
    print("done")
    return dataframe


def AddColumn(dataframe,dataframeName='Test',columnName='test1',columnType="String",defaultValue=None):
    
    if columnType=="String":
        dataframe=dataframe.withColumn(columnName,lit(defaultValue).cast(StringType()))
        dataframe.createOrReplaceTempView(dataframeName)
        print("done")
        return dataframe
    elif columnType=="Integer":
        dataframe=dataframe.withColumn(columnName,lit(defaultValue).cast(IntegerType()))
        dataframe.createOrReplaceTempView(dataframeName)
        print("done")
        return dataframe
    elif columnType=="Float":
        dataframe=dataframe.withColumn(columnName,lit(defaultValue).cast(FloatType()))
        dataframe.createOrReplaceTempView(dataframeName)
        print("done")
        return dataframe

def dropNull(dataframe):
    return dataframe.na.drop().show()
  
def Save(dataframe,outpath):
    dataframe.write.csv(outpath)
