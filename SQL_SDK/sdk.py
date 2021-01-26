from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit

#spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder \
    .master("local") \
    .appName("DML app") \
    .config("header", "true") \
    .getOrCreate() 

def CreateDatabase(databaseName):
    spark.sql('CREATE DATABASE ' + databaseName)
    print('Database Created.')

def CreateTable(tablealiasname,columnnametype='id INT, name STRING, age INT'):
    #spark.sql('CREATE  TABLE ' + tablealiasname + '(' + columnnametype +') USING csv OPTIONS(PATH \'' + tablealiasname +'.csv \' )')
    print('Table' , tablealiasname, 'created.')


def InsertDate(tablealiasname,cloumnvalue=' 101 , \'123 Park Ave, San Jose \', 111111'):
    spark.sql ('INSERT INTO ' +tablealiasname + ' VALUES (' +cloumnvalue + ')')
    print('Insert in to ' + tablealiasname + 'Done.')

def Run_Sql(sqlcommand):
    spark.sql(sqlcommand).show()

def ImportData(path,tablealiasname='Test',type='csv',header="true"):
    
    if type=="csv" : 
        df =spark.read.format("csv").option("header",header).load(path)  
        df.createOrReplaceTempView(tablealiasname)
        print("=== Print out schema ===")
        df.printSchema()
        return df
    elif type=="json":
        df =spark.read.format("json").option("header",header).load(path)  
        df.createOrReplaceTempView(tablealiasname)
        print("=== Print out schema ===")
        df.printSchema()
        return df

def ShowColumn(tablename):   
    print("Number of the column:" + str(len(tablename.columns)) )
    df_list=tablename.dtypes 
    for i in df_list:
          print(i)


def ChangeColumnType(tablename,columnName,newtype):
    if newtype=="Float":
        tablename=tablename.withColumn(columnName,tablename[columnName].cast(FloatType()))
        print("done")
        return tablename
    elif newtype=="String":
        tablename=tablename.withColumn(columnName,tablename[columnName].cast(StringType()))
        print("done")
        return tablename
    elif newtype=="Integer":
        tablename=tablename.withColumn(columnName,tablename[columnName].cast(IntegerType()))
        print("done")
        return tablename
    

def ChangeColumnName(tablename,columnNameOld,ColumnNameNew):
    tablename=tablename.withColumnRenamed(columnNameOld,ColumnNameNew)
    print("done")
    return tablename


def AddColumn(tablename,tablealiasname='Test',columnName='test1',columnType="String",defaultValue=None):
    
    if columnType=="String":
        tablename=tablename.withColumn(columnName,lit(defaultValue).cast(StringType()))
        tablename.createOrReplaceTempView(tablealiasname)
        print("done")
        return tablename
    elif columnType=="Integer":
        tablename=tablename.withColumn(columnName,lit(defaultValue).cast(IntegerType()))
        tablename.createOrReplaceTempView(tablealiasname)
        print("done")
        return tablename
    elif columnType=="Float":
        tablename=tablename.withColumn(columnName,lit(defaultValue).cast(FloatType()))
        tablename.createOrReplaceTempView(tablealiasname)
        print("done")
        return tablename

def DropNull(tablename):
    return tablename.na.drop().show()
  
def Save(tablename,outpath):
    tablename.write.csv(outpath)
