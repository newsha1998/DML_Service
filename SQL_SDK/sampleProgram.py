import sdk

table1 =None
database1=None

sdk.CreateDatabase('temp')
sdk.CreateTable('student','id INT, name STRING, age INT')
#sdk.InsertDate('student','101 , \'123 Park Ave, San Jose \', 111111')

#table1=sdk.ImportData('SQl_SDK/temp.csv','test','csv','true')

#sdk.ShowColumn(table1), 

#sdk.Run_Sql("select * from student")

#sdk.Run_Sql("select * from test")


