'''
desc
ASSIGNMENT – 3  SPARK SQL using json file

Note : provide the commands along with output.

Consider the following json file as datasource.
	 {"id" : "101", "ename" : "Ram", "age" : "29"}
    {"id" : "102", "ename" : "Sai", "age" : "30"}
   {"id" : "103", "ename" : "joe", "age" : "32"}
   {"id" : "104", "ename" : "sirish", "age" : "30"}
   {"id" : "105", "ename" : "shekhar", "age" : "32"}

1.	From the above json file answer the following.
a)	Display file data using data frame.
b)	Display the structure of data frame .
c)	Display only enames from the given file using select.


2.	From the json file given, answer the following.
a)	Provide the count of each age.
b)	Provide all records in sorted order.
c)	Display  records whose age <=30.
'''

from pathlib import Path
from pyspark import SparkContext, SQLContext
baseDir = Path.cwd()
inputFile = baseDir.joinpath("Inputs", "emp.json")
sc = SparkContext()
sqlc = SQLContext(sc)

dfs = sqlc.read.json(str(inputFile))

print("***** ASSIGNMENT – 3 : Spark SQL using json file *****")
print("1. From the above json file:")
print("a) Data using data frame is: \n")
dfs.show()
print("b) Structure of the data frame is: \n")
dfs.printSchema()
print("c) 'enames' from the given file using select: \n")
dfs.select('ename').show()

print("2. From the above json file:")
print("a) Count of each age is: \n")
dfs.groupby('age').count().show()
print("b) All records in sorted order are: \n")
dfs.orderBy('age').show()
print("c) Records whose age <=30 are: \n")
dfs.filter(dfs.age <= 30).show()
