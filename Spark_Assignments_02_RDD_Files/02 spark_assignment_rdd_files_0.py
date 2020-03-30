'''
desc
2.	Consider the following file sample.txt with  in ur local environment. Answer each question by creating new RDD for it.

“ Welcome to spark L2 session.  This is a text file which contains text for the demonstration of text files with spark. Spark is from Apache and this is going to be very useful demonstration for all. Happy learning with spark to all.  Bye for now.”

a)	Display file using RDD
b)	Display first line of sample.txt.
c)	Display first 2 lines of sample.txt
d)	Identify how many words are there and provide word count.

'''

from pyspark import SparkContext
from pathlib import Path
sc = SparkContext()

baseDir = Path.cwd()
inputFile = baseDir.joinpath("Inputs", "sample.txt")

fileRDD = sc.textFile(str(inputFile))

print("***** ASSIGNMENT – 2 : Working with associative arrays *****")
print("2. Considering the file sample.txt:")
print("a) Display file using RDD:\n", fileRDD.collect())
lineRDD = fileRDD.map(lambda x: x.split(".")).collect()
print("b) First line of 'sample.txt' is: \n", str(lineRDD[0][0]) + '.')
print("c) First 2 lines of 'sample.txt' are: \n", str(lineRDD[0][0]) + '.\n' +
      str(lineRDD[0][1]) + '.')
wordCount = fileRDD.map(lambda x: x.replace(".", "")).flatMap(lambda x: x.split(" "))\
    .map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
print("d) Number of words in 'sample.txt' is: \n", fileRDD.flatMap(lambda x: x.split(" ")).count(),
      "\nAnd there word count is: \n", wordCount.collect())

# flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect())




