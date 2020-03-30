'''
desc

“Welcome to spark L2 session.  This is a text file which contains text for the demonstration of text files with spark.
Spark is from Apache and this is going to be very useful demonstration for all. Happy learning with spark to all.
Bye for now.”

3.Consider the sample.txt file and answer the following using individual RDDs.

a)	Display all tokens in the given file separated by a space
b)	Display all distinct words from the text files.
c)	Provide word count of given file.

'''

from pyspark import SparkContext
from pathlib import Path
sc = SparkContext()

baseDir = Path.cwd()
inputFile = baseDir.joinpath("Inputs", "sample.txt")

fileRDD = sc.textFile(str(inputFile))

print("***** ASSIGNMENT – 2 : Working with associative arrays *****")
print("3. Considering the file sample.txt:")
tokens = fileRDD.map(lambda x: x.replace(".", "")).flatMap(lambda x: x.split(" "))
print("a) All the tokens in the given text file are: \n",
      tokens.collect())
print("b) All the distinct words in the given text file are: \n",
      tokens.distinct().collect())
print("c) Word count of the given text file is: ", tokens.count(),
      "\n   With distinct word count been: ", tokens.distinct().count())




