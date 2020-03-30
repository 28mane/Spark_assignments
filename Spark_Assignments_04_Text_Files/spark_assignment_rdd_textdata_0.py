'''
desc
Consider the following text file.
India’s football team captain Sunil Chhetri recently took to Twitter pleading fans to support his team in a four-nation tournament held in Mumbai. Virat Kohli, the captain of national cricket team, doesn’t have to issue such appeals as he mostly plays in front of a full house. But football is gaining a following in India, more so after the launch of the professional Indian Super League. Over the next month, cricket will take a backseat as India joins the world in watching the FIFA World Cup.

1. Create RDD for above text file with 3 partitions.
2. display the contents of file using RDD.
3. create individual RDD for first 3 lines.
4. Give a wordcount for each token in the file and save it to another file.
5. Differentiate between functionality of map and flatmap.
6. Display data in each partition individually and load them into separate files.
'''
from pyspark import SparkContext, SQLContext
from pathlib import Path

baseDir = Path.cwd()
inputFile = baseDir.joinpath("Inputs", "Sports.txt")
outputpath = baseDir.joinpath("Outputs")

sc = SparkContext()
sqlc = SQLContext(sc)


rddtxt = sc.textFile(str(inputFile), 3)
numPartitions = rddtxt.getNumPartitions()

print("***** ASSIGNMENT – 4 : Spark Text Files and Partitions *****")
print("1. RDD for above text file with", numPartitions, "partitions is created successfully.")

print("\n2. contents of file using RDD is: ")
print(rddtxt.collect())

print("\n3. Individual RDD for first 3 lines is: ")
rddtxtsplit = rddtxt.map(lambda x: x.split(". ")).collect()
rddtxtline = []
for i in range(0, 3):
    rddtxtline.append(sc.parallelize(rddtxtsplit[0][i]))
for i in range(len(rddtxtline)):
    print(f"RDD for line {i+1} is: ", rddtxtline[i])


print("\n4. Wordcount for each token in the file: ")
tokens = rddtxt.map(lambda x: x.replace(".", "")).flatMap(lambda x: x.split(" "))\
    .map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
print(tokens.collect())
print("And the result is saved under wordcount in Outputs directory")
wordcount_output = outputpath.joinpath("wordcount")
if Path.exists(wordcount_output):
    pass
else:
    tokens.saveAsTextFile(str(wordcount_output))

print("\n5. map VS flatmap: ")
inputFile = baseDir.joinpath("Inputs", "Data.txt")
data = sc.textFile(str(inputFile))
mp = data.map(lambda line: line.split(" "))
print("MAP function result: ", mp.collect())
fmp = data.flatMap(lambda line: line.split(" "))
print("FLATMAP function result: ", fmp.collect())

print("\n6. Data in each partition: ")


def show(index, iterator):
    yield 'index: '+str(index)+" values: "+str(list(iterator))


print(rddtxt.mapPartitionsWithIndex(show).collect())


def filter_partition(x):
    def filter_partition_(i, iter):
        return iter if i == x else []
    return filter_partition_


print("And the result is saved under partitions in Outputs directory")
for i in range(numPartitions):
    tmp = rddtxt.mapPartitionsWithIndex(filter_partition(i)).coalesce(1)
    opPath = outputpath.joinpath("partitions")
    if Path.exists(opPath.joinpath("partition_0") and opPath.joinpath("partition_1") and opPath.joinpath("partition_2")):
        pass
    else:
        dirname = opPath.joinpath('partition_{0}'.format(i))
        tmp.saveAsTextFile(str(dirname))

    
























