from pyspark import SparkContext, SQLContext
from pathlib import Path
sc = SparkContext()
sqlc = SQLContext(sc)

baseDir = Path.cwd()
inputFile = baseDir.joinpath("Spark_Assignments_06_RDD_Basics_2", "Inputs", "sales.txt")
outputPath = baseDir.joinpath("Outputs")


rddSalesWithPartitions = sc.textFile(str(inputFile), 4)
print("RDD <" + str(rddSalesWithPartitions) + "> created with", str(rddSalesWithPartitions.getNumPartitions()), "partitions")

print("ii) ")
for par in rddSalesWithPartitions.glom().collect():
    print(rddSalesWithPartitions.glom(), "->", par)

rddSalesWithPartitions.car