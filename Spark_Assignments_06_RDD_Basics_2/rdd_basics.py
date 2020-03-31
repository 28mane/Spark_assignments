from pyspark import SparkContext, SQLContext
from pathlib import Path
sc = SparkContext()
sqlc = SQLContext(sc)

baseDir = Path.cwd()
inputFile = baseDir.joinpath("Inputs", "sales.txt")
outputPath = baseDir.joinpath("Outputs")

rddSales = sc.textFile(str(inputFile))

print("***** ASSIGNMENT – 6 : Creation of  RDD and  operations on RDDs *****")
print("\n1. Considering the given 'sales' text file: ")
print("i) RDD form is: <" + str(rddSales) + "> and its type is:", type(rddSales))
print(rddSales.collect())

print("ii) Data sorted by priority is as: ")
rddSort = rddSales.sortBy(lambda p: p[0])
print(rddSort.collect())

print("\n2. Considering the given 'sales' text file: ")
print("i) Placing Low and high priority data into 2 separate files: ")
rddGroup = rddSales.groupBy(lambda p: p[0])
for data in rddGroup.collect():
    if data[0] == 'L' and not (Path.exists(outputPath.joinpath("Low_Priority"))):
        daL = sc.parallelize(data[1])
        lowres = outputPath.joinpath("Low_Priority")
        daL.saveAsTextFile(str(lowres))
    elif data[0] == 'H' and not (Path.exists(outputPath.joinpath("High_Priority"))):
        daH = sc.parallelize(data[1])
        highres = outputPath.joinpath("High_Priority")
        daH.saveAsTextFile(str(highres))
    else:
        pass
print("Low priority data stores at location  :", outputPath.joinpath("Low_Priority"))
print("High priority data stores at location :", outputPath.joinpath("High_Priority"))

print("ii) Placing records with less than 20 as qty into another file: ")
#for rows in rddSales.collect():
#    row = rows.split(',')
#    if row[1].isdigit() and int(row[1]) < 20:
#        print(rows)
#    if row[1].is_integer() and row[1] < 20:
#        print(row)
rddQty = rddSales.map(lambda rows: rows.split(',')).\
    filter(lambda row: row if row[1].isdigit() and int(row[1]) < 20 else False)
print("Data:", rddQty.collect())
Qty_LT_20 = outputPath.joinpath("Qty_LT_20")
if Path.exists(Qty_LT_20):
    pass
else:
    rddQty.saveAsTextFile(str(Qty_LT_20))
print("Stored at location: ", Qty_LT_20)

print("\n3. Considering the given 'sales' text file: ")
print("i) Creating RDDs for priority & sales, qty & sales: ")
rdd_priority = rddSales.map(lambda rows: rows.split(',')).map(lambda row: row[0])
rdd_qty = rddSales.map(lambda rows: rows.split(',')).map(lambda row: row[1])
rdd_sales = rddSales.map(lambda rows: rows.split(',')).map(lambda row: row[2])
rdd_p_s = rdd_priority.zip(rdd_sales)
print("RDD for priority & sales is: ", rdd_p_s)
print(rdd_p_s.collect())
rdd_q_s = rdd_qty.zip(rdd_sales)
print("RDD for qty & sales is: ", rdd_q_s)
print(rdd_q_s.collect())

print("\n4. Considering the given 'sales' text file: ")
print("ii) Count of records in low and high priorities: ")
rddLow = rddSales.map(lambda rows: rows.split(',')).filter(lambda row: row if row[0] == 'Low' else False)
print("Record count of Low priority: ", rddLow.count())
rddHigh = rddSales.map(lambda rows: rows.split(',')).filter(lambda row: row if row[0] == 'High' else False)
print("Record count of High priority: ", rddHigh.count())

print("\n5. Considering the given 'sales' text file: ")
print("i) Loading data into an RDD with 4 partitions:")
rddSalesWithPartitions = sc.textFile(str(inputFile), 4)
print("RDD <" + str(rddSalesWithPartitions) + "> created with",
      str(rddSalesWithPartitions.getNumPartitions()), "partitions.")

print("ii) Displaying data along with partitions:")
for par in rddSalesWithPartitions.glom().collect():
    print(rddSalesWithPartitions.glom(), "->", par)

print("\n6. Considering the given 'sales' text file: ")
print("i) Re-partitioning data into 6 partitions:")
rddSalesRePartitions = rddSalesWithPartitions.repartition(6)
print("RDD <" + str(rddSalesRePartitions) + "> created with",
      str(rddSalesRePartitions.getNumPartitions()), "re-partitions.")

print("ii) Displaying data from all partitions and loading into another text file")
i = 0
pardataoutput = outputPath.joinpath("Six_Data_Partitions")
for par in rddSalesRePartitions.glom().collect():
    if Path.exists(pardataoutput.joinpath("partition_0") and pardataoutput.joinpath("partition_1")
                   and pardataoutput.joinpath("partition_2") and pardataoutput.joinpath("partition_3")
                   and pardataoutput.joinpath("partition_4") and pardataoutput.joinpath("partition_5")):
        pass
    else:
        dirname = pardataoutput.joinpath('partition_{0}'.format(i))
        sc.parallelize(par).saveAsTextFile(str(dirname))
    print("Partition_" + str(i), ": ", par)
    i += 1
print("Re-partitioned data stored at location: ", pardataoutput)



