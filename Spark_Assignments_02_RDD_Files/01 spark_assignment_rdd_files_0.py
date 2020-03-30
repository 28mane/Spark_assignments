'''
desc
Consider the following associative array :
{  (Ram,10), (sam,30),(joe,40), (sai,54), (Ram,34),(sam,99),(joe,50),(tej,34),(Tej,39),(sai,120),(tej,45) }

1.	Consider above data and answer the following

a)	Provide the output in the form of keypairs using RDD.
b)	Provide aggregation of each name
c)	Provide the count of occurrence each name

'''
#from pyspark import SparkConf
from pyspark import SparkContext
#sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
sc = SparkContext()


rdd1 = sc.parallelize([("Ram", 10), ("sam", 30), ("joe", 40), ("sai", 54), ("Ram", 34),
                       ("sam", 99), ("joe", 50), ("tej", 34), ("Tej", 39), ("sai", 120), ("tej", 45)])

print("***** ASSIGNMENT – 2 : Working with associative arrays *****")
print("1. Considering above data:")
print("a) Output in the form of keypairs using RDD is: ", rdd1.collect())
rdd2 = rdd1.reduceByKey(lambda x, y: x + y)
print("b) Aggregation of each name is: ", rdd2.collect())
rdd3 = rdd1.countByKey()
print("c) Count of occurrence each name is: ", dict(rdd3))


