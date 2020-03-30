'''
desc
Consider the following array.
{10,21,90,34,40,98,21,44,59,21,90,34,29,19, 21,90,34,29,49,78 } . Create a new RDD for each of the following assignments.

1.	For the above array,
a)	Create a RDD for the above array.
b)	Display the array
c)	Display the first element of the array

2.	Consider the above array
a)	Display the sorted output (ascending and descending) through an RDD.
b)	Display the distinct elements of the array using an RDD
c)	Display distinct elements without using a new RDD.

3.	Consider the above array
a)	Display maximum and minimum of given array using RDD.
b)	Display top 5 list elements using RDD
c)	Combine above array with a new array { 30,35,45,60,75,85} and display output.
d)	Provide the sum of the array elements using reduce with distinct values.
e)	Provide the sum of the array elements using reduce.
'''

from pyspark import SparkContext
sc = SparkContext()

arrayRDD1 = sc.parallelize([10, 21, 90, 34, 40, 98, 21, 44, 59, 21,
                           90, 34, 29, 19, 21, 90, 34, 29, 49, 78])

arrayRDD2 = sc.parallelize([30, 35, 45, 60, 75, 85])

print("***** ASSIGNMENT – 1 : Creation of  RDD and  operations on RDDs *****")
print("1. For the given array:")
print("a) RDD form is: ", arrayRDD1, "and its type is: ", type(arrayRDD1))
print("b) Display: ", arrayRDD1.collect())
print("c) First element is: ", arrayRDD1.first())


print("2. Considering the given array:")
print("a) Sorted output, \n"
      "Ascending is: ",  arrayRDD1.sortBy(lambda x: x, ascending=True).collect(), '\n'
      "Descending is: ", arrayRDD1.sortBy(lambda x: x, ascending=False).collect())
distinctArrayRDD = arrayRDD1.distinct()
print("b) Distinct elements using new RDD are: ", distinctArrayRDD.collect())
print("c) Distinct elements without using new RDD are: ", arrayRDD1.distinct().collect())


print("3. Considering the given array:")
print("a) Maximum is: ", arrayRDD1.max(), "\n"
      "   Minimum is: ", arrayRDD1.min())
print("b) Top 5 list elements are: ", arrayRDD1.top(5))
print("c) Combining it with new array results: ", arrayRDD2.union(arrayRDD1).collect())
print("d) Sum of elements using reduce is: ", arrayRDD1.reduce(lambda res, x: res + x))
print("e) Sum of distinct elements using reduce is: ", arrayRDD1.distinct().reduce(lambda res, x: res + x))

