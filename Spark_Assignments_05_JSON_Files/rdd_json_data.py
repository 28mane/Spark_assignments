'''
desc
Consider the following json file
{"id": 1,  "fname": "Jeanette",  "lname": "Penddreth",   "gender": "Female"  }
{"id": 2,  "fname": "Giavani",  "lname": "Frediani",   "gender": "Male"}
{"id": 3,  "fname": "Noell",  "lname": "Bea",  "gender": "Female"}
{"id": 4,  "fname": "Willard",  "lname": "Valek",   "gender": "Male"}

1. Create RDD for above json file.
2. display the contents in tabular format.
3. Apply filter on id , gender to restrict output.
4. display contents in array format.
5. classify whole data into 2 files based on gender and save them as json files.
6. differentiate between show and collect when used with data frames.
7. sort the given data based out of gender and store in a file.
8. convert given json file into a text file.
'''

from pyspark import SparkContext, SQLContext
from pathlib import Path
sc = SparkContext()
sqlc = SQLContext(sc)

baseDir = Path.cwd()
inputFile = baseDir.joinpath("Inputs", "emp.json")
outputPath = baseDir.joinpath("Outputs")

dfs = sqlc.read.json(str(inputFile))
rdd = sc.textFile(str(inputFile))

print("***** ASSIGNMENT – 5 : Spark JSON Files *****")

print("\n1. RDD for given json file is: ", rdd.collect())

print("\n2. The contents in tabular format are as below: ")
dfs.show()

print("\n3. To filter on id , gender to restrict output: ")
dfs.filter((dfs.id <= 4) & (dfs.gender == 'Male')).show()

print("\n4. The contents in array format are as below: ")
print(rdd.collect())

print("\n5. classifying data into 2 JSON files based on gender: ")


def writetojson(gender, outputPath):
    if gender == 'Female':
        dfsFemale = dfs.filter((dfs.gender == 'Female'))
        outputPath = outputPath.joinpath("json_files", "dfs_female_json")
        if Path.exists(outputPath):
            pass
        else:
            dfsFemale.write.format('json').save(str(outputPath))
            print("Female data:")
            dfsFemale.show()
            print("written at location: ", outputPath)
    elif gender == 'Male':
        dfsMale = dfs.filter((dfs.gender == 'Male'))
        outputPath = outputPath.joinpath("json_files", "dfs_male_json")
        if Path.exists(outputPath):
            pass
        else:
            dfsMale.write.format('json').save(str(outputPath))
            print("\nMale data:")
            dfsMale.show()
            print("written at location: ", outputPath)
    else:
        pass


writetojson('Female', outputPath)

writetojson('Male', outputPath)

print("\n6. show vs collect: ")
print("Data as RDD via collect: ", rdd.collect())
print("Data as Dataframe via show: ")
dfs.show()

print("\n7. Data sorted based on gender: ")
sorted_on_gender = dfs.sort(dfs.gender)
sorted_on_gender.show()
sorted_on_gender_path = outputPath.joinpath("sorted_on_gender")
if Path.exists(sorted_on_gender_path):
    pass
else:
    sorted_on_gender.write.format('json').save(str(sorted_on_gender_path))
print("written at location: ", sorted_on_gender_path)

print("\nConverting given json file into a text file:")
outputPath = outputPath.joinpath("text_files")
if Path.exists(outputPath):
    pass
else:
    dfs.rdd.saveAsTextFile(str(outputPath))
print("The text file is stored at location: ", outputPath)
