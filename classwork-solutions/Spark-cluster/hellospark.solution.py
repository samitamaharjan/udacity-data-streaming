from pyspark.sql import SparkSession

# create a variable with the absolute path to the text file
logFile = "/home/workspace/Test.txt"  # Should be some file on your system

# create a Spark session
spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

# set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file
logData = spark.read.text(logFile).cache()

# call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found
numDs = logData.filter(logData.value.contains('d')).count()

# call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found
numSs = logData.filter(logData.value.contains('s')).count()

print("*******")
print("*******")
print("*****Lines with d: %i, lines with s: %i" % (numDs, numSs))
print("*******")
print("*******")

# stop the spark application
spark.stop()