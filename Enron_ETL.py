# Databricks notebook source
#In order to work with the NLTK library do the following:
# 1. In the Workspace create a library by select a folder, then click on the dropdown->Create->Library
# 2. In the New Library Screen select Python as language
# 3. As Pypi name specify "nltk" as the Pypi name

import nltk
import config.py

# COMMAND ----------

raw_file = sc.textFile('/FileStore/tables/enron/enron_mysqldump-c4838.sql')

# COMMAND ----------

raw_file.take(10)

# COMMAND ----------

#EL = Employee List
el = raw_file.filter(lambda line: "INSERT INTO employeelist VALUES" in line).map(lambda line: line.split(" ")[4][1:-2].replace("'","").split(",")).map(lambda array: (int(array[0]),array[1],array[2],array[3]))
eldf = spark.createDataFrame(el,["id","first","last","email"])
eldf.describe().show()

# COMMAND ----------

eldf.printSchema()

# COMMAND ----------

# msg = Message Table
def cleanString(arrayString):
  result = []
  for s in arrayString:
    if (not s.isdigit()):
      result.append(s[:-1])
    else:
      result.append(s)
  return result

msg = raw_file.filter(lambda line: "INSERT INTO message VALUES" in line).map(lambda line: line[28:-2].split(",'")).map(cleanString).map(lambda e: (int(e[0]),e[1],e[2],e[3],e[4],e[5],e[6]))
msgdf=spark.createDataFrame(msg,["mid","sender","timestamp","omid","subject","body","folder"])
msgdf.describe().show()

# COMMAND ----------

msgdf.show()

# COMMAND ----------

#ri = Recipient Info Table
ri = raw_file.filter(lambda line: "INSERT INTO recipientinfo VALUES" in line).map(lambda line: line.split(" ")[4][1:-2].replace("'","").split(",")).map(lambda array: (int(array[0]),int(array[1]),array[2],array[3]))

ridf = spark.createDataFrame(ri,["rid","mid","rtype","remail"])
ridf.show()

# COMMAND ----------

ridf.take(25)
