from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import pandas as pd
import matplotlib.pyplot as plt


cf = SparkConf()
cf.set("spark.submit.deployMode", "client")
sc = SparkContext.getOrCreate(cf)

spark = SparkSession \
    .builder \
    .appName("NYC Complaint Clean and Integrate") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

complaint_historic_df = spark.read.csv(path='NYPD_Complaint_Data_Historic.csv',header=True)
complaint_current_df = spark.read.csv(path='NYPD_Complaint_Data_Current__Year_To_Date_.csv',header=True)


print("The historic dataset has "+str(complaint_historic_df.count())+" rows") # 7825499
print("The current (Year to Date) dataset has "+str(complaint_current_df.count())+" rows") # 531768

#complaint_historic_df.printSchema()
#complaint_current_df.printSchema()

complaint_historic_df.createOrReplaceTempView("complaint_historic")
complaint_current_df.createOrReplaceTempView("complaint_current")

complaint_df = spark.sql("SELECT CMPLNT_NUM, CMPLNT_FR_DT, CMPLNT_TO_DT, ADDR_PCT_CD, "
                         "KY_CD, OFNS_DESC, LAW_CAT_CD, BORO_NM, SUSP_AGE_GROUP, SUSP_RACE, SUSP_SEX, "
                         "VIC_AGE_GROUP, VIC_RACE, VIC_SEX, Latitude, Longitude "
                         "FROM complaint_historic "
                         "UNION ALL "
                         "SELECT CMPLNT_NUM, CMPLNT_FR_DT, CMPLNT_TO_DT, ADDR_PCT_CD, "
                         "KY_CD, OFNS_DESC, LAW_CAT_CD, BORO_NM, SUSP_AGE_GROUP, SUSP_RACE, SUSP_SEX, "
                         "VIC_AGE_GROUP, VIC_RACE, VIC_SEX, Latitude, Longitude "
                         "FROM complaint_current")


print("The unioned dataset has "+str(complaint_df.count())+" rows") # 8357267

# check the uniqueness constraints
complaint_df.groupBy('CMPLNT_NUM').count().orderBy('count', ascending=False).show()
# exclude duplicate id rows
complaint_df = complaint_df.dropDuplicates(["CMPLNT_NUM"])

# check again
complaint_df.groupBy('CMPLNT_NUM').count().orderBy('count', ascending=False).show()

print("The unioned dataset after uniqueness constraints has "+str(complaint_df.count())+" rows") # 8348469

# CMPLNT_FR_DT is Exact date of occurrence for the reported event (or starting date of occurrence, if CMPLNT_TO_DT exists)
# CMPLNT_TO_DT is Ending date of occurrence for the reported event, if exact time of occurrence is unknown
# create a Crime_Date column(pick CMPLNT_FR_DT if it exists, otherwise pick CMPLNT_TO_DT)
complaint_df2 = complaint_df.withColumn('Crime_Date', F.when(complaint_df['CMPLNT_FR_DT']=='null', 'CMPLNT_TO_DT').otherwise(complaint_df['CMPLNT_FR_DT']))
complaint_df2.show(5)
print("Null values in Crime_Date column: "+str(complaint_df2.filter(complaint_df2['Crime_Date'].isNull()).count())) # 655

# exclude null dates
complaint_df2 = complaint_df2.filter(complaint_df2['Crime_Date'].isNotNull())

# examine the crime date
complaint_df2 = complaint_df2.withColumn('Crime_Date', F.to_date('Crime_Date', 'M/d/y'))
complaint_df2.select('Crime_Date').orderBy('Crime_Date').show()
complaint_df2.select('Crime_Date').orderBy('Crime_Date', ascending=False).show()

# found outlier & interested in complaints happened after 2015-01-01, excludes rows that have date before 2015-01-01
complaint_after_2015_df = complaint_df2.filter((F.col('Crime_Date') >= F.lit('2015-01-01')))


# combine year and month as Crime_Year_Month
complaint_after_2015_df = complaint_after_2015_df.withColumn('Crime_Year_Month', F.concat(F.year('Crime_Date'),F.lit('-'), F.month('Crime_Date')))
complaint_after_2015_df.show(5)

# change column names for geomapping
complaint_after_2015_df = complaint_after_2015_df.withColumnRenamed('Longitude', 'longitude')
complaint_after_2015_df = complaint_after_2015_df.withColumnRenamed('Latitude', 'latitude')


# write the pyspark dataframe as a single csv file
complaint_after_2015_df.coalesce(1).write.mode("overwrite").option("header",True).csv('NYPD_Complaint_After_2015.csv')

