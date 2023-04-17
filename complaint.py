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
    .appName("NYC Complaint Analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

complaint_historic_df = spark.read.csv(path='NYPD_Complaint_Data_Historic.csv',header=True)
complaint_current_df = spark.read.csv(path='NYPD_Complaint_Data_Current__Year_To_Date_.csv',header=True)


#print(complaint_historic_df.count()) # 7825499
#print(complaint_current_df.count()) # 531768

#complaint_historic_df.printSchema()
#complaint_current_df.printSchema()

complaint_historic_df.createOrReplaceTempView("complaint_historic")
complaint_current_df.createOrReplaceTempView("complaint_current")

complaint_df = spark.sql("SELECT CMPLNT_NUM, CMPLNT_FR_DT, CMPLNT_TO_DT, ADDR_PCT_CD, "
                         "KY_CD, OFNS_DESC, BORO_NM, SUSP_AGE_GROUP, SUSP_RACE, SUSP_SEX, "
                         "VIC_AGE_GROUP, VIC_RACE, VIC_SEX, Latitude, Longitude "
                         "FROM complaint_historic "
                         "UNION ALL "
                         "SELECT CMPLNT_NUM, CMPLNT_FR_DT, CMPLNT_TO_DT, ADDR_PCT_CD, "
                         "KY_CD, OFNS_DESC, BORO_NM, SUSP_AGE_GROUP, SUSP_RACE, SUSP_SEX, "
                         "VIC_AGE_GROUP, VIC_RACE, VIC_SEX, Latitude, Longitude "
                         "FROM complaint_current")

# the unioned dataset has 8357267 rows
#print(complaint_df.count())

# CMPLNT_FR_DT is Exact date of occurrence for the reported event (or starting date of occurrence, if CMPLNT_TO_DT exists)
# CMPLNT_TO_DT is Ending date of occurrence for the reported event, if exact time of occurrence is unknown
# create a Crime_Date column(pick CMPLNT_FR_DT if it exists, otherwise pick CMPLNT_TO_DT)
complaint_df2 = complaint_df.withColumn('Crime_Date', F.when(complaint_df['CMPLNT_FR_DT']=='null', 'CMPLNT_TO_DT').otherwise(complaint_df['CMPLNT_FR_DT']))
complaint_df2.show()

# interested in complaints happened after 2015-01-01, excludes rows that have date before 2015-01-01
complaint_after_2015_df = complaint_df2.withColumn('Crime_Date', F.to_date('Crime_Date', 'M/d/y')).filter((F.col('Crime_Date') >= F.lit('2015-01-01')))

# combine year and month as Crime_Year_Month
complaint_after_2015_df = complaint_after_2015_df.withColumn('Crime_Year_Month', F.concat(F.year('Crime_Date'),F.lit('-'), F.month('Crime_Date')))
complaint_after_2015_df.show()

complaint_after_2015_df.createOrReplaceTempView("complaint_after_2015")

# how many complaints occurred each month after 2015-01-01?
complaint_after_2015_df.groupBy(F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date')).show()

# plot the complaint count by months
pd_df = complaint_after_2015_df.groupBy(F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date')).toPandas()
pd_df.plot('Crime_Year_Month', 'count', legend=False)
#plt.xticks(range(len(pd_df['Crime_Year_Month'])), pd_df['Crime_Year_Month'], size='small', rotation=45)
plt.show()

# want to analyze complaints based on offense types
# examine the null values in offense description column, and their correlated key codes
spark.sql("SELECT DISTINCT(KY_CD, OFNS_DESC) FROM complaint_after_2015 WHERE OFNS_DESC is null ").show()

# want to exclude the rows that have null in OFNS_DESC column
complaint_after_2015_df2 = complaint_after_2015_df.filter(complaint_after_2015_df['OFNS_DESC'].isNotNull())

# how many complaints based on offense types occurred each month after 2015-01-01?
complaint_after_2015_df2.groupBy(F.year('Crime_Date'), F.month('Crime_Date'),'Crime_Year_Month','OFNS_DESC').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'), 'OFNS_DESC').show()


# want to analyze victim race
# examine the values in victim race column
complaint_after_2015_df.select("VIC_RACE").distinct().show()

# there are null and UNKNOWN values in the victim race column
# want to change the null values and '(null)' values to UNKNOWN in victim race column
complaint_after_2015_df3 = complaint_after_2015_df.withColumn('VIC_RACE', F.when(complaint_after_2015_df['VIC_RACE'].isNull(), 'UNKNOWN').otherwise(complaint_after_2015_df['VIC_RACE']))
complaint_after_2015_df3 = complaint_after_2015_df3.withColumn('VIC_RACE', F.when(complaint_after_2015_df3['VIC_RACE']=='(null)', 'UNKNOWN').otherwise(complaint_after_2015_df3['VIC_RACE']))
complaint_after_2015_df3.select("VIC_RACE").distinct().show()

# victim races VS. complaint count each month after 2015-01-01
complaint_after_2015_df3.groupBy(F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month', 'VIC_RACE').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'),  'VIC_RACE').show()

# plot the victim races VS. complaint count
pd_df = complaint_after_2015_df3.groupBy(F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month', 'VIC_RACE').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'),  'VIC_RACE').toPandas()
pd_df = pd_df.pivot(index='Crime_Year_Month', columns='VIC_RACE', values='count')
pd_df.plot()
plt.show()

# interested in asian victims, exclude the rows that are not asian victims
complaint_after_2015_asian = complaint_after_2015_df3.filter(complaint_after_2015_df3['VIC_RACE']=="ASIAN / PACIFIC ISLANDER")

complaint_after_2015_asian.groupBy(F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month', 'VIC_RACE').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'),  'VIC_RACE').show()

# plot the Asian victim complaint count by months
pd_df = complaint_after_2015_asian.groupBy(F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month', 'VIC_RACE').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'),  'VIC_RACE').toPandas()
pd_df.plot('Crime_Year_Month', 'count', legend=False)
#plt.xticks(range(len(pd_df['Crime_Year_Month'])), pd_df['Crime_Year_Month'], size='small', rotation=45)
plt.show()

# complaint cases in precincts
# 41 rows have null in precinct column, exclude them
print(complaint_after_2015_df.filter(complaint_after_2015_df['ADDR_PCT_CD'].isNull()).count())
complaint_after_2015_df4 = complaint_after_2015_df.filter(complaint_after_2015_df['ADDR_PCT_CD'].isNotNull())

complaint_after_2015_df4.groupBy('ADDR_PCT_CD',F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month').count().orderBy('ADDR_PCT_CD', F.year('Crime_Date'), F.month('Crime_Date')).show()

