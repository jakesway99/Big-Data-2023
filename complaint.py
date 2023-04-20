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

# CMPLNT_FR_DT is Exact date of occurrence for the reported event (or starting date of occurrence, if CMPLNT_TO_DT exists)
# CMPLNT_TO_DT is Ending date of occurrence for the reported event, if exact time of occurrence is unknown
# create a Crime_Date column(pick CMPLNT_FR_DT if it exists, otherwise pick CMPLNT_TO_DT)
complaint_df2 = complaint_df.withColumn('Crime_Date', F.when(complaint_df['CMPLNT_FR_DT']=='null', 'CMPLNT_TO_DT').otherwise(complaint_df['CMPLNT_FR_DT']))
complaint_df2.show(5)

# interested in complaints happened after 2015-01-01, excludes rows that have date before 2015-01-01
complaint_after_2015_df = complaint_df2.withColumn('Crime_Date', F.to_date('Crime_Date', 'M/d/y')).filter((F.col('Crime_Date') >= F.lit('2015-01-01')))

# combine year and month as Crime_Year_Month
complaint_after_2015_df = complaint_after_2015_df.withColumn('Crime_Year_Month', F.concat(F.year('Crime_Date'),F.lit('-'), F.month('Crime_Date')))
complaint_after_2015_df.show(5)

complaint_after_2015_df.createOrReplaceTempView("complaint_after_2015")

# how many complaints occurred each month after 2015-01-01?
complaint_month = complaint_after_2015_df.groupBy(F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'))
complaint_month = complaint_month.select('Crime_Year_Month', 'count')
complaint_month.show()

# plot the complaint count by months
pd_df = complaint_month.toPandas()
pd_df.plot('Crime_Year_Month', 'count', legend=False)
#plt.xticks(range(len(pd_df['Crime_Year_Month'])), pd_df['Crime_Year_Month'], size='small', rotation=45)
plt.show()

# want to analyze complaints based on offense level
# column LAW_CAT_CD includes the Level of offense: felony, misdemeanor, violation
# examine the null values in offense level column
print("Null values in LAW_CAT_CD column: "+str(complaint_after_2015_df.filter(complaint_after_2015_df['LAW_CAT_CD'].isNull()).count())) # 0
complaint_offense_level = complaint_after_2015_df.groupBy(F.year('Crime_Date'), F.month('Crime_Date'),'Crime_Year_Month','LAW_CAT_CD').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'), 'LAW_CAT_CD')
complaint_offense_level = complaint_offense_level.select('Crime_Year_Month','LAW_CAT_CD', 'count')
complaint_offense_level.show()

# plot the complaint count and offense level by months
pd_df = complaint_offense_level.toPandas()
pd_df = pd_df.pivot(index='Crime_Year_Month', columns='LAW_CAT_CD', values='count')
pd_df.plot()
plt.show()


# want to analyze complaints based on offense types
# examine the null values in offense description column, and their correlated key codes
spark.sql("SELECT DISTINCT(KY_CD, OFNS_DESC) FROM complaint_after_2015 WHERE OFNS_DESC is null ").show()

# want to exclude the rows that have null in OFNS_DESC column
complaint_offense_type = complaint_after_2015_df.filter(complaint_after_2015_df['OFNS_DESC'].isNotNull())
complaint_offense_type = complaint_after_2015_df.filter(complaint_after_2015_df['OFNS_DESC']!="(null)")

offense_type = complaint_offense_type.select('OFNS_DESC').distinct().collect()
print("Offense types: \n")
for row in offense_type:
    print(row.OFNS_DESC)



# how many complaints based on offense types occurred each month after 2015-01-01?
complaint_offense_type = complaint_offense_type.groupBy(F.year('Crime_Date'), F.month('Crime_Date'),'Crime_Year_Month','OFNS_DESC').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'), 'OFNS_DESC')
complaint_offense_type = complaint_offense_type.select('Crime_Year_Month','OFNS_DESC','count')
complaint_offense_type.show()

complaint_offense_type.createOrReplaceTempView("complaint_offense_type")
# how many homicide occurred?
homicide = spark.sql("SELECT Crime_Year_Month, OFNS_DESC, count FROM complaint_offense_type WHERE OFNS_DESC = 'MURDER & NON-NEGL. MANSLAUGHTER'")
homicide.show()

pd_df = homicide.toPandas()
pd_df.plot('Crime_Year_Month', 'count', legend=False)
plt.show()

# how many assaults occured?
assaults = spark.sql("SELECT Crime_Year_Month, OFNS_DESC, count FROM complaint_offense_type WHERE OFNS_DESC = 'ASSAULT 3 & RELATED OFFENSES' or OFNS_DESC = 'FELONY ASSAULT'")
assaults.show()

pd_df = assaults.toPandas()
pd_df = pd_df.pivot(index='Crime_Year_Month', columns='OFNS_DESC', values='count')
pd_df.plot()
plt.show()

# how many robbery, burglary, and larceny occurred?
rbl = spark.sql("SELECT Crime_Year_Month, OFNS_DESC, count FROM complaint_offense_type WHERE OFNS_DESC = 'ROBBERY' or OFNS_DESC = 'BURGLARY' or OFNS_DESC = 'PETIT LARCENY' or OFNS_DESC = 'GRAND LARCENY'")
rbl.show()

pd_df = rbl.toPandas()
pd_df = pd_df.pivot(index='Crime_Year_Month', columns='OFNS_DESC', values='count')
pd_df.plot()
plt.show()

# how many motor vehicle theft occurred?
mvt = spark.sql("SELECT Crime_Year_Month, OFNS_DESC, count FROM complaint_offense_type WHERE OFNS_DESC = 'PETIT LARCENY OF MOTOR VEHICLE' or OFNS_DESC = 'GRAND LARCENY OF MOTOR VEHICLE'")
mvt.show()

pd_df = mvt.toPandas()
pd_df = pd_df.pivot(index='Crime_Year_Month', columns='OFNS_DESC', values='count')
pd_df.plot()
plt.show()


# how many drug offense occurred?
do = spark.sql("SELECT Crime_Year_Month, OFNS_DESC, count FROM complaint_offense_type WHERE OFNS_DESC = 'DANGEROUS DRUGS' or OFNS_DESC = 'UNDER THE INFLUENCE OF DRUGS' or OFNS_DESC = 'CANNABIS RELATED OFFENSES'")
do.show()

pd_df = do.toPandas()
pd_df = pd_df.pivot(index='Crime_Year_Month', columns='OFNS_DESC', values='count')
pd_df.plot()
plt.show()

# want to analyze victim race
# examine the values in victim race column
complaint_after_2015_df.select("VIC_RACE").distinct().show()

# there are null and UNKNOWN values in the victim race column
# want to change the null values and '(null)' values to UNKNOWN in victim race column
complaint_clean_race = complaint_after_2015_df.withColumn('VIC_RACE', F.when(complaint_after_2015_df['VIC_RACE'].isNull(), 'UNKNOWN').otherwise(complaint_after_2015_df['VIC_RACE']))
complaint_clean_race = complaint_clean_race.withColumn('VIC_RACE', F.when(complaint_clean_race['VIC_RACE']=='(null)', 'UNKNOWN').otherwise(complaint_clean_race['VIC_RACE']))
complaint_clean_race.select("VIC_RACE").distinct().show()

# victim races VS. complaint count each month after 2015-01-01
complaint_race = complaint_clean_race.groupBy(F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month', 'VIC_RACE').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'),  'VIC_RACE')
complaint_race = complaint_race.select('Crime_Year_Month', 'VIC_RACE', 'count')
complaint_race.show()

# plot the victim races VS. complaint count
pd_df = complaint_race.toPandas()
pd_df = pd_df.pivot(index='Crime_Year_Month', columns='VIC_RACE', values='count')
pd_df.plot()
plt.show()

# interested in asian victims, exclude the rows that are not asian victims
complaint_asian = complaint_clean_race.filter(complaint_clean_race['VIC_RACE']=="ASIAN / PACIFIC ISLANDER")
complaint_asian = complaint_asian.groupBy(F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month', 'VIC_RACE').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'),  'VIC_RACE')
complaint_asian = complaint_asian.select('Crime_Year_Month', 'count')

# plot the Asian victim complaint count by months
pd_df = complaint_asian.toPandas()
pd_df.plot('Crime_Year_Month', 'count', legend=False)
#plt.xticks(range(len(pd_df['Crime_Year_Month'])), pd_df['Crime_Year_Month'], size='small', rotation=45)
plt.show()

# complaint cases in precincts
# 41 rows have null in precinct column, exclude them
print("Null values in precinct column: " + str(complaint_after_2015_df.filter(complaint_after_2015_df['ADDR_PCT_CD'].isNull()).count()))

complaint_precinct = complaint_after_2015_df.filter(complaint_after_2015_df['ADDR_PCT_CD'].isNotNull())

complaint_precinct = complaint_precinct.groupBy('ADDR_PCT_CD',F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month').count().orderBy('ADDR_PCT_CD', F.year('Crime_Date'), F.month('Crime_Date'))
complaint_precinct = complaint_precinct.select('ADDR_PCT_CD', 'Crime_Year_Month')
complaint_precinct.show()
