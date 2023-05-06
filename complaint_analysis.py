from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import matplotlib.pyplot as plt

def showSingleLineGraph(df, title, a, b):
    pd_df = df.toPandas()

    pd_df['Crime_Year_Month'] = pd.to_datetime(pd_df['Crime_Year_Month'], format='%Y-%m')
    # get the growth, compare to the same month of previous year
    pd_df['growth'] = (pd_df['count'] - pd_df['count'].shift(12)) / pd_df['count'].shift(12) * 100
    print(pd_df.to_string())

    # using moving average method, window size = 12 months
    rolling_mean = pd_df['count'].rolling(12).mean()
    plt.figure(figsize=(20, 10))

    # plot the original crime count and the moving average line
    plt.plot(pd_df['Crime_Year_Month'], pd_df['count'], marker='o', label='total crimes every month', color='#2a5674')
    plt.plot(pd_df['Crime_Year_Month'], rolling_mean, label='moving average line (12 months)', color='#4f90a6',
             linestyle='--', linewidth=3)

    # draw a background for covid-19 period
    plt.axvspan(pd.to_datetime('2020-03-01'), pd.to_datetime('2022-12-01'), color='gray', alpha=0.1, hatch='//')

    # add annotation
    for i in range(1, len(pd_df)):
        if i >= 12:  # previous year data not available for the first year, so skipping the first year
            percent_change = pd_df['growth'][i]
            print(percent_change)

            # set the vertical displacement
            ymin, ymax = plt.ylim()
            offset = (ymax - ymin) * 0.025

            text_x = pd_df['Crime_Year_Month'][i]
            text_y = pd_df['count'][i] + offset
            text = "{:.1f}%".format(percent_change)
            if abs(percent_change) >= a and abs(percent_change) < b:
                plt.text(text_x, text_y, text, ha='center', va='bottom', fontsize=10,
                         bbox=dict(facecolor='white', edgecolor='none', alpha=0.7, boxstyle='round'), zorder=3)
            if percent_change >= b:
                plt.text(text_x, text_y, text, ha='center', va='bottom', fontsize=10,
                         bbox=dict(facecolor='white', edgecolor='red', alpha=0.7, boxstyle='round'), zorder=4)
            if percent_change <= 0-b:
                plt.text(text_x, text_y, text, ha='center', va='bottom', fontsize=10,
                         bbox=dict(facecolor='white', edgecolor='green', alpha=0.7, boxstyle='round'), zorder=4)

    plt.legend()


    #plt.figtext(0.95, 0.05, '* Annotations: the percentage changes of crime count\ncompared with the same month in previous year', ha='right', va='bottom', fontsize=8)

    # xticks = [year_month for year_month in pd_df['Crime_Year_Month'] if year_month.endswith('-1') or year_month.endswith('-4') or year_month.endswith('-7') or year_month.endswith('-10')]
    xticks_label = [year_month.strftime("%Y-%m") for year_month in pd_df['Crime_Year_Month'] if
                 year_month.month == 1 or year_month.month == 3 or year_month.month == 5 or year_month.month == 7 or year_month.month == 9 or year_month.month == 11]
    xticks = [year_month for year_month in pd_df['Crime_Year_Month'] if
             year_month.month == 1 or year_month.month == 3 or year_month.month == 5 or year_month.month == 7 or year_month.month == 9 or year_month.month == 11]

    plt.xticks(xticks, xticks_label, rotation=90)
    plt.xlabel('Month', fontsize=16, labelpad=15)
    plt.ylabel('Number of Crimes', fontsize=16, labelpad=20)
    plt.title(title, fontsize=20, pad=25)
    plt.figure(1).subplots_adjust(
        **dict(left=0.1, right=.9, bottom=.15, top=.9, wspace=.1, hspace=.1))
    plt.show()


cf = SparkConf()
cf.set("spark.submit.deployMode", "client")
sc = SparkContext.getOrCreate(cf)

spark = SparkSession \
    .builder \
    .appName("NYC Complaint Analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

complaint_after_2015_df = spark.read.csv(path='NYPD_Complaint_After_2015.csv',header=True)
print("The dataset has "+str(complaint_after_2015_df.count())+" rows") # 3721217

# change the datatype of Crime_Date from string to date
complaint_after_2015_df = complaint_after_2015_df.withColumn("Crime_Date", F.col("Crime_Date").cast(DateType()))
complaint_after_2015_df.show()

complaint_after_2015_df.createOrReplaceTempView("complaint_after_2015")

###################
# Total Crimes
###################
# how many total complaints occurred each month after 2015-01-01?
complaint_month = complaint_after_2015_df.groupBy(F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'))
# complaint_month = complaint_month.select('Crime_Year_Month', 'count')
complaint_month.show()

showSingleLineGraph(complaint_month, 'The Number of Crimes Occurred From 2015 - 2022', 10, 20)

###################
# Crimes in Different Offense Levels
###################
# want to analyze complaints based on offense level
# column LAW_CAT_CD includes the Level of offense: felony, misdemeanor, violation
# examine the null values in offense level column
print("Null values in LAW_CAT_CD column: "+str(complaint_after_2015_df.filter(complaint_after_2015_df['LAW_CAT_CD'].isNull()).count())) # 0
complaint_offense_level = complaint_after_2015_df.groupBy(F.year('Crime_Date'), F.month('Crime_Date'),'Crime_Year_Month','LAW_CAT_CD').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'), 'LAW_CAT_CD')
complaint_offense_level = complaint_offense_level.select('Crime_Year_Month','LAW_CAT_CD', 'count')
complaint_offense_level.show()

pd_df = complaint_offense_level.toPandas()

pd_df['Crime_Year_Month'] =pd.to_datetime(pd_df['Crime_Year_Month'], format='%Y-%m')
pivot_df = pd_df.pivot(index='Crime_Year_Month', columns='LAW_CAT_CD', values='count')
ma_df = pivot_df.rolling(window=12).mean()

print(ma_df.to_string())

# plot
fig, ax = plt.subplots(figsize=(20, 12))

# red, orange, yellow
ori_line_colors = ['#A8A8FF', '#86C3FF', '#86E1E1']
ma_line_colors = ['#000099', '#004C99', '#008383']


i=0
j=0
# iterate every level, draw lines for original data
for level in pivot_df.columns:
    ax.plot(pivot_df.index, pivot_df[level], label=level, marker='o',color=ori_line_colors[i], alpha=0.9)
    i+=1

# iterate every level, draw lines for moving average
for level in ma_df.columns:
    ax.plot(ma_df.index, ma_df[level], label=level + ' MA', color=ma_line_colors[j], linestyle='--', linewidth=3)
    j+=1

    # draw two horizontal lines for the first datapoint of moving average line and the last datapoint of the moving average line
    ax.axhline(y=ma_df[level].iloc[11], ls='--', color='gray', alpha=0.6)
    ax.axhline(y=ma_df[level].iloc[-1], ls='--', color='gray', alpha=0.6)

    ax.annotate(f'{ma_df[level].iloc[11]:.2f}',
                xy=(ma_df.index[11], ma_df[level].iloc[11]), xytext=(0, -20),
                textcoords='offset points', ha='center', color='#FF8000', fontsize=15)

    ax.annotate(f'{ma_df[level].iloc[-1]:.2f}',
                xy=(ma_df.index[-1], ma_df[level].iloc[-1]), xytext=(0, 20),
                textcoords='offset points', ha='center', color='#FF8000', fontsize=15)

# draw a background for covid-19 period
plt.axvspan(pd.to_datetime('2020-03-01'), pd.to_datetime('2022-12-01'), color='gray', alpha=0.1, hatch='//')

ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0, ncol=1)

xticks = [year_month for year_month in pd_df['Crime_Year_Month'] if year_month.month == 1 or year_month.month == 3 or year_month.month == 5 or year_month.month == 7 or year_month.month == 9 or year_month.month == 11]

plt.xticks(xticks, rotation=90)
ax.set_xticklabels(year_month.strftime("%Y-%m") for year_month in pd_df['Crime_Year_Month'] if year_month.month == 1 or year_month.month == 3 or year_month.month == 5 or year_month.month == 7 or year_month.month == 9 or year_month.month == 11)

plt.xlabel('Crime Month', fontsize=16, labelpad=15)
plt.ylabel('The Number of Crimes', fontsize=16, labelpad=20)
plt.title("The Number of Crimes in Three Offence Levels", fontsize=20, pad=25)
plt.figure(1).subplots_adjust(
    **dict(left=0.1, right=.85, bottom=.15, top=.9, wspace=.1, hspace=.1))
plt.show()


###################
# Crimes in Different Offense Types
###################
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
#complaint_offense_type = complaint_offense_type.select('Crime_Year_Month','OFNS_DESC','count')
complaint_offense_type.show()

complaint_offense_type.createOrReplaceTempView("complaint_offense_type")

###################
# Homicide
###################
# how many homicide occurred?
homicide = spark.sql("SELECT Crime_Year_Month, OFNS_DESC, count FROM complaint_offense_type WHERE OFNS_DESC = 'MURDER & NON-NEGL. MANSLAUGHTER'")
homicide.show()

showSingleLineGraph(homicide, 'Homicide', 10, 20)

###################
# Assaults
###################
# how many assaults occured?
assaults = spark.sql("SELECT * FROM complaint_offense_type WHERE OFNS_DESC = 'ASSAULT 3 & RELATED OFFENSES' or OFNS_DESC = 'FELONY ASSAULT'")
assaults = assaults.groupBy('year(Crime_Date)', 'month(Crime_Date)', 'Crime_Year_Month').agg(F.sum('count').alias('count')).orderBy('year(Crime_Date)', 'month(Crime_Date)')
assaults.show()

showSingleLineGraph(assaults, 'Assaults', 10, 20)

###################
# robbery, burglary, and larceny
###################
# how many robbery, burglary, and larceny occurred?
rbl = spark.sql("SELECT * FROM complaint_offense_type WHERE OFNS_DESC = 'ROBBERY' or OFNS_DESC = 'BURGLARY' or OFNS_DESC = 'PETIT LARCENY' or OFNS_DESC = 'GRAND LARCENY'")
rbl = rbl.groupBy('year(Crime_Date)', 'month(Crime_Date)', 'Crime_Year_Month').agg(F.sum('count').alias('count')).orderBy('year(Crime_Date)', 'month(Crime_Date)')
rbl.show()

showSingleLineGraph(rbl, 'Robbery, Burglary, and Larceny', 10, 20)

###################
# motor vehicle theft
###################
# how many motor vehicle theft occurred?
mvt = spark.sql("SELECT * FROM complaint_offense_type WHERE OFNS_DESC = 'PETIT LARCENY OF MOTOR VEHICLE' or OFNS_DESC = 'GRAND LARCENY OF MOTOR VEHICLE'")
mvt = mvt.groupBy('year(Crime_Date)', 'month(Crime_Date)', 'Crime_Year_Month').agg(F.sum('count').alias('count')).orderBy('year(Crime_Date)', 'month(Crime_Date)')
mvt.show()

showSingleLineGraph(mvt, 'Motor Vehicle Theft', 10, 20)

###################
# drug offense
###################
# how many drug offense occurred?
do = spark.sql("SELECT * FROM complaint_offense_type WHERE OFNS_DESC = 'DANGEROUS DRUGS' or OFNS_DESC = 'UNDER THE INFLUENCE OF DRUGS' or OFNS_DESC = 'CANNABIS RELATED OFFENSES'")
do = do.groupBy('year(Crime_Date)', 'month(Crime_Date)', 'Crime_Year_Month').agg(F.sum('count').alias('count')).orderBy('year(Crime_Date)', 'month(Crime_Date)')
do.show()

showSingleLineGraph(do, 'Drug Offense', 10, 20)


###################
# Asian Victims Percentage
###################
# want to analyze victim race
# examine the values in victim race column
complaint_after_2015_df.select("VIC_RACE").distinct().show()

# there are null and UNKNOWN values in the victim race column
# want to change the null values and '(null)' values to UNKNOWN in victim race column
complaint_clean_race = complaint_after_2015_df.filter(complaint_after_2015_df['VIC_RACE'].isNotNull())
complaint_clean_race = complaint_clean_race.filter(complaint_after_2015_df['VIC_RACE']!='(null)')
complaint_clean_race = complaint_clean_race.filter(complaint_after_2015_df['VIC_RACE']!='UNKNOWN')
complaint_clean_race.select("VIC_RACE").distinct().show()

# victim races VS. complaint count each month after 2015-01-01
complaint_race = complaint_clean_race.groupBy(F.year('Crime_Date'), F.month('Crime_Date'), 'Crime_Year_Month', 'VIC_RACE').count().orderBy(F.year('Crime_Date'), F.month('Crime_Date'),  'VIC_RACE')
# complaint_race = complaint_race.select('Crime_Year_Month', 'VIC_RACE', 'count')
complaint_race.show()

# interested in asian victims, exclude the rows that are not asian victims
complaint_asian = complaint_race.filter(complaint_clean_race['VIC_RACE']=="ASIAN / PACIFIC ISLANDER")
complaint_asian.show()
complaint_asian = complaint_asian.withColumnRenamed("count", "asian_count")
#complaint_asian = complaint_asian.groupBy('year(Crime_Date)', 'month(Crime_Date)', 'Crime_Year_Month').count().alias('asian_count').orderBy('year(Crime_Date)', 'month(Crime_Date)', 'Crime_Year_Month')
# complaint_asian = complaint_asian.select('Crime_Year_Month', 'count')

total_race = complaint_race.groupBy('Crime_Year_Month').agg(F.sum('count').alias('total_race_count')).orderBy('Crime_Year_Month')

total_race.show()

asian_percent = complaint_asian.join(total_race, 'Crime_Year_Month') \
    .withColumn('percent', F.col('asian_count') / F.col('total_race_count') * 100)

asian_percent = asian_percent.orderBy('year(Crime_Date)', 'month(Crime_Date)')
asian_percent.show()


# plot the Asian victim complaint count by months
pd_df = asian_percent.toPandas()
pd_df['Crime_Year_Month'] = pd.to_datetime(pd_df['Crime_Year_Month'], format='%Y-%m')
print(pd_df.to_string())


# using moving average method, window size = 12 months
rolling_mean = pd_df['percent'].rolling(12).mean()
plt.figure(figsize=(20, 10))

# plot the original percentage and the moving average line
plt.plot(pd_df['Crime_Year_Month'], pd_df['percent'], marker='o', label='percent every month', color='#63589f')
plt.plot(pd_df['Crime_Year_Month'], rolling_mean, label='moving average line (12 months)', color='#b998dd',
         linestyle='--', linewidth=3)

# draw a background for covid-19 period
plt.axvspan(pd.to_datetime('2020-03-01'), pd.to_datetime('2022-12-01'), color='gray', alpha=0.1, hatch='//')

plt.legend()

xticks_label = [year_month.strftime("%Y-%m") for year_month in pd_df['Crime_Year_Month'] if
                year_month.month == 1 or year_month.month == 3 or year_month.month == 5 or year_month.month == 7 or year_month.month == 9 or year_month.month == 11]
xticks = [year_month for year_month in pd_df['Crime_Year_Month'] if
        year_month.month == 1 or year_month.month == 3 or year_month.month == 5 or year_month.month == 7 or year_month.month == 9 or year_month.month == 11]

plt.xticks(xticks, xticks_label, rotation=90)

plt.xlabel('Month', fontsize=16, labelpad=15)
plt.ylabel('Percentage of Victims', fontsize=16, labelpad=20)
plt.title('Percentage of Asian/Pacific Islander Victims', fontsize=20, pad=25)
plt.figure(1).subplots_adjust(
        **dict(left=0.1, right=.9, bottom=.15, top=.9, wspace=.1, hspace=.1))
plt.show()





