import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import BooleanType
import matplotlib.pyplot as plt


def generate_graph_pd(df):
    shooting_df = df.withColumn('year', f.year('occur_date')).withColumn('month', f.month('occur_date'))
    shooting_df.createOrReplaceTempView("shootings")
    by_month_df = spark.sql(
        "SELECT year,month, count(*) as cnt FROM shootings GROUP BY year, month ORDER BY year, month")
    by_month_df = by_month_df.withColumn('year_month', f.concat_ws('-', 'year', 'month'))
    by_month_df = by_month_df.drop('year', 'month')
    count_by_year_month_pd = by_month_df.toPandas()
    return count_by_year_month_pd


spark = SparkSession.builder.appName("covid-shootings").getOrCreate()
shooting_df_old = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
shooting_df_new = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])
precinct_neighborhood = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[3])
neighborhood_income = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[4])

shooting_df_old = shooting_df_old.toDF(*[c.lower() for c in shooting_df_old.columns])  # convert columns to lower case
shooting_df_old = shooting_df_old.withColumn('occur_date', f.to_date('occur_date', 'M/d/yyyy'))
shooting_df_old.createOrReplaceTempView("shooting_old")
shooting_df_old = spark.sql("SELECT * FROM shooting_old WHERE occur_date BETWEEN '2015-1-1' AND '2021-12-31'")
shooting_df_old = shooting_df_old.drop('x_coord_cd', 'y_coord_cd', 'lon_lat')

shooting_df_new = shooting_df_new.toDF(*[c.lower() for c in shooting_df_new.columns])  # convert columns to lower case
shooting_df_new = shooting_df_new.drop('loc_classfctn_desc', 'x_coord_cd', 'y_coord_cd', 'new georeferenced column',
                                       'loc_of_occur_desc')

col = 'statistical_murder_flag'  # this column has different data types in the data set
shooting_df_new = shooting_df_new.withColumn(col, f.when(f.col(col) == 'N', 'False').when(f.col(col) == 'Y', 'True'))
shooting_df_new = shooting_df_new.withColumn(col, f.col(col).cast(BooleanType()))
shooting_df_new = shooting_df_new.withColumn('occur_date', f.to_date('occur_date', 'M/d/yyyy'))

all_shooting_df = shooting_df_new.union(shooting_df_old)

# remove duplicates
all_shooting_df = all_shooting_df.dropDuplicates(['incident_key'])
all_shooting_df.createOrReplaceTempView("shootings")

# all_shooting_df.write.option("header", True).csv("shootings_all.csv")

precinct_neighborhood.createOrReplaceTempView("neighborhood")
shooting_neighborhood_df = spark.sql(
    "SELECT incident_key, occur_date, boro, shootings.precinct, perp_age_group, perp_sex, perp_race, vic_age_group, "
    "vic_sex, vic_race, latitude, longitude, Neighborhood FROM shootings LEFT JOIN neighborhood ON shootings.precinct "
    "= neighborhood.Precinct")

count_by_year_month_pd = generate_graph_pd(all_shooting_df)

# Create a line plot
plt.figure(figsize=(12, 6))
plt.plot(count_by_year_month_pd['year_month'], count_by_year_month_pd['cnt'], marker='o')

xticks = [year_month for year_month in count_by_year_month_pd['year_month'] if
          year_month.endswith('-1') or year_month.endswith('-7')]
plt.xticks(xticks, rotation=90)

plt.xlabel('Year-Month')
plt.ylabel('Count')
plt.title('Counts by Year and Month')
plt.xticks(rotation=90)

plt.show()

shooting_neighborhood_df.createOrReplaceTempView("shootings_n")
neighborhood_income.createOrReplaceTempView("income")

# want to exclude central park b/c it's not a neighborhood
shooting_income_df = spark.sql("SELECT * FROM shootings_n LEFT JOIN income using(Neighborhood) where precinct <> 22")
shooting_income_df = shooting_income_df.withColumnRenamed("Median Household Income",
                                                          "med_house_income").withColumnRenamed("Neighborhood",
                                                                                                "neighborhood")
shooting_income_df.createOrReplaceTempView("shootings_income")

all_dfs = []
shooting_low_income_df = spark.sql("SELECT * FROM shootings_income WHERE med_house_income < 25000")
all_dfs.append(shooting_low_income_df)
shooting_low_mid_income_df = spark.sql("SELECT * FROM shootings_income WHERE med_house_income BETWEEN 25000 AND 50000")
all_dfs.append(shooting_low_mid_income_df)
shooting_mid_income_df = spark.sql("SELECT * FROM shootings_income WHERE med_house_income BETWEEN 50001 AND 70000")
all_dfs.append(shooting_mid_income_df)
shooting_mid_high_income_df = spark.sql("SELECT * FROM shootings_income WHERE med_house_income BETWEEN 70001 AND 90000")
all_dfs.append(shooting_mid_high_income_df)
shooting_high_income_df = spark.sql("SELECT * FROM shootings_income WHERE med_house_income > 90000")
all_dfs.append(shooting_high_income_df)

all_pd_dfs = []

for df in all_dfs:
    pd_df = generate_graph_pd(df)
    all_pd_dfs.append(pd_df)

labels = ['Low Income', 'Low-Mid Income', 'Mid Income', 'Mid-High Income', 'High Income']

fig, ax = plt.subplots()

for idx, df in enumerate(all_pd_dfs):
    ax.plot(pd_df['year_month'], df['cnt'], label=labels[idx])

ax.set_xlabel('Year and Month')
ax.set_ylabel('Count')

ax.set_title('Shooting Incident Counts and Income Level of Neighborhood')

xticks = [year_month for year_month in all_pd_dfs[0]['year_month'] if
          year_month.endswith('-1') or year_month.endswith('-7')]

plt.xticks(xticks, rotation=45)

ax.legend()

plt.show()
