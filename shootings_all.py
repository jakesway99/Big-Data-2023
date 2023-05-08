import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import BooleanType
import matplotlib.pyplot as plt
import pandas as pd


def generate_graph_pd(df):
    shooting_df = df.withColumn('year', f.year('occur_date')).withColumn('month', f.lpad(f.month('occur_date'), 2, '0'))
    shooting_df.createOrReplaceTempView("shootings")
    by_month_df = spark.sql(
        "SELECT year,month, count(*) as cnt FROM shootings GROUP BY year, month ORDER BY year, month")
    by_month_df = by_month_df.withColumn('year_month', f.concat_ws('-', 'year', 'month'))
    by_month_df = by_month_df.drop('year', 'month')
    count_by_year_month_pd = by_month_df.toPandas()

    count_by_year_month_pd['year_month_datetime'] = pd.to_datetime(count_by_year_month_pd['year_month'])
    count_by_year_month_pd['index'] = count_by_year_month_pd['year_month_datetime']
    count_by_year_month_pd.set_index('index', inplace=True)
    count_by_year_month_pd['sma'] = count_by_year_month_pd['cnt'].rolling(window=12).mean()

    return count_by_year_month_pd


spark = SparkSession.builder.appName("covid-shootings").getOrCreate()
shooting_df_old = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
shooting_df_new = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])

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

all_shooting_df.coalesce(1).write.csv("shootings_all_output.csv", header=True)

count_by_year_month_pd = generate_graph_pd(all_shooting_df)

# Create a line plot
plt.figure(figsize=(12, 6))
plt.plot(count_by_year_month_pd['year_month'], count_by_year_month_pd['cnt'], marker='o')

xticks = [year_month for idx, year_month in enumerate(count_by_year_month_pd['year_month']) if idx % 2 == 0]
plt.xticks(xticks, rotation=90)

plt.xlabel('Year-Month')
plt.ylabel('Count')
plt.title('Counts by Year and Month')
plt.xticks(rotation=90)

plt.show()
