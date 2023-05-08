import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns


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


def shooting_graph_levels(df):
    df.createOrReplaceTempView("shootings_income")

    all_dfs = []
    shooting_low_mid_income_df = spark.sql("SELECT * FROM shootings_income WHERE income BETWEEN 0 AND 41000")
    all_dfs.append(shooting_low_mid_income_df)
    shooting_mid_income_df = spark.sql("SELECT * FROM shootings_income WHERE income BETWEEN 41001 AND 72000")
    all_dfs.append(shooting_mid_income_df)
    shooting_mid_high_income_df = spark.sql(
        "SELECT * FROM shootings_income WHERE income BETWEEN 72001 AND 123000")
    all_dfs.append(shooting_mid_high_income_df)
    shooting_high_income_df = spark.sql("SELECT * FROM shootings_income WHERE income > 123001")
    all_dfs.append(shooting_high_income_df)

    all_pd_dfs = []

    for df in all_dfs:
        pd_df = generate_graph_pd(df)
        all_pd_dfs.append(pd_df)

    labels = ['Low Income', 'Mid Income', 'Mid-High Income', 'High Income']

    fig, ax = plt.subplots()

    for idx, df in enumerate(all_pd_dfs):
        ax.plot(df['year_month'], df['cnt'], label=labels[idx])
        # ax.plot(df['year_month'], df['sma'], label=labels[idx] + ' (1Y Moving Avg)', linestyle='--')

    ax.set_xlabel('Year and Month')
    ax.set_ylabel('Count')

    ax.set_title('Shooting Incidents By Neighborhood Median Household Income')

    xticks = [year_month for idx, year_month in enumerate(all_pd_dfs[0]['year_month']) if idx % 2 == 0]

    plt.xticks(xticks, rotation=90)
    ax.legend()
    plt.show()

    # pandemic started affecting NYC in mid March.
    pandemic_start_month = pd.to_datetime('2020-04-01')

    # Calculate average shootings per month for each income level before and after the pandemic
    avg_before = []
    avg_after = []

    avg_before_by_month = np.zeros((12, 4))
    avg_after_by_month = np.zeros((12, 4))

    # loop through all the income dfs
    for count, df in enumerate(all_pd_dfs):
        df['year_month'] = pd.to_datetime(df['year_month'])
        df['month'] = df['year_month_datetime'].dt.month
        before_pandemic = df[df['year_month'] < pandemic_start_month]
        after_pandemic = df[df['year_month'] >= pandemic_start_month]
        before_avg = before_pandemic['cnt'].mean()
        after_avg = after_pandemic['cnt'].mean()
        avg_before.append(before_avg)
        avg_after.append(after_avg)

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        for month in range(1, 13):
            cnt_all_months_before = before_pandemic.loc[df['month'] == month]
            avg_before_by_month[month - 1][count] = cnt_all_months_before['cnt'].mean()

            cnt_all_months_after = after_pandemic.loc[df['month'] == month]
            avg_after_by_month[month - 1][count] = cnt_all_months_after['cnt'].mean()

    percentage_changes = []
    for before, after in zip(avg_before, avg_after):
        percentage_changes.append((after - before) / before * 100)

    # percentage changes from before covid to after covid
    labels = ['Low Income', 'Mid Income', 'Mid-High Income', 'High Income']
    percentage_changes_df = pd.DataFrame({'Income Level': labels, 'Percentage Change': percentage_changes})
    print(percentage_changes_df)

    avg_before_by_month = np.nan_to_num(avg_before_by_month)
    avg_after_by_month = np.nan_to_num(avg_after_by_month)

    percentage_changes_by_month = np.divide(
        avg_after_by_month - avg_before_by_month,
        avg_before_by_month,
        out=np.zeros_like(avg_after_by_month),
        where=(avg_before_by_month != 0),
    ) * 100

    # Create a table (DataFrame) to display the results
    month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    labels = ['Low Income', 'Mid Income', 'Mid-High Income', 'High Income']

    percentage_changes_df = pd.DataFrame(percentage_changes_by_month, columns=labels, index=month_labels)
    print(percentage_changes_df)
    return percentage_changes_df


spark = SparkSession.builder.appName("shootings-income").getOrCreate()
shootings_income_nta_df = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
shootings_income_nta_df = shootings_income_nta_df.withColumn('occur_date', f.to_date('occur_date', 'M/d/yyyy'))
change_by_month = shooting_graph_levels(shootings_income_nta_df)
change_by_month = change_by_month.drop('High Income', axis=1)

data = change_by_month.reset_index().rename(columns={'index': 'Month'})

melt = pd.melt(data, id_vars=['Month'], var_name='Income Level', value_name='Crime Rate')
pivot = melt.pivot(index='Month', columns='Income Level', values='Crime Rate')
correct_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
data_pivot = pivot.reindex(correct_order)

sns.heatmap(data_pivot, cmap=sns.cm.rocket_r)

plt.show()
