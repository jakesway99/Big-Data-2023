# Big-Data-2023
Project for CS-GY-6513 - Big Data 

April 16th progress

NYC Complaints
1. Combined the Historic and Current dataset, selected couple columns that we are interested in. Columns: CMPLNT_NUM, CMPLNT_FR_DT, CMPLNT_TO_DT, ADDR_PCT_CD, KY_CD, OFNS_DESC, BORO_NM, SUSP_AGE_GROUP, SUSP_RACE, SUSP_SEX, VIC_AGE_GROUP, VIC_RACE, VIC_SEX, Latitude, Longitude
2. Created a Crime_Date column based on columns CMPLNT_FR_DT and CMPLNT_TO_DT, CMPLNT_FR_DT if it exists, otherwise CMPLNT_TO_DT
3. Excluded rows that have Crime_Date before 2015-01-01
4. Showed how many complaints occurred each month
5. Examined offense description column (OFNS_DESC), decided to exclude rows that have null values in this column for analyzing complaints based on offense types
6. Showed how many complaints based on offense types occurred each month
7. Examined victim race column (VIC_RACE), decided to replace rows that have null values in this column with UNKNOWN for analyzing complaints and victim races

April 17th progress
1. Created a Crime_Year_Month column based on Crime_Date, this is used in plots
2. Created a plot for total complaints vs Crime_Year_Date
3. Created a plot for victim races VS. complaint count every month
4. Interested in asian victims, excluded the rows that are not asian victims, group by year and month
5. Created a plot for Asian victim complaints vs months
6. Interested in complaint cases in precincts, examined the null values in precinct column and excluded them, group by precinct, year and month

April 20th progress
1. Analyzed complaints based on offense level, created a plot based on three offense levels: felony, misdemeanor, violation
2. Classified and created plots for homicide, assaults, robbery, burglary, larceny, motor vehicle theft and drug offenses
3. Made the tables look nicer (show only the year-month)

April 21st progress - (More Data Cleaning)
1. Checked and corrected the uniqueness constraints
2. Excluded rows with null values in Crime_Date column
3. Added labels and titles for plots
