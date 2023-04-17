# Big-Data-2023
Project for CS-GY-6513 - Big Data 

April 16th progress

NYC Complaints
1. Combined the Historic and Current dataset, selected couple columns that we are interested in. Columns: CMPLNT_NUM, CMPLNT_FR_DT, CMPLNT_TO_DT, ADDR_PCT_CD, KY_CD, OFNS_DESC, BORO_NM, SUSP_AGE_GROUP, SUSP_RACE, SUSP_SEX, VIC_AGE_GROUP, VIC_RACE, VIC_SEX, Latitude, Longitude
2. Created a Crime_Date column based on columns CMPLNT_FR_DT and CMPLNT_TO_DT, CMPLNT_FR_DT if it exists, otherwise CMPLNT_TO_DT
3. Excluded rows that have Crime_Date before 2015-01-01
4. Showed how many complaints occurred each month
<img width="347" alt="Screen Shot 2023-04-16 at 3 38 27 PM" src="https://user-images.githubusercontent.com/55362828/232338079-7f73fa7b-64f9-45de-a66d-27ce8ed8d10e.png">
5. Examined offense description column (OFNS_DESC), decided to exclude rows that have null values in this column for analyzing complaints based on offense types
6. Showed how many complaints based on offense types occurred each month
<img width="507" alt="ESCAPE 31" src="https://user-images.githubusercontent.com/55362828/232338084-41ccdfb2-2fd7-4b82-bf99-21589384ee9c.png">
7. Examined victim race column (VIC_RACE), decided to replace rows that have null values in this column with UNKNOWN for analyzing complaints and victim races
<img width="513" alt="BLACK HISPANIC 1154" src="https://user-images.githubusercontent.com/55362828/232338093-acb777c5-db63-4097-92c5-823d1969d8ee.png">

April 17th progress
1. Created a Crime_Year_Month column based on Crime_Date, this is used in plots
2. Created a plot for total complaints vs Crime_Year_Date
<img width="609" alt="Crime Year Month" src="https://user-images.githubusercontent.com/55362828/232588781-e2efc879-3d3b-4132-816b-2b7a7f235a3a.png">
3. Interested in asian victims, excluded the rows that are not asian victims, group by year and month
<img width="647" alt="onty showing top 20 rows" src="https://user-images.githubusercontent.com/55362828/232588928-a79903b2-860a-4928-ae61-c69796bfd5dd.png">
4. Crated a plot for Asian victim complaints vs months
<img width="577" alt="Crime Year Month" src="https://user-images.githubusercontent.com/55362828/232588975-f627d599-12e0-4e07-93a7-0af056fe7541.png">
5. Interested in complaint cases in precincts, examined the null values in precinct column and excluded them, group by precinct, year and month
<img width="570" alt="2015-11" src="https://user-images.githubusercontent.com/55362828/232589116-d99935c8-d2df-4693-93c4-0cf73fd0e4f0.png">

