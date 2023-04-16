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

