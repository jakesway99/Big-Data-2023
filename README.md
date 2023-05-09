# Big-Data-2023
Project for CS-GY-6513 - Big Data 


To Reproduce Complaint Analysis
1. Download the datasets from these two links: 
    [NYPD Complaint Data Historic](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i), 
    [NYPD Complaint Data Current (Year To Date)](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243)
2. Run complaint_clean_integrate.py
3. Run complaint_analysis.py

To Reproduce Shooting Analysis:
1. Download all 3 datasets:
   - [2010 Neighborhood Tabulation Areas (NTAs)](https://data.cityofnewyork.us/City-Government/2010-Neighborhood-Tabulation-Areas-NTAs-/cpf4-rkhq)
   - [NYPD Shooting Incident Data Historic](https://data.cityofnewyork.us/Public-Safety/NYPD-Shooting-Incident-Data-Historic-/833y-fsy8)
   - [NYPD Shooting Incident Data Year To Date](https://data.cityofnewyork.us/Public-Safety/NYPD-Shooting-Incident-Data-Year-To-Date-/5ucz-vwe8)
   - These datasets are included in GitHub as NTA_json.geojson, shootings_historic.csv, and shootings_YTD.csv
2. Run shootings_all.py with inputs: shootings_historic.csv, shootings_YTD.csv
3. Go into the shootings_all_output folder and rename the output csv as shootings_all_output.csv, move this file to the main directory
4. Run geomapping.py with inputs shootings_all_output.csv, shootings
   - this outputs shootings_nta_incomes.csv
   - this script requires NTA_json.geojson in your main directory
5. Run shootings_graphs.py with input shootings_nta_incomes.csv

Note: the ntacode_incomes.csv is not easily reproducible. Running get_neighborhoods_nta.py yields the all_ntanames.csv, but that file was then manually populated with incomes.
