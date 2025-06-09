# covid-pipeline
This project builds complete  ETL pipeline using  AWS GLUE and S3 processing daily covid-19 reports (01.01.2021,01.01.2022,01.01.2023)
GOAL:
  
-  Merge daily CSVs
-  Normilazation of country
-  Calculate new daily cases
-  Add 7-day moving averages
-  Clean and filter data
-  Output structured results for dashboard us
TOOLS AND USAGE:
  AWS GLUE :py spark and etl jobs
  Crawler:schema detection 
  s3 : storage for raw data and cleandata
  parquet:output file format

**ETL Flow**

**Raw Input**  
` folder – raw 

**ETL Steps** (in Glue):
1. Read + merge all daily files
2. Remove nulls and 0s where needed
3. Calculate:
   - `New_Cases = confirmed - previous_day_confirmed`
   - `New_Cases_MA7 = 7-day moving avg of New_Cases`
4. Round metrics like `case_fatality_ratio`
5. Output clean Parquet file to `S3/processed/`
6. Screenshots check folder for all screenshots


