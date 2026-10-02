# CMS Provider Data Metastore Task

Given the CMS provider data metastore, write a Python script that downloads all data sets related to the theme "Hospitals".

## Requirements

- The script should download all datasets related to the theme "Hospitals" from the CMS provider data metastore.
- The CSV column headers currently use mixed case, spaces, and special characters. Convert all column names to snake_case.
  - Example: "Patients’ rating of the facility linear mean score" becomes "patients_rating_of_the_facility_linear_mean_score"
- The CSV files should be downloaded and processed in parallel.
- The job should be designed to run every day.
- Only download files that have been modified since the previous run.
- Track run metadata so the script can detect what changed since the last execution.
- The solution must be written in Python and must run on a standard Windows or Linux computer.
- Do not rely on platform-specific services such as Databricks, AWS, or similar.
- Include a `requirements.txt` file if the job uses Python packages that are not included in the default Python installation.

## Source

- https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items

## Submission

Please email your code and a sample of your output to your recruiter or interviewer.

Add any additional comments or description below.
