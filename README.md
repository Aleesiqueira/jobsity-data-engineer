# Data Engineering - Challenge
This repository contains all files and scripts from the developed solution to ingest trips data and return info to the user.

## Solution Architecture
![image](https://user-images.githubusercontent.com/32855093/123534785-f1faa900-d6f5-11eb-9dae-ce0797eef64f.png)

The developed process runs as follows:
 - A GCE VM instance runs in GCP with a python script deployed as an always-restart service to monitor a Google Drive shared [folder](https://drive.google.com/drive/folders/144a7RKyQC6o9GB1TkEZ-wuSpqV8Lr5tZ?usp=sharing). 
 - If a "trips.csv" file is located, it automatically downloads the file to the machine and send it to [BigQuery](https://console.cloud.google.com/bigquery?project=jobsity-317503&ws=!1m0). After doing it, it puts the "processed_" prefix in the downloaded file name to indicate that it has been read.
 - During the script execution, log events are sent through a Pub/Sub topic that triggers a Cloud Function to insert them into BigQuery.
 - With the data already in the raw.trips table, a few custom querys are executed to distribute the data into a Star-Schema data model that has dimensions and fact tables.
 - After the dimensions and fact are updated, we can consume the information in the Data Studio [report](https://datastudio.google.com/reporting/53518e1c-51d2-4c17-a09d-b5f0369bbf75).

### Google Credentials
 - E-mail: jobsitydataengineer@gmail.com
 - Password: challenge@2021
