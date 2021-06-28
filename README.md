# Data Engineering - Challenge
This repository contains all files and scripts from the developed solution to ingest trips data and return info to the user.

## Solution Architecture
![image](https://user-images.githubusercontent.com/32855093/123534785-f1faa900-d6f5-11eb-9dae-ce0797eef64f.png)

The developed process runs as follows:
 - A GCE VM instance runs in GCP with a python script deployed as an always-restart service to monitor a Google Drive shared [folder](https://drive.google.com/drive/folders/144a7RKyQC6o9GB1TkEZ-wuSpqV8Lr5tZ?usp=sharing). 
 - If a "trips.csv" file is located, it automatically downloads the file to the machine and send it to [BigQuery](https://console.cloud.google.com/bigquery?project=jobsity-317503&ws=!1m0). After doing it, it puts the "processed_" prefix in the downloaded file name to indicate that it has been read.
 - During the script execution, log events are sent through a [Pub/Sub](https://console.cloud.google.com/cloudpubsub/topic/list?project=jobsity-317503) topic that triggers a [Cloud Function](https://console.cloud.google.com/functions/list?project=jobsity-317503&tab=source) to insert them into BigQuery.
 - With the data already in the raw.trips table, a few custom querys are executed in the python script to distribute the data on BigQuery into a Star-Schema data model that has dimensions and fact tables in a dataset called trusted.
 - After the dimensions and fact are updated, we can consume the information in a Data Studio [report](https://datastudio.google.com/reporting/53518e1c-51d2-4c17-a09d-b5f0369bbf75). Beeing able to monitor my ingestions with a log report and extract insights and with an overview panel that contains visual representations of the data and cards to highlight the KPIs that are most important.

### Google Credentials
 - E-mail: jobsitydataengineer@gmail.com
 - Password: challenge@2021

### Considerations
- Container: As the main engine of this ingestion process is a python script that runs in a Compute Engine instance, we could containerize it to run in a Kubernetes environment, which could also be in GCP, ingesting data into BigQuery. 
