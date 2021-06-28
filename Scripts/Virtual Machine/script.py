from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import pubsub_v1
from google.cloud import bigquery
import pandas as pd
import datetime
import os.path
import time
import json
import csv
import io

def pubsub(project_id, message):

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id,'trigger-log')
    message_bytes = message.encode('utf-8')

    print('1')
    
    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()
        time.sleep(5)
        return( 'Message published.')
    except Exception as e:
        return(e)


def main():

    project_id = 'jobsity-317503'
    bq_client = bigquery.Client(project=project_id)

    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = '/home/alexsander_rodrigues_siqueira/jobsity-317503-8f6aa2b02ebe.json'

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3',credentials=creds)

    # Call the Drive v3 API
    results = service.files().list(q="'144a7RKyQC6o9GB1TkEZ-wuSpqV8Lr5tZ' in parents",
                          fields='nextPageToken, files(id, name)').execute()
    items = results.get('files', [])

    if not items:
        print('No files to import')
    else:
        for item in items:
            if item['name'] == 'trips.csv':
                print('Files:')
                print(u'{0} ({1})'.format(item['name'], item['id']))
                file_id = item['id']
                file_name = item['name'] + '_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S")

                message = {"id-execution" : "1"
                          , "type" : "file"
                          , "origin" : "google drive"
                          , "name" : item['name']
                          , "status" : "Processing"
                          , "message" : "Importing file"}
                message = json.dumps(message)
                pubsub(project_id,message)

                request = service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    #print("Download %d%%." % int(status.progress() * 100))
                
                fh.seek(0)
                pd_df = pd.read_csv(fh)
                pd_df.to_csv(file_name)
                qt_read = len(pd_df.index)

                SCOPES = ['https://www.googleapis.com/auth/drive.file']
                #rename imported file
                body = { "name": "processed_" + file_name }
                service.files().update(fileId=file_id, body=body).execute()

                message = {"id-execution" : "2"
                          , "name" : item['name']
                          , "qt_read": qt_read}
                message = json.dumps(message)
                print(pubsub(project_id,message))

                dtinsert = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                pd_df['dtinsert'] = dtinsert

                dataset = bq_client.dataset(dataset_id='raw').table('trips')
                table = bq_client.get_table(dataset)
                insert_row = bq_client.insert_rows_from_dataframe(table=table, dataframe=pd_df)
                print(f"Tabela populada com sucesso: raw.trips")

                message = {"id-execution" : "2"
                          , "name" : item['name']
                          , "qt_write" : qt_read
                          , "dt_end" : datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}
                message = json.dumps(message)
                print(pubsub(project_id,message))

                #Update region dimension
                bq_client.query('''
                                  INSERT INTO `jobsity-317503.trusted.dim_region` 
                                  SELECT (SELECT max(id) id FROM `jobsity-317503.trusted.dim_region`) + row_number() over(order by region) as id
                                      ,region
                                  FROM `jobsity-317503.raw.trips`
                                  WHERE region not in (select region from `jobsity-317503.trusted.dim_region`)
                                  GROUP BY region;
                                ''')

                #Update datasource dimension 
                bq_client.query('''
                                  INSERT INTO `jobsity-317503.trusted.dim_datasource` 
                                  SELECT (SELECT max(id) id FROM `jobsity-317503.trusted.dim_region`) + row_number() over(order by datasource) as id
                                      ,datasource
                                  FROM `jobsity-317503.raw.trips`
                                  WHERE datasource not in (select datasource from `jobsity-317503.trusted.dim_datasource`)
                                  GROUP BY datasource
                            ''')

                #Update fact table
                bq_client.query('''
                                  INSERT INTO `jobsity-317503.trusted.fact_trips`
                                  WITH consulta AS (
                                      SELECT ROW_NUMBER() OVER(PARTITION BY region,origin_coord,destination_coord,datetime ORDER BY dtinsert DESC) as dedup 
                                          ,region
                                          ,REPLACE(origin_coord, 'POINT ','') AS origin_coord
                                          ,REPLACE(destination_coord, 'POINT ','') AS destination_coord
                                          ,datasource
                                          ,datetime
                                          ,MD5(CONCAT(region,origin_coord,destination_coord,datetime)) AS key_trip
                                      FROM `jobsity-317503.raw.trips`)
                                  SELECT ROW_NUMBER() OVER(ORDER BY a.datetime) as id 
                                      ,cast(a.datetime as timestamp) as datetime
                                      ,cast(substring(a.origin_coord,2,STRPOS(a.origin_coord,' ')-2) as numeric) as origin_lat 
                                      ,cast(substring(a.origin_coord,STRPOS(a.origin_coord,' ')+1,STRPOS(substring(a.origin_coord,STRPOS(a.origin_coord,' ')+1,length(a.origin_coord)),')')-1) as numeric) as origin_lon 
                                      ,cast(substring(a.destination_coord,2,STRPOS(a.destination_coord,' ')-2) as numeric) as destination_lat 
                                      ,cast(substring(a.destination_coord,STRPOS(a.destination_coord,' ')+1,STRPOS(substring(a.destination_coord,STRPOS(a.destination_coord,' ')+1,length(a.destination_coord)),')')-1) as numeric) as destination_lon 
                                      ,b.id as region_id 
                                      ,c.id as datasource_id 
                                      ,a.key_trip
                                      ,cast(left(cast(CURRENT_DATETIME() as string),19) as timestamp) as dtinsert
                                  FROM consulta a
                                  INNER JOIN `jobsity-317503.trusted.dim_region` AS b ON a.region = b.desc_region
                                  INNER JOIN `jobsity-317503.trusted.dim_datasource` AS c ON a.datasource = c.desc_datasource
                                  LEFT JOIN `jobsity-317503.trusted.fact_trips` AS d ON d.key_trip = a.key_trip
                                  WHERE a.dedup = 1
                                  AND d.key_trip IS NULL
                                ''')

                # upload imported file to history folder
                file_metadata = {
                                    "name": item['name'],
                                    "parents": ["1wlzXmY7Vz2_AyHFRZO1PneUfZo1PKok1"] #History folder ID
                                }
                media = MediaFileUpload(file_name, resumable=True)
                file = service.files().create(body=file_metadata, media_body=media, fields='id, name, parents').execute()

                message = {"id-execution" : "2"
                          , "name" : item['name']
                          , "status" : "Finished"
                          , "message" : "Success"
                          , "dt_end" : datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}
                message = json.dumps(message)
                pubsub(project_id,message)
            else:
                print('No files to import')


if __name__ == '__main__':
    main()
