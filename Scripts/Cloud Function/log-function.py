import base64
import json
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.bigquery._helpers import _record_field_to_json

def log_function(event, context):
     print(event)
     pubsub_message = base64.b64decode(event['data']).decode('utf-8')
     pubsub_message = json.loads(pubsub_message)
     print(f'Input: {pubsub_message}')

     PROJECT_ID = 'jobsity-317503'
     client = bigquery.Client(project=PROJECT_ID)
     
     id_exec = pubsub_message['id-execution']
     v_name = pubsub_message['name']

     print(f'ID Exec: {id_exec}')

     if id_exec == '1':
          
          v_type = pubsub_message['type']
          v_origin = pubsub_message['origin']
          v_status = pubsub_message['status']
          v_message = pubsub_message['message']
          v_dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

          # Consulta último id da tabela para definir o próximo
          query = client.query('''SELECT MAX(id_load) id_load FROM `jobsity-317503.log.loads`''')
          for row in query:
               if row.id_load == None:
                    v_id_load = 1
               else:
                    v_id_load = int(row.id_load) + 1
          
          print(f'id_load: {v_id_load}')

          row = {'id_load' : v_id_load,
               'type' : v_type,
               'origin' : v_origin,
               'name' : v_name,
               'status' : v_status,
               'message' : v_message,
               'qt_read' : None,
               'qt_write' : None,
               'dt_start' : v_dt_start,
               'dt_end' : None
               ,'dtinsert' : datetime.today().strftime('%Y-%m-%d %H:%M:%S')}
          
          print(f'Row: {row}')

          #Insere JSON no BigQuery
          bq_client = bigquery.Client(project=PROJECT_ID)
          dataset = bq_client.dataset(dataset_id='log').table('loads')
          table = bq_client.get_table(dataset)
          schema = table.schema
          json_rows = [_record_field_to_json(schema, row) for rows in [1]]
          insert_row = bq_client.insert_rows_json(table=table, json_rows=json_rows)
          print(f'Insert: {insert_row}')
          print(f'Carga {v_id_load} criada com sucesso')
          
     elif id_exec == '2':

          # Consulta último id para ser atualizado
          query = client.query(''' SELECT * 
                                   FROM `jobsity-317503.log.vw_loads` 
                                   WHERE id_load = (SELECT MAX(id_load) id_load 
                                                    FROM `jobsity-317503.log.loads` 
                                                    WHERE name = '{0}') 
                              '''.format(v_name))
          for row in query:
               v_id_load = int(row.id_load)
               v_type = row.type
               v_origin = row.origin
               v_status = row.status
               v_message = row.message
               v_qt_read = row.qt_read
               v_qt_write = row.qt_write
               v_dt_start = row.dt_start
               v_dt_end = row.dt_end

          print(f'id_load: {v_id_load}')

          for campo in pubsub_message:
               if campo == 'status':
                    v_status = pubsub_message[campo]
               elif campo == 'message':
                    v_message = pubsub_message[campo]
               elif campo == 'qt_read':
                    v_qt_read = pubsub_message[campo]
               elif campo == 'qt_write':
                    v_qt_write = pubsub_message[campo]
               elif campo == 'dt_end':
                    v_dt_end = pubsub_message[campo]

          row = {'id_load' : v_id_load,
               'type' : v_type,
               'origin' : v_origin,
               'name' : v_name,
               'status' : v_status,
               'message' : v_message,
               'qt_read' : v_qt_read,
               'qt_write' : v_qt_write,
               'dt_start' : v_dt_start,
               'dt_end' : v_dt_end,
               'dtinsert' : datetime.today().strftime('%Y-%m-%d %H:%M:%S')}
          
          print(f'Row: {row}')

          #Insere JSON no BigQuery
          bq_client = bigquery.Client(project=PROJECT_ID)
          dataset = bq_client.dataset(dataset_id='log').table('loads')
          table = bq_client.get_table(dataset)
          schema = table.schema
          json_rows = [_record_field_to_json(schema, row) for rows in [1]]
          insert_row = bq_client.insert_rows_json(table=table, json_rows=json_rows)
          print(f'Insert: {insert_row}')
          print(f'Load {v_id_load} updated successfully')
