from base import Base
import pandas as pd
# import requests
# import concurrent.futures
# import json
# import base64
import logging
import time
import queue
# import threading
# import requests.exceptions
from datetime import datetime,timedelta
# import re
import os

class Downloader(Base):
    
    def __init__(self):
        Base.__init__(self)
        self.todayFile = datetime.now()
        self.formatted_time = self.todayFile.strftime("%H_%M_%S")
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'error_log.txt')
        logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info('Inicio de la ejecución')
        self.queue = queue.Queue()
        self.thread_count = 5
        
    def dateToday(self):
        self.today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).date()
        self.todayPostresql = self.today.strftime('%d-%m-%Y')
        print('todayPostresql', self.todayPostresql)
    
    # Query de casos creados en crmcomwareapp
    def queryCasesCRM(self):
        self.connect()
        return self.query( f"""
            SELECT DISTINCT ON (tp."CASE_ID")
                tp."CREATION_DATE",
                tp."CASE_ID",
                tp."FILE_NAME"
            FROM
                "TTBL_TIPIFICACION_COSMOS" tp
            WHERE
                tp."CREATION_DATE" IS NOT NULL 
                AND tp."FILE_NAME" <> ''
                AND tp."CREATION_DATE" >= to_date('19-03-2024', 'DD-MM-YYYY')
                AND tp."CREATION_DATE" < to_date('20-03-2024', 'DD-MM-YYYY')
                AND tp."CHANNEL_TYPE" = '60'
            ORDER BY tp."CASE_ID", tp."CREATION_DATE" ASC
        """)
      
    # Construir df con la informacion de los casos
    def buildDFCases(self):
        try:
            self.dfCases = pd.DataFrame(self.queryCasesCRM())
            data = len(self.dfCases['FILE_NAME'])
            print('data',data)
            
            if self.dfCases.empty:
                print("El DataFrame está vacío dfCasesCRMWARE.")
                self.dfCasesCrm = self.dfCases

        except Exception as e:
            logging.error(f"Error buildDFCases: {e}")
    
    def download_file(self, file_name):
        path = "C:\\upload_adjuntos_bucket_ventana_digital\\descarga"
        local_file_path = os.path.join(path, file_name)
        print('local_file_path', local_file_path)

        for retry in range(3):
            try:
                s3_client = self.connectAWS()

                # Obtener el prefijo para la descarga del archivo
                prefix = self.dfCases.loc[self.dfCases['FILE_NAME'] == file_name, 'CASE_ID'].values[0]
                print('prefix', prefix)

                # Listar objetos en el bucket con el prefijo dado
                objects = s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
                for obj in objects.get('Contents', []):
                    key = obj['Key']
                    print('obj', obj)
                    
                    # Descargar el objeto si coincide con el nombre del archivo
                    if key.endswith(file_name):
                        s3_client.download_file(self.bucket, key, local_file_path)
                        print(f"Descargado: {file_name}")
                        return  # Salir del método después de la descarga exitosa

                error_message = f"No se encontró el archivo {file_name} con el prefijo {prefix} en el bucket."
                logging.error(error_message)
                break  # Si no se encuentra el archivo, no hay necesidad de reintentar

            except Exception as e:
                print(f"Error al descargar {file_name}: {e}")
                if retry < 2:  # Permitir hasta 2 reintentos adicionales
                    print("Reintentando la descarga en 5 segundos...")
                    time.sleep(5)  