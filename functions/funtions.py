import json

import requests
import hashlib
from datetime import datetime
import pandas as pd
from variables.variables_entorno import URL


def encrypt(data):
    try:
        data = str(data).encode('utf-8')
        return hashlib.sha1(data).hexdigest()
    except:
        print(' Error al Encriptar la data')


def send_request_get():
    try:
        r = requests.get(URL)
        return r.json()
    except:
        print("Error al reenviar el Request se hace un reintento")
        r = requests.get(URL)
        return r.json()


def make_etl(responseService):
    time_init = datetime.now()
    transform_list = []
    response = [worker for worker in responseService]
    try:
        for resp in response:
            languages = [language for language in resp['languages']]
            languages_name = [name['name'] for name in languages]
            sha1_encrypt = encrypt(languages_name)
            timer = datetime.now() - time_init
            timer = timer.total_seconds() * 1000
            transform_list.append({
                'Region': resp['region'],
                'City Name': resp.get('capital') if resp.get('capital') is not None else 'no_tiene',
                'Languaje': sha1_encrypt,
                'time': timer
            })
        return transform_list
    except:
        print('Error en el proceso de etl de la informacion')


def create_data_frame(etl_dictionary):
    try:
        df = pd.DataFrame(etl_dictionary)
        return df
    except:
        print('error en el proceso de convercion data frame')


def data_analytic(data_frame):
    total_data_frame = data_frame['time'].sum()
    mean_data_frame = data_frame['time'].mean()
    max_data_frame = data_frame['time'].max()
    min_data_frame = data_frame['time'].min()
    object_analytic = {'total_data_frame': total_data_frame, 'mean_data_frame': mean_data_frame,
                       'max_data_frame': max_data_frame, 'min_data_frame': min_data_frame}
    convert_analytic_results_json(json.dumps(object_analytic))
    return object_analytic


def convert_data_frame_json(data_frame):
    try:
        js = data_frame.to_json(orient='records')
        js = json.loads(js)
        with open('out_put/data.json', 'w') as file:
            json.dump(js, file)
            print('export data.json completed directory out_put/data.json')
    except:
        print('error en el proceso de excritura de data.json')


def convert_analytic_results_json(data):
    try:
        with open('out_put/data_analytic.json', 'w') as file:
            json.dump(json.loads(data), file)
            print('export data.json completed directory out_put/data_analytic.json')
    except:
        print('error en el proceso de excritura de data_analytic.json')
