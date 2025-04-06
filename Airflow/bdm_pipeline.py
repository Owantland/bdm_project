from airflow import DAG
import os
import pandas as pd
import chardet
import csv
import json
import time
import smtplib
from datetime import datetime, timedelta
from tqdm import tqdm
from azure.storage.blob import BlobServiceClient
import requests as req
from airflow.operators.python import PythonOperator
from airflow.models import XCom
from airflow.utils.trigger_rule import TriggerRule
# from airflow.sensors.filesystem import FileSensor

folder = 'event_data'
if not os.path.exists(folder):
    os.makedirs(folder)

folder_place = 'place_data'
if not os.path.exists(folder):
    os.makedirs(folder)

filepath_rome = os.path.join(folder, 'italian_event_rome.csv')
filepath_paris = os.path.join(folder, 'france_activity_paris.csv')
filepath_madrid = os.path.join(folder, 'spain_activity_madrid.csv')
filepath_barca = os.path.join(folder, 'spain_activity_barc.csv')

# Barcelona data path
BRCN_PATH = os.path.join(folder_place, 'bcr_cultural_places.csv')
    # Madrid data paths
MDR_MON_PATH = os.path.join(folder_place, 'madrid_monuments.csv')
MDR_MUS_PATH = os.path.join(folder_place, 'madrid_museums.csv')
MDR_THE_PATH = os.path.join(folder_place, 'madrid_theaters.csv')
MDR_CIN_PATH = os.path.join(folder_place, 'madrid_cinemas.csv')
MDR_CON_PATH = os.path.join(folder_place, 'madrid_concert_halls.csv')
    # Paris data paths
PRS_MUS_PATH = os.path.join(folder_place, 'paris_museums.csv')
PRS_MON_PATH = os.path.join(folder_place, 'paris_monuments.csv')
PRS_CUL_PATH = os.path.join(folder_place, 'paris_cultural_places.json')

def azure_connection():
    KEY_PATH = os.path.join('data', 'keys.csv')
    keys = pd.read_csv(KEY_PATH)
    os.environ["AZURE_CONNECTION_STRING"] = keys["AZURE_CONNECTION_STRING"][0]
    os.environ["RAPID_API_KEY"] = keys["RAPID_API_KEY"][0]
    os.environ["RAPID_API_HOST"] = keys["RAPID_API_HOST"][0]
    os.environ["OPENROUTE_KEY"] = keys["OPENROUTE_KEY"][0]

    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    client = BlobServiceClient.from_connection_string(connection_string)
    return client

###################### Make the two diffent function for failer task or complete task##########

def init_progress_task(**context):
    context['ti'].xcom_push(key='progress_summary', value=[])

def task_complete(context):
    ti = context['ti']
    task_id = ti.task_id
    progress = ti.xcom_pull(key='progress_summary', task_ids='init_progress_task') or []
    progress.append(f"-Task: {task_id}")
    XCom.set(
        key='progress_summary',
        value=progress,
        execution_date=ti.execution_date,
        task_id='init_progress_task',
        dag_id=ti.dag_id
    )
    ti.xcom_push(key='progress_summary', value=progress)

def task_failed(context):
    ti = context['ti']
    #is that mail decided to used as Recipient
    to = 'Recipient@mail.com'
    task_id = ti.task_id
    exception = context.get('exception')
    progress = ti.xcom_pull(key='progress_summary', task_ids='init_progress_task') or []
    failed = f"-Task: {task_id} FAILED for: {exception}"
    subject_email_recap = f"Task: {task_id} failed in Airflow"

    if len(progress) >= 2:
        body_email_recap = (
            "Airflow recap\n\n"
            "Tasks complete:\n"
            + "- " + "\n- ".join(progress)
            + "\nTask Failed:\n"
            + failed + "\n"
        )
    elif len(progress) == 1:
        body_email_recap = (
            "Airflow recap\n\n"
            "Task complete:\n"
            + "\n".join(progress)
            + "\n\nTask Failed:\n"
            + failed + "\n"
        )
    else:
        body_email_recap = (
            "Airflow recap\n\n"
            "Task Failed:\n"
            + failed + "\n"
        )
    
    send_email(subject_email_recap, body_email_recap, to=[to])
    progress.append(failed)
    XCom.set(
        key='progress_summary',
        value=progress,
        execution_date=ti.execution_date,
        task_id='init_progress_task',
        dag_id=ti.dag_id
    )
    ti.xcom_push(key='progress_summary', value=progress)

def send_email(subject, body, to):
    #dipend about the user used gmail or outlook mail and the port change
    smtp_port = 25     
    #dipend about the user used gmail or outlook mail ect..            
    smtp_server = 'smtp.user.com'
    #is that mail decided to used as Sender
    smtp_user = 'sender@mail.com'
    #password of the user (never share)
    smtp_password = 'password'
    sender = smtp_user 
    message = f"Subject: {subject}\n\n{body}"

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.ehlo()            
        server.starttls()        
        server.ehlo()            
        server.login(smtp_user, smtp_password)
        server.sendmail(sender, to, message)


def upload_blob(client, blob_name, local_file_path):
  container_name = "bdmcontainerp1"
  container_client = client.get_container_client(container_name)

  with open(local_file_path, "rb") as data:
      container_client.upload_blob(name=blob_name, data=data, overwrite=True)

  blobs = [blob.name for blob in container_client.list_blobs()]
  if blob_name not in blobs:
    raise Exception("Blob was not uploaded")

def import_events():
    # Connection to the Azure server
    try:
        client = azure_connection()
    except Exception as e:
        print("Error of connection to Azure:", e)
        raise
    # Event Rome
    try:
        url_event_rome = 'https://preprod.comune.roma.it/catalog9/it/datastore/dump/92699c41-7f59-4064-ba00-a3f2593037d0?bom=True'
        res_rome = req.get(url_event_rome)
        os.makedirs(os.path.dirname(filepath_rome), exist_ok=True)
        with open(filepath_rome, 'w') as file:
            file.write(res_rome.text)
        upload_blob(client, "events/rome_events.csv", filepath_rome)
        os.remove(filepath_rome)
        print("Rome event processed successfully.")
    except Exception as e:
        print("Error processing Rome event:", e)
        raise
    
    # Event Paris
    try:
        url_event_paris = 'https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/que-faire-a-paris-/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B'
        res_paris = req.get(url_event_paris)
        os.makedirs(os.path.dirname(filepath_paris), exist_ok=True)
        with open(filepath_paris, 'w') as file:
            file.write(res_paris.text)
        upload_blob(client, "events/paris_events.csv", filepath_paris)
        os.remove(filepath_paris)
        print("Paris event processed successfully.")
    except Exception as e:
        print("Error processing Paris event:", e)
        raise
    
    # Event Barcelona
    try:
        url_event_barca = 'https://opendata-ajuntament.barcelona.cat/data/dataset/2767159c-1c98-46b8-a686-2b25b40cb053/resource/3abb2414-1ee0-446e-9c25-380e938adb73/download'
        res_barca = req.get(url_event_barca)
        os.makedirs(os.path.dirname(filepath_barca), exist_ok=True)
        with open(filepath_barca, 'wb') as f:
            f.write(res_barca.content)
    
        # Rileva la codifica e decodifica il file
        with open(filepath_barca, 'rb') as f:
            rawdata = f.read()
            guess = chardet.detect(rawdata)
    
        encoding_to_use = guess['encoding'] if guess['encoding'] else 'utf-8-sig'
        content_text = rawdata.decode(encoding_to_use)
    
        with open(filepath_barca, 'w', encoding='utf-8') as f:
            f.write(content_text)
    
        upload_blob(client, "events/barcelona_events.csv", filepath_barca)
        os.remove(filepath_barca)
        print("Barcelona event processed successfully.")
    except Exception as e:
        print("Error processing Barcelona event:", e)
        raise
    
    # Event Madrid
    try:
        url_event_madrid = 'https://datos.madrid.es/egob/catalogo/300107-0-agenda-actividades-eventos.csv'
        res_madrid = req.get(url_event_madrid)
        os.makedirs(os.path.dirname(filepath_madrid), exist_ok=True)
        with open(filepath_madrid, 'w') as file:
            file.write(res_madrid.text)
        upload_blob(client, "events/madrid_events.csv", filepath_madrid)
        os.remove(filepath_madrid)
        print("Madrid event processed successfully.")
    except Exception as e:
        print("Error processing Madrid event:", e)
        raise

# Downloads and processes data from the Barcelona Opendata portal through the use of their API
def dwnld_brcn(client):
    offset = 0
    url = f'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=31431b23-d5b9-42b8-bcd0-a84da9d8c7fa&offset={offset}'
    r = req.get(url).json()
    r = r['result']
    lec_ini(r)
    offset+=100

    while offset <= 800:
        url = f'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=31431b23-d5b9-42b8-bcd0-a84da9d8c7fa&offset={offset}'
        r = req.get(url).json()
        r = r['result']
        lec_post(r)
        offset+=100

    upload_blob(client, f"cultural_places/brcn_cultural_places.csv", BRCN_PATH)
    os.remove(BRCN_PATH)

# Initial download and parsing of Barcelona data
def lec_ini(results):
    # Write into CSV file
    os.makedirs(os.path.dirname(BRCN_PATH), exist_ok=True)
    with open(BRCN_PATH, 'w', encoding='utf-8') as csvfile:
        fieldnames = ['_id', 'register_id', 'name', 'institution_id', 'institution_name', 'created', 'modified',
                      'addresses_roadtype_id', 'addresses_roadtype_name', 'addresses_road_id', 'addresses_road_name',
                      'addresses_start_street_number', 'addresses_end_street_number', 'addresses_neighborhood_id',
                      'addresses_neighborhood_name', 'addresses_district_id', 'addresses_district_name',
                      'addresses_zip_code', 'addresses_town', 'addresses_main_address', 'addresses_type', 'values_id',
                      'values_attribute_id', 'values_category', 'values_attribute_name', 'values_value',
                      'values_outstanding', 'values_description', 'geo_epgs_25831_x', 'geo_epgs_25831_y',
                      'geo_epgs_4326_lat', 'geo_epgs_4326_lon']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for record in results['records']:
            writer.writerow({
                '_id': record["_id"],
                'register_id': record["register_id"],
                'name': record["name"],
                'institution_id': record["institution_id"],
                'institution_name': record["institution_name"],
                'created': record["created"],
                'modified': record["modified"],
                'addresses_roadtype_id': record["addresses_roadtype_id"],
                'addresses_roadtype_name': record["addresses_roadtype_name"],
                'addresses_road_id': record["addresses_road_id"],
                'addresses_road_name': record["addresses_road_name"],
                'addresses_start_street_number': record["addresses_start_street_number"],
                'addresses_end_street_number': record["addresses_end_street_number"],
                'addresses_neighborhood_id': record["addresses_neighborhood_id"],
                'addresses_neighborhood_name': record["addresses_neighborhood_name"],
                'addresses_district_id': record["addresses_district_id"],
                'addresses_district_name': record["addresses_district_name"],
                'addresses_zip_code': record["addresses_zip_code"],
                'addresses_town': record["addresses_town"],
                'addresses_main_address': record["addresses_main_address"],
                'addresses_type': record["addresses_type"],
                'values_id': record["values_id"],
                'values_attribute_id': record["values_attribute_id"],
                'values_category': record["values_category"],
                'values_attribute_name': record["values_attribute_name"],
                'values_value': record["values_value"],
                'values_outstanding': record["values_outstanding"],
                'values_description': record["values_description"],
                'geo_epgs_25831_x': record["geo_epgs_25831_x"],
                'geo_epgs_25831_y': record["geo_epgs_25831_y"],
                'geo_epgs_4326_lat': record["geo_epgs_4326_lat"],
                'geo_epgs_4326_lon': record["geo_epgs_4326_lon"]
            })
    csvfile.close()
# Parsing of the Barcelona data after the first
def lec_post(results):
    # Write into CSV file
    os.makedirs(os.path.dirname(BRCN_PATH), exist_ok=True)
    with open(BRCN_PATH, 'a', encoding='utf-8') as csvfile:
        fieldnames = ['_id', 'register_id', 'name', 'institution_id', 'institution_name', 'created', 'modified',
                      'addresses_roadtype_id', 'addresses_roadtype_name', 'addresses_road_id', 'addresses_road_name',
                      'addresses_start_street_number', 'addresses_end_street_number', 'addresses_neighborhood_id',
                      'addresses_neighborhood_name', 'addresses_district_id', 'addresses_district_name',
                      'addresses_zip_code', 'addresses_town', 'addresses_main_address', 'addresses_type', 'values_id',
                      'values_attribute_id', 'values_category', 'values_attribute_name', 'values_value',
                      'values_outstanding', 'values_description', 'geo_epgs_25831_x', 'geo_epgs_25831_y',
                      'geo_epgs_4326_lat', 'geo_epgs_4326_lon']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        for record in results['records']:
            writer.writerow({
                '_id': record["_id"],
                'register_id': record["register_id"],
                'name': record["name"],
                'institution_id': record["institution_id"],
                'institution_name': record["institution_name"],
                'created': record["created"],
                'modified': record["modified"],
                'addresses_roadtype_id': record["addresses_roadtype_id"],
                'addresses_roadtype_name': record["addresses_roadtype_name"],
                'addresses_road_id': record["addresses_road_id"],
                'addresses_road_name': record["addresses_road_name"],
                'addresses_start_street_number': record["addresses_start_street_number"],
                'addresses_end_street_number': record["addresses_end_street_number"],
                'addresses_neighborhood_id': record["addresses_neighborhood_id"],
                'addresses_neighborhood_name': record["addresses_neighborhood_name"],
                'addresses_district_id': record["addresses_district_id"],
                'addresses_district_name': record["addresses_district_name"],
                'addresses_zip_code': record["addresses_zip_code"],
                'addresses_town': record["addresses_town"],
                'addresses_main_address': record["addresses_main_address"],
                'addresses_type': record["addresses_type"],
                'values_id': record["values_id"],
                'values_attribute_id': record["values_attribute_id"],
                'values_category': record["values_category"],
                'values_attribute_name': record["values_attribute_name"],
                'values_value': record["values_value"],
                'values_outstanding': record["values_outstanding"],
                'values_description': record["values_description"],
                'geo_epgs_25831_x': record["geo_epgs_25831_x"],
                'geo_epgs_25831_y': record["geo_epgs_25831_y"],
                'geo_epgs_4326_lat': record["geo_epgs_4326_lat"],
                'geo_epgs_4326_lon': record["geo_epgs_4326_lon"]
            })
    csvfile.close()

# Downloads and processes data from the Madrid Opendata portal through the use of their API
def dwnld_madrid(client):
    # URLs for datasets
    url_monument = 'https://datos.madrid.es/egob/catalogo/208844-0-monumentos-edificios.csv'
    url_museums = 'https://datos.madrid.es/egob/catalogo/201132-0-museos.csv'
    url_theaters = 'https://datos.madrid.es/egob/catalogo/208862-7650046-ocio_salas.csv'
    url_cinemas = 'https://datos.madrid.es/egob/catalogo/208862-7650164-ocio_salas.csv'
    url_concert_halls = 'https://datos.madrid.es/egob/catalogo/208862-7650180-ocio_salas.csv'

    # Download and upload Madrid monument data
    os.makedirs(os.path.dirname(MDR_MON_PATH), exist_ok=True)
    with open(MDR_MON_PATH, 'wb') as f, \
            req.get(url_monument, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/madrid_monuments.csv", MDR_MON_PATH)
    os.remove(MDR_MON_PATH)

    # Download and upload Madrid museum data
    os.makedirs(os.path.dirname(MDR_MUS_PATH), exist_ok=True)
    with open(MDR_MUS_PATH, 'wb') as f, \
            req.get(url_museums, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/madrid_museums.csv", MDR_MUS_PATH)
    os.remove(MDR_MUS_PATH)

    # Download and then upload Madrid theaters data
    os.makedirs(os.path.dirname(MDR_THE_PATH), exist_ok=True)
    with open(MDR_THE_PATH, 'wb') as f, \
            req.get(url_theaters, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/madrid_theaters.csv", MDR_THE_PATH)
    os.remove(MDR_THE_PATH)

    # Download and then upload Madrid Cinema data
    os.makedirs(os.path.dirname(MDR_CIN_PATH), exist_ok=True)
    with open(MDR_CIN_PATH, 'wb') as f, \
            req.get(url_cinemas, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/madrid_cinemas.csv", MDR_CIN_PATH)
    os.remove(MDR_CIN_PATH)

    # Download and then upload Madrid Concert Hall data
    os.makedirs(os.path.dirname(MDR_CON_PATH), exist_ok=True)
    with open(MDR_CON_PATH, 'wb') as f, \
            req.get(url_concert_halls, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/madrid_concert_halls.csv", MDR_CON_PATH)
    os.remove(MDR_CON_PATH)
# Downloads and processes data from the Paris Opendata portal through the use of their API
def dwnld_paris(client):
    url_museums = 'https://data.culture.gouv.fr/api/explore/v2.1/catalog/datasets/musees-de-france-base-museofile/exports/csv?delimiter=%3B&list_separator=%2C&quote_all=false&with_bom=true'
    url_monuments = 'https://data.culture.gouv.fr/api/explore/v2.1/catalog/datasets/musees-de-france-base-museofile/exports/csv?delimiter=%3B&list_separator=%2C&quote_all=false&with_bom=true'
    url_cultural_places = 'https://tabular-api.data.gouv.fr/api/resources/ae3fc35e-604f-4b2e-85fd-f5354bace2c0/data/?page=2'

    # Download datasets
    os.makedirs(os.path.dirname(PRS_MUS_PATH), exist_ok=True)
    with open(PRS_MUS_PATH, 'wb') as f, \
            req.get(url_museums, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/paris_museums.csv", PRS_MUS_PATH)
    os.remove(PRS_MUS_PATH)

    os.makedirs(os.path.dirname(PRS_MON_PATH), exist_ok=True)
    with open(PRS_MON_PATH, 'wb') as f, \
            req.get(url_monuments, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/paris_monuments.csv", PRS_MON_PATH)
    os.remove(PRS_MON_PATH)

    while True:
        r = req.get(url_cultural_places).json()
        data = r['data']

        with open(PRS_CUL_PATH, "a") as outfile:
            json.dump(r, outfile)
            outfile.close()

        # Find the next page of data
        url_cultural_places = r["links"]['next']
        if url_cultural_places is None:
            break
    upload_blob(client, f"cultural_places/paris_cultural_places.json", PRS_CUL_PATH)
    os.remove(PRS_CUL_PATH)


def import_main_point():
    # Connection to the Azure server
    try:
        client = azure_connection()
    except Exception as e:
        print("Error of connection to Azure:", e)
    try:
        dwnld_brcn(client)
        dwnld_madrid(client)
        dwnld_paris(client)
        print("import_main_point completed successfully")
    except Exception as e:
        print(f"ERROR in import main point: {e}")
        raise

def save_images(res, client, querystring, city):
    hotels = res['data']['hotels']
    for hotel in hotels:
        photos = hotel['property']['photoUrls']
        for photo in photos:
            img_res = req.get(photo, stream=True)
            filename = city + '/' + str(hotel['hotel_id']) + '_' + querystring['dest_id'] + '_' + querystring['arrival_date'] + '_' + querystring['departure_date'] + '.jpg'
            if img_res.status_code == 200:
                os.makedirs(os.path.dirname('accomodation_images/' + filename), exist_ok=True)
                with open('accomodation_images/' + filename, "wb") as file:
                    for chunk in img_res.iter_content(1024):  # Guardar en bloques de 1024 bytes
                        file.write(chunk)
            upload_blob(client, f'accomodation_images/{filename}', f'accomodation_images/{filename}')
            os.remove(f'accomodation_images/{filename}')

def save_json(res, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as file:
        json.dump(res, file)


def import_accomodation_and_weather_info():
    client = azure_connection()
    accomodation_endpoint = "https://booking-com15.p.rapidapi.com/api/v1/hotels/searchHotels"
    weather_endpoint = f'https://api.open-meteo.com/v1/forecast'

    delta = timedelta(days=1)

    destination_ids = {
        "Barcelona": "-372490",
        "Rome": "-126693",
        "Madrid": "-390625",
        "Paris": "-1456928"
    }

    headers = {
                "x-rapidapi-key": os.environ["RAPID_API_KEY"],
                "x-rapidapi-host": os.environ["RAPID_API_HOST"]
            }

    accomodation_querystring = {
                "dest_id": '',
                "search_type": "CITY",
                "arrival_date": '',
                "departure_date": '',
                "adults": "2",
                "children_age": "0",
                "room_qty": "1",
                "page_number": "1",
                "units": "metric",
                "temperature_unit": "c",
                "languagecode": "en-us",
                "currency_code": "EUR"
            }

    destination_coords = {
                    'Barcelona': {'latitude': 41.3874, 'longitude': 2.1686},
                    'Paris': {'latitude': 48.8575, 'longitude': 2.3514},
                    'Madrid': {'latitude': 40.4167, 'longitude': 3.7033},
                    'Rome': {'latitude': 41.8967, 'longitude': 12.4822}
                    }

    weather_metrics = 'temperature_2m,precipitation_probability,apparent_temperature,precipitation,cloud_cover,wind_speed_10m'
    weather_querystring = {
                    'latitude': '',
                    'longitude': '',
                    'hourly': weather_metrics,
                    'start_date': '',
                    'end_date': ''
                    }

    start = datetime.strptime('2025-06-05', "%Y-%m-%d")
    end = datetime.strptime('2025-06-06', "%Y-%m-%d")
    last_city = 'Rome'
    try:
        for single_date in tqdm([start + i * delta for i in range((end - start).days + 1)]):
            
            arrival_date = single_date.strftime("%Y-%m-%d")
            departure_date = (single_date + timedelta(days=1)).strftime("%Y-%m-%d")
            start_date = single_date.replace(year=single_date.year - 1).strftime("%Y-%m-%d")
            end_date = (single_date + timedelta(days=1)).replace(year=single_date.year - 1).strftime("%Y-%m-%d")

            for city, city_id in destination_ids.items():
                if last_city and city != last_city:
                    continue
                print(city)
                accomodation_querystring['dest_id'] = city_id
                accomodation_querystring['arrival_date'] = arrival_date
                accomodation_querystring['departure_date'] = departure_date
                
                accomodation_res = req.get(accomodation_endpoint, headers=headers, params=accomodation_querystring)
                if accomodation_res.status_code == 200:
                    print("Accomodation data retrieved successfully")
                    accomodation_res = accomodation_res.json()
                    save_images(accomodation_res, client, accomodation_querystring, city)
                    json_res_filename =  city + '/' + accomodation_querystring['arrival_date'] + '_' + accomodation_querystring['departure_date'] + '.json'
                    save_json(accomodation_res, f'accomodation/{json_res_filename}')
                    upload_blob(client, f'accomodation/{json_res_filename}', f'accomodation/{json_res_filename}')
                    os.remove(f'accomodation/{json_res_filename}')
                else:
                    print('Accomodation', city, arrival_date, departure_date, accomodation_res)
                    last_city = city
                    raise

                latitude, longitude = destination_coords[city].values()
                weather_querystring['latitude'] = latitude
                weather_querystring['longitude'] = longitude
                weather_querystring['start_date'] = start_date
                weather_querystring['end_date'] = start_date
                weather_json_filename = city + '/' + start_date + '.json'
                
                weather_res = req.get(weather_endpoint, params=weather_querystring)
                if weather_res.status_code == 200:
                    print("Weather data retrieved successfully")
                    weather_res = weather_res.json()
                    save_json(weather_res, f'weather/{weather_json_filename}')
                    upload_blob(client, f'weather/{weather_json_filename}', f'weather/{weather_json_filename}')
                    os.remove(f'weather/{weather_json_filename}')
                else:
                    print('Weather', city, start_date, end_date, weather_res)
                    last_city = city
                    raise
                
                if last_city:
                    last_city = None

            if single_date >= end:
                break
                

    except Exception as e:
        print(e)
        print(last_city, arrival_date, departure_date)

def recap_task_complete(**context):
    ti = context['ti']
    #is that mail decided to used as Recipient
    to = 'Recipient@mail.com'
    dag_run = context.get('dag_run')
    progress = ti.xcom_pull(key='progress_summary', task_ids='init_progress_task') or []

    subject_email_recap = f"DAG: {dag_run.dag_id} Complete in Airflow!"
    body_email_recap = (
        f"DAG complete: {dag_run.dag_id}\n\n"
        f"List of completed tasks:\n"
        + "\n".join(progress)
        + "\n\n"
        "All task is complete!"
    )
    send_email(subject_email_recap, body_email_recap, to=[to])


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'depends_on_past': False,
}

with DAG(
    dag_id='bdm_data_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['bdm', 'events', 'data'],
) as dag:

    init_progress = PythonOperator(
        task_id='init_progress_task',
        python_callable=init_progress_task
    )

    accomodation_and_weather = PythonOperator(
        task_id='import_accomodation_and_weather_info',
        python_callable=import_accomodation_and_weather_info,
        on_success_callback=task_complete,
        on_failure_callback=task_failed
    )

    main_point = PythonOperator(
        task_id='import_main_point',
        python_callable=import_main_point,
        on_success_callback=task_complete,
        on_failure_callback=task_failed
    )

    events = PythonOperator(
        task_id='import_events',
        python_callable=import_events,
        on_success_callback=task_complete,
        on_failure_callback=task_failed
    )

    recap = PythonOperator(
        task_id='recap_task_complete',
        python_callable=recap_task_complete,
        trigger_rule=TriggerRule.ALL_DONE
    )

    init_progress >> events>> main_point >> accomodation_and_weather >> recap
