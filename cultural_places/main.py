# Import the necessary libraries
import requests
import csv
import json
import time
import pandas as pd
from azure.storage.blob import BlobServiceClient
import os

# Sets up important paths for functions
os.makedirs('data', exist_ok=True)
KEY_PATH = os.path.join('data', 'keys.csv')
# Barcelona data path
BRCN_PATH = os.path.join('data', 'bcr_cultural_places.csv')
# Madrid data paths
MDR_MON_PATH = os.path.join('data', 'madrid_monuments.csv')
MDR_MUS_PATH = os.path.join('data', 'madrid_museums.csv')
MDR_THE_PATH = os.path.join('data', 'madrid_theaters.csv')
MDR_CIN_PATH = os.path.join('data', 'madrid_cinemas.csv')
MDR_CON_PATH = os.path.join('data', 'madrid_concert_halls.csv')
# Paris data paths
PRS_MUS_PATH = os.path.join('data', 'paris_museums.csv')
PRS_MON_PATH = os.path.join('data', 'paris_monuments.csv')
PRS_CUL_PATH = os.path.join('data', 'paris_cultural_places.json')


# Downloads and processes data from the Barcelona Opendata portal through the use of their API
def dwnld_brcn(client):
    offset = 0
    url = f'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=31431b23-d5b9-42b8-bcd0-a84da9d8c7fa&offset={offset}'
    r = requests.get(url).json()
    r = r['result']
    lec_ini(r)
    offset+=100

    while offset <= 800:
        url = f'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=31431b23-d5b9-42b8-bcd0-a84da9d8c7fa&offset={offset}'
        r = requests.get(url).json()
        r = r['result']
        lec_post(r)
        offset+=100

    upload_blob(client, f"cultural_places/brcn_cultural_places.csv", BRCN_PATH)
    os.remove(BRCN_PATH)


# Initial download and parsing of Barcelona data
def lec_ini(results):
    # Write into CSV file
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
    with open(MDR_MON_PATH, 'wb') as f, \
            requests.get(url_monument, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/madrid_monuments.csv", MDR_MON_PATH)
    os.remove(MDR_MON_PATH)

    # Download and upload Madrid museum data
    with open(MDR_MUS_PATH, 'wb') as f, \
            requests.get(url_museums, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/madrid_museums.csv", MDR_MUS_PATH)
    os.remove(MDR_MUS_PATH)

    # Download and then upload Madrid theaters data
    with open(MDR_THE_PATH, 'wb') as f, \
            requests.get(url_theaters, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/madrid_theaters.csv", MDR_THE_PATH)
    os.remove(MDR_THE_PATH)

    # Download and then upload Madrid Cinema data
    with open(MDR_CIN_PATH, 'wb') as f, \
            requests.get(url_cinemas, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/madrid_cinemas.csv", MDR_CIN_PATH)
    os.remove(MDR_CIN_PATH)

    # Download and then upload Madrid Concert Hall data
    with open(MDR_CON_PATH, 'wb') as f, \
            requests.get(url_concert_halls, stream=True) as r:
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
    with open(PRS_MUS_PATH, 'wb') as f, \
            requests.get(url_museums, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/paris_museums.csv", PRS_MUS_PATH)
    os.remove(PRS_MUS_PATH)

    with open(PRS_MON_PATH, 'wb') as f, \
            requests.get(url_monuments, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()
    upload_blob(client, f"cultural_places/paris_monuments.csv", PRS_MON_PATH)
    os.remove(PRS_MON_PATH)

    while True:
        r = requests.get(url_cultural_places).json()
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


# Defines the settings for the connection to the azure data lake
def azure_connection():
    keys = pd.read_csv(KEY_PATH)
    os.environ["AZURE_CONNECTION_STRING"] = keys["AZURE_CONNECTION_STRING"][0]
    os.environ["RAPID_API_KEY"] = keys["RAPID_API_KEY"][0]
    os.environ["RAPID_API_HOST"] = keys["RAPID_API_HOST"][0]
    os.environ["OPENROUTE_KEY"] = keys["OPENROUTE_KEY"][0]

    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    return blob_service_client

# Uploads the data file as a blob to the azure data lake
def upload_blob(client, blob_name, local_file_path):
  container_name = "bdmcontainerp1"
  container_client = client.get_container_client(container_name)

  with open(local_file_path, "rb") as data:
      container_client.upload_blob(name=blob_name, data=data, overwrite=True)

  blobs = [blob.name for blob in container_client.list_blobs()]
  if blob_name not in blobs:
    raise Exception("Blob was not uploaded")

def main():
    client = azure_connection()
    dwnld_brcn(client)
    dwnld_madrid(client)
    dwnld_paris(client)

if __name__ == '__main__':
    main()