import os
import json

from numpy.distutils.conv_template import header
from tqdm import tqdm
import requests
from pyspark.sql import SparkSession
import pyspark.pandas as ps
import numpy as np
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import csv
from neo4j import GraphDatabase
from session_helper_neo4j import create_session, clean_session

"""
    CONSTANTS
"""
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
# Cultural places
CUL_PLAC_PATH = os.path.join('data', 'cultural_places')
# Neo4J CSV paths
N4J_PATH = os.path.join('data', 'sdm_analysis')

# List of cities
filenames = ["brcn_cultural_places.csv", "madrid_monuments.csv", "madrid_museums.csv", "madrid_theaters.csv", "madrid_cinemas.csv",
             "madrid_concert_halls.csv", "paris_museums.csv", "paris_monuments.csv","paris_cultural_places.json"]
file_paths = [BRCN_PATH, MDR_MON_PATH, MDR_MUS_PATH,
              MDR_THE_PATH, MDR_CIN_PATH, MDR_CON_PATH,
              PRS_MUS_PATH, PRS_MON_PATH, PRS_CUL_PATH]
landing_path = f"landing_zone/cultural_places/"
trusted_path = f"trusted_zone/cultural_places/"

"""
    FUNCTIONS
"""
"""
    Landing Zone Functions
"""
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

    lp = landing_path + "brcn_cultural_places.csv"
    with open(file=BRCN_PATH, mode="rb") as data:
        upload_file(client, lp, data)
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
            l = clean_line(line)
            f.write(l.encode())
        f.close()

    lp = landing_path + "madrid_monuments.csv"
    with open(file=MDR_MON_PATH, mode="rb") as data:
        upload_file(client, lp, data)
        data.close()
    # os.remove(MDR_MON_PATH)

    # Download and upload Madrid museum data
    with open(MDR_MUS_PATH, 'wb') as f, \
            requests.get(url_museums, stream=True) as r:
        for line in r.iter_lines():
            l = clean_line(line)
            f.write(l.encode())
        f.close()

    lp = landing_path + "madrid_museums.csv"
    with open(file=MDR_MUS_PATH, mode="rb") as data:
        upload_file(client, lp, data)
        data.close()
    os.remove(MDR_MUS_PATH)

    # Download and then upload Madrid theaters data
    with open(MDR_THE_PATH, 'wb') as f, \
            requests.get(url_theaters, stream=True) as r:
        for line in r.iter_lines():
            l = clean_line(line)
            f.write(l.encode())
        f.close()

    lp = landing_path + "madrid_theaters.csv"
    with open(file=MDR_THE_PATH, mode="rb") as data:
        upload_file(client, lp, data)
        data.close()
    os.remove(MDR_THE_PATH)

    # Download and then upload Madrid Cinema data
    with open(MDR_CIN_PATH, 'wb') as f, \
            requests.get(url_cinemas, stream=True) as r:
        for line in r.iter_lines():
            l = clean_line(line)
            f.write(l.encode())
        f.close()

    lp = landing_path + "madrid_cinemas.csv"
    with open(file=MDR_CIN_PATH, mode="rb") as data:
        upload_file(client, lp, data)
        data.close()
    os.remove(MDR_CIN_PATH)

    # Download and then upload Madrid Concert Hall data
    with open(MDR_CON_PATH, 'wb') as f, \
            requests.get(url_concert_halls, stream=True) as r:
        for line in r.iter_lines():
            l = clean_line(line)
            f.write(l.encode())
        f.close()

    lp = landing_path + "madrid_concert_halls.csv"
    with open(file=MDR_CON_PATH, mode="rb") as data:
        upload_file(client, lp, data)
        data.close()
    # os.remove(MDR_CON_PATH)


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

    lp = landing_path + "paris_museums.csv"
    with open(file=PRS_MUS_PATH, mode="rb") as data:
        upload_file(client, lp, data)
        data.close()
    os.remove(PRS_MUS_PATH)

    with open(PRS_MON_PATH, 'wb') as f, \
            requests.get(url_monuments, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()

    lp = landing_path + "paris_monuments.csv"
    with open(file=PRS_MON_PATH, mode="rb") as data:
        upload_file(client, lp, data)
        data.close()
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

    lp = landing_path + "paris_cultural_places.json"
    with open(file=PRS_CUL_PATH, mode="rb") as data:
        upload_file(client, lp, data)
        data.close()
    os.remove(PRS_CUL_PATH)


"""
    Helper Functions
"""
# Defines the settings for the connection to the azure data lake
def azure_connection():
    storage_container_name = 'bdmcontainerp1'
    keys = pd.read_csv(KEY_PATH)
    os.environ["AZURE_CONNECTION_STRING"] = keys["AZURE_CONNECTION_STRING"][0]
    os.environ["RAPID_API_KEY"] = keys["RAPID_API_KEY"][0]
    os.environ["RAPID_API_HOST"] = keys["RAPID_API_HOST"][0]
    os.environ["OPENROUTE_KEY"] = keys["OPENROUTE_KEY"][0]

    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    datalake_client = DataLakeServiceClient.from_connection_string(connection_string)
    file_system_client = datalake_client.get_file_system_client(storage_container_name)
    return file_system_client

# Creates Spark connection
def spark_connection():
    spark = SparkSession.builder \
        .appName("BDMProject") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    return spark

# Upload a file to the Azure Datalake
def upload_file(fs_client, path: str, data, overwrite=True) -> None:
    file_client = fs_client.get_file_client(path)
    if overwrite and file_client.exists():
        file_client.delete_file()
    file_client.create_file()
    file_client.upload_data(data, overwrite=True)

# Checks if the file exists in Azure Datalake
def file_exists(fs_client, path: str) -> bool:
    try:
        file_client = fs_client.get_file_client(path)
        return file_client.exists()
    except Exception:
        return False


"""
    Trusted Zone Functions
"""
def clean_line(line):
    l = line.decode('ISO-8859-1')
    l = l.replace('"', '')
    l = l.replace('&amp;', '&')
    l = l.replace('&Ntilde;', 'ñ')
    l = l + '\n'
    return l


def clean_coordinates(cord):
    try:
        cord = round(cord, 0)
    except TypeError:
        cord = int(cord)
        cord = round(cord, 0)
    cord = int(cord)
    return cord


def get_x_coordinates(cord_col):
    cord = str(cord_col)
    sep = cord.find(",")
    cord = cord[0:sep]

    try:
        cord = float(cord) * 10000000
    except ValueError:
        cord = np.nan
    return cord


def get_y_coordinates(cord_col):
    cord = str(cord_col)
    sep = cord.find(",")
    cord = cord[sep + 2::]

    try:
        cord = float(cord) * 10000000
    except ValueError:
        cord = np.nan
    return cord


def clean_zip(code):
    code = str(code)
    while len(code) < 5:
        code = "0" + code
    return code


def clean_text(text):
    text = text.lower()
    # Replace symbols
    text = text.replace(".", "")
    text = text.replace("-", "")
    text = text.replace("  ", " ")
    # Replace special characters
    text = text.replace("á", "a")
    text = text.replace("é", "e")
    text = text.replace("í", "i")
    text = text.replace("ó", "o")
    text = text.replace("ú", "u")
    return text


def clean_df(df, zip_col, text_cols:list, cord_cols:list, city):
    # Ensure the zip code is a string with 5 characters and drop all rows that have
    # problematic values
    df["zip_code"] = df[zip_col].apply(clean_zip)
    df[zip_col] = ps.to_numeric(df[zip_col], errors='coerce')
    df.dropna(inplace=True)
    df = df.drop([zip_col], axis=1)

    # Clean text columns
    for col in text_cols:
        df[col] = df[col].apply(clean_text)

    # Clean coordinates
    for cord_col in cord_cols:
        df[cord_col] = df[cord_col].apply(clean_coordinates)

    # Assign city
    df['city'] = city
    return df


def upload_cultural_places(client, path):
    for (root, dirs, file) in os.walk(path):
        for f in file:
            if '.csv' in f:
                path_f = os.path.join(path, f)
                tp = trusted_path + 'cultural_places_csv/' + f

                with open(file=path_f, mode="rb") as data:
                    upload_file(client, tp, data)
                os.remove(path_f)
                print(tp)


def process_cultural_places():
    col_names = ["ID", "Name", "zip_code",
                 "address", "x_coordinate",
                 "y_coordinate", "city"]

    # BCR
    df = ps.read_csv(BRCN_PATH)
    cols = ["_id", "name", "addresses_zip_code",
            "addresses_road_name", "geo_epgs_25831_x", "geo_epgs_25831_y"]
    text_cols = ["name", "addresses_road_name"]
    cord_cols = ["geo_epgs_25831_x", "geo_epgs_25831_y"]
    df = df[cols]
    df_bcr = clean_df(df, "addresses_zip_code", text_cols, cord_cols, 'Barcelona')
    df_bcr = df_bcr[["_id", "name", "zip_code",
                     "addresses_road_name", "geo_epgs_25831_x",
                     "geo_epgs_25831_y", "city"]]
    df_bcr.rename(columns={"_id": col_names[0],
                           "name": col_names[1],
                           "zip_code": col_names[2],
                           "addresses_road_name": col_names[3],
                           "geo_epgs_25831_x": col_names[4],
                           "geo_epgs_25831_y":col_names[5],
                           "city":col_names[6]}, inplace=True)
    print(df_bcr.head(5))

    # Madrid
    madrid_dfs = []
    for file in file_paths[1:6]:
        df = ps.read_csv(file, sep=";")
        cols = ["PK", "NOMBRE", "CODIGO-POSTAL",
                "NOMBRE-VIA", "COORDENADA-X", "COORDENADA-Y"]
        text_cols = ["NOMBRE", "NOMBRE-VIA"]
        cord_cols = ["COORDENADA-X", "COORDENADA-Y"]
        df = df[cols]
        df = clean_df(df, "CODIGO-POSTAL", text_cols, cord_cols, "Madrid")
        df = df[["PK", "NOMBRE", "zip_code",
                 "NOMBRE-VIA", "COORDENADA-X",
                 "COORDENADA-Y", "city"]]
        df.rename(columns={"PK": col_names[0],
                           "NOMBRE": col_names[1],
                           "zip_code": col_names[2],
                           "NOMBRE-VIA": col_names[3],
                           "COORDENADA-X": col_names[4],
                           "COORDENADA-Y": col_names[5],
                           "city": col_names[6]}, inplace=True)
        print(df.head(5))
        madrid_dfs.append(df)

    #Paris
    df = ps.read_csv(PRS_MUS_PATH, sep=";")
    cols = ["identifiant", "nom_officiel", "code_postal", "adresse", "coordonnees"]
    text_cols = ["nom_officiel", "adresse"]
    cord_cols = ["x_coordinates", "y_coordinates"]

    df = df[cols]
    df["x_coordinates"] = df['coordonnees'].apply(get_x_coordinates)
    df["y_coordinates"] = df['coordonnees'].apply(get_y_coordinates)

    df.dropna(inplace=True)
    df = df.drop(['coordonnees'], axis=1)
    df = clean_df(df, "code_postal", text_cols, cord_cols, "Paris")
    df_paris = df[["identifiant", "nom_officiel", "zip_code", "adresse","x_coordinates","y_coordinates", "city"]]
    df_paris.rename(columns={"identifiant": col_names[0],
                             "nom_officiel": col_names[1],
                             "zip_code": col_names[2],
                             "adresse": col_names[3],
                             "x_coordinates": col_names[4],
                             "y_coordinates": col_names[5],
                             "city": col_names[6]}, inplace=True)
    print(df_paris.head(5))

    df = ps.concat([df_bcr, df_paris])
    for file in madrid_dfs:
        df = ps.concat([df, file])

    df.to_csv(CUL_PLAC_PATH)


"""
    Exploitation Zone Functions
"""
def download_files_from_directory(file_system_client, directory_name, destination_path):
    paths = file_system_client.get_paths(path=directory_name)
    for path in paths:
        f_name = path.name
        file_client = file_system_client.get_file_client(path.name)
        sep = f_name.rfind("/") + 1
        f_name = f_name[sep::]

        with  open(destination_path + "/" + f_name, 'wb') as local_file:
            download = file_client.download_file()
            local_file.write(download.readall())
            local_file.close()


def download_to_local(fs, directory_name: str, destination_path: str):
    directory_client = fs.get_directory_client(directory_name)
    os.makedirs(destination_path, exist_ok=True)
    download_files_from_directory(fs, directory_name, destination_path)


def clean_accommodations():
    cols = ["property_id", "property_latitude", "property_longitude",
            "property_name", "city_code_name", "property_postalcode"]
    path = N4J_PATH + "/accommodation.csv"
    path2 = N4J_PATH + "/accommodation_fix.csv"

    df = pd.read_csv(path)

    df = df[cols]
    df = df.drop_duplicates()
    df["property_postalcode"] = df["property_postalcode"].apply(clean_zip)

    df.to_csv(path2)

# Neo4J processes
# Load Nodes
def load_bcn_zips(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/barcelona_codes.csv' AS line
        CREATE (:ZIPCODE{
            ZIPCODE: line.POSTAL_CODE,
            CITY: "Barcelona"
        })
        """
    )

def load_mad_zips(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/madrid_codes.csv' AS line
        CREATE (:ZIPCODE{
            ZIPCODE: line.POSTAL_CODE,
            CITY: "Madrid"
        })
        """
    )

def load_prs_zips(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/paris_codes.csv' AS line
        CREATE (:ZIPCODE{
            ZIPCODE: line.POSTAL_CODE,
            CITY: "Paris"
        })
        """
    )

def load_node_accommodations(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/accommodation.csv' AS line
        CREATE (:ACCOMMODATION {
            ID: line.property_id,
            NAME: line.property_name,
            X_POS: line.property_longitude,
            Y_POS: line.property_latitude,
            CITY: line.city_code_name
        })
        """
    )

# Load Edges
def load_rel_bcn_zip(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/barcelona_distances.csv' AS line
        MATCH (p1:ZIPCODE {ZIPCODE: line.COD_POSTAL_1})
        MATCH (p2:ZIPCODE {ZIPCODE: line.COD_POSTAL_2})
        CREATE (p1)-[:ROUTE {
            Distance: toFloat(line.distance)
        }]->(p2)
        """
    )

def load_rel_mad_zip(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/madrid_distances.csv' AS line
        MATCH (p1:ZIPCODE {ZIPCODE: line.COD_POSTAL_1})
        MATCH (p2:ZIPCODE {ZIPCODE: line.COD_POSTAL_2})
        CREATE (p1)-[:ROUTE {
            Distance: toFloat(line.distance)
        }]->(p2)
        """
    )

def load_rel_prs_zip(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/paris_distances.csv' AS line
        MATCH (p1:ZIPCODE {ZIPCODE: line.COD_POSTAL_1})
        MATCH (p2:ZIPCODE {ZIPCODE: line.COD_POSTAL_2})
        CREATE (p1)-[:ROUTE {
            Distance: toFloat(line.distance)
        }]->(p2)
        """
    )

def load_rel_acom_zip(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/accommodation_zips.csv' AS line
        MATCH (p1:ACCOMMODATION {ID: line.property_id})
        MATCH (p2:ZIPCODE {ZIPCODE: line.property_postalcode})
        CREATE (p1)-[:LOCATEDIN]->(p2)
        """
    )

def prepare_csv():
    session = create_session()
    session = clean_session(session)

    print("Loading nodes...")
    session.execute_write(load_node_accommodations)
    session.execute_write(load_bcn_zips)
    session.execute_write(load_mad_zips)
    session.execute_write(load_prs_zips)

    print("Loading edges...")
    session.execute_write(load_rel_bcn_zip)
    session.execute_write(load_rel_mad_zip)
    session.execute_write(load_rel_prs_zip)
    session.execute_write(load_rel_acom_zip)

    session.close()



def get_and_sync_cultural_places(fs, filenames):
    # Repeat the process for each file
    for i, file in tqdm(enumerate(filenames)):
        # Prepare the paths
        lp = landing_path + file
        file_path = file_paths[i]

        """
            Landing Zone
        """
        # If file exists in landing zone move on, else reprocess
        if file_exists(fs, lp):
            file_client = fs.get_file_client(lp)
            with open(file_path, mode="wb") as local_file:
                download = file_client.download_file()
                local_file.write(download.readall())
                local_file.close()
        elif file == filenames[0]:
            dwnld_brcn(fs)
        elif file in filenames[1:6]:
            dwnld_madrid(fs)
        else:
            dwnld_paris(fs)

    """
        Trusted Zone
    """
    # If file exists in the trusted zone download it for use in future operations
    tp = trusted_path + 'cultural_places_csv'
    if file_exists(fs, tp):
        # download_to_local(fs, tp, CUL_PLAC_PATH)
        pass
    else:
        process_cultural_places()
        upload_cultural_places(fs, CUL_PLAC_PATH)


def main():
    client = azure_connection()
    spark = spark_connection()

    get_and_sync_cultural_places(client, filenames)

    # Perform loading into Neo4J for graph data analysis
    prepare_csv()

if __name__ == '__main__':
    main()
