# Import the necessary libraries
import requests
import csv
import json
import time


def dwnld_bcrn():
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


def lec_ini(results):
    # Write into CSV file
    with open('bcr_cultural_places.csv', 'w', encoding='utf-8') as csvfile:
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


def lec_post(results):
    # Write into CSV file
    with open('bcr_cultural_places.csv', 'a', encoding='utf-8') as csvfile:
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


def dwnld_madrid():
    # URLs for datasets
    url_monument = 'https://datos.madrid.es/egob/catalogo/208844-0-monumentos-edificios.csv'
    url_museums = 'https://datos.madrid.es/egob/catalogo/201132-0-museos.csv'
    url_theaters = 'https://datos.madrid.es/egob/catalogo/208862-7650046-ocio_salas.csv'
    url_cinemas = 'https://datos.madrid.es/egob/catalogo/208862-7650164-ocio_salas.csv'
    url_concert_halls = 'https://datos.madrid.es/egob/catalogo/208862-7650180-ocio_salas.csv'

    # Download datasets
    with open("madrid_monuments.csv", 'wb') as f, \
            requests.get(url_monument, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()

    with open("madrid_museums.csv", 'wb') as f, \
            requests.get(url_museums, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()

    with open("madrid_theaters.csv", 'wb') as f, \
            requests.get(url_theaters, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()

    with open("madrid_cinemas.csv", 'wb') as f, \
            requests.get(url_cinemas, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()

    with open("madrid_concert_halls.csv", 'wb') as f, \
            requests.get(url_concert_halls, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()


def dwnld_paris():
    url_museums = 'https://data.culture.gouv.fr/api/explore/v2.1/catalog/datasets/musees-de-france-base-museofile/exports/csv?delimiter=%3B&list_separator=%2C&quote_all=false&with_bom=true'
    url_monuments = 'https://data.culture.gouv.fr/api/explore/v2.1/catalog/datasets/musees-de-france-base-museofile/exports/csv?delimiter=%3B&list_separator=%2C&quote_all=false&with_bom=true'
    url_cultural_places = 'https://tabular-api.data.gouv.fr/api/resources/ae3fc35e-604f-4b2e-85fd-f5354bace2c0/data/?page=2'

    # Download datasets
    with open("paris_museums.csv", 'wb') as f, \
            requests.get(url_museums, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()

    with open("paris_monuments.csv", 'wb') as f, \
            requests.get(url_monuments, stream=True) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())
        f.close()

    while True:
        r = requests.get(url_cultural_places).json()
        data = r['data']

        with open("paris_cultural_places.json", "a") as outfile:
            json.dump(r, outfile)
            outfile.close()

        # Find the next page of data
        url_cultural_places = r["links"]['next']
        if url_cultural_places is None:
            break


def main():
    dwnld_bcrn()
    dwnld_madrid()
    dwnld_paris()

main()