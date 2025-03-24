import requests
import json

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
