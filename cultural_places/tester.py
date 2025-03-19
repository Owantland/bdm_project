# Import the necessary libraries
import requests
import csv
import json
import time


offset = 0
url = f'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=31431b23-d5b9-42b8-bcd0-a84da9d8c7fa&offset={offset}'
r = requests.get(url).json()
r = r['result']

for record in r['records']:
    print(record['register_id'])