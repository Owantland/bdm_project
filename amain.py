import os
import json
import time
from collections import defaultdict
from tqdm import tqdm
import hashlib
from datetime import datetime, timedelta
from tqdm import tqdm
import requests as req
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, explode, col, array, lit, transform
from azure.storage.blob import BlobServiceClient
from hdfs import InsecureClient as HdfsClient
from io import BytesIO
import numpy as np
from PIL import Image
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
import findspark
from sqlalchemy import create_engine, inspect, text
import io
from PIL import Image
import torch
import clip
from pymilvus import (
    connections,
    FieldSchema, CollectionSchema, DataType, Collection, utility
)
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType
import time


def build_struct(flat_schema: dict) -> StructType:
    fields = []
    for name, typ in flat_schema.items():
        if isinstance(typ, list):
            elem = typ[0]
            if isinstance(elem, dict):
                struct = build_struct(elem)
                fields.append(StructField(name, ArrayType(struct), True))
            else:
                spark_type = {'int': IntegerType(), 'float': DoubleType(), 'str': StringType(), 'bool': BooleanType()}.get(elem, StringType())
                fields.append(StructField(name, ArrayType(spark_type), True))
        else:
            spark_type = {'int': IntegerType(), 'float': DoubleType(), 'str': StringType(), 'bool': BooleanType()}.get(typ, StringType())
            fields.append(StructField(name, spark_type, True))
    return StructType(fields)

def string_to_sha256(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def flatten_df(df):
    from pyspark.sql.types import StructType
    flat_cols = []
    nested_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            nested_cols.append(field.name)
        else:
            flat_cols.append(col(field.name))
    for nested in nested_cols:
        for f in df.schema[nested].dataType.fields:
            flat_cols.append(col(f"{nested}.{f.name}").alias(f"{nested}_{f.name}"))
    return df.select(flat_cols)

# Configuración del Data Lake
# Asume que 'file_system_name' es el filesystem de Delta Lake
def get_data_lake_service(account_url: str, credential) -> DataLakeServiceClient:
    return DataLakeServiceClient(account_url=account_url, credential=credential)

@udf(StringType())
def sha256_udf(url: str) -> str:
    return hashlib.sha256(url.encode('utf-8')).hexdigest() if url else None

@udf(StringType())
def standardized_hours_udf(ts: str) -> str:
    return datetime.fromisoformat(ts).strftime('%H:%M') if ts else None


def file_exists(fs_client, path: str) -> bool:
    try:
        file_client = fs_client.get_file_client(path)
        return file_client.exists()
    except Exception:
        return False


def upload_file(fs_client, path: str, data: bytes, overwrite=True) -> None:
    file_client = fs_client.get_file_client(path)
    if overwrite and file_client.exists():
        file_client.delete_file()
    file_client.create_file()
    file_client.append_data(data, offset=0)
    file_client.flush_data(len(data))

# ---------------------------------------------------------------------
# FLATTEN, CAST, SCHEMA Y COMPRESIÓN DE IMÁGENES (sin cambios)
# ---------------------------------------------------------------------


def load_json_schema(path: str) -> dict:
    with open(path) as f:
        return json.load(f)

def compress_image(image_bytes: bytes, max_width: int = 1024, quality: int = 75) -> bytes:
    with Image.open(BytesIO(image_bytes)) as img:
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        if img.width > max_width:
            ratio = max_width / float(img.width)
            new_height = int(img.height * ratio)
            img = img.resize((max_width, new_height), Image.LANCZOS)
        buffer = BytesIO()
        img.save(buffer, format="JPEG", quality=quality, optimize=True)
        return buffer.getvalue()

def process_accommodation_record(record: dict, schema: dict) -> dict:
    # Crear DF inferido
    accommodation_schema_dict = json.loads(open('accommodation_schema.json').read())
    accommodation_struct = build_struct(accommodation_schema_dict)
     # Crear DF completo e inferido
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(record)]))

    # Extraer columnas planas y hasta 3 niveles de anidación

    all_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            for subfield in field.dataType.fields:
                if isinstance(subfield.dataType, StructType):
                    for subsubfield in subfield.dataType.fields:
                        if isinstance(subsubfield.dataType, StructType):
                            for subsubsubfield in subsubfield.dataType.fields:
                                all_cols.append(
                                    col(f"{field.name}.{subfield.name}.{subsubfield.name}.{subsubsubfield.name}").alias(f"{field.name}_{subfield.name}_{subsubfield.name}_{subsubsubfield.name}")
                                )
                        else:
                            all_cols.append(
                                col(f"{field.name}.{subfield.name}.{subsubfield.name}").alias(f"{field.name}_{subfield.name}_{subsubfield.name}")
                            )
                else:
                    all_cols.append(
                        col(f"{field.name}.{subfield.name}").alias(f"{field.name}_{subfield.name}")
                    )
        else:
            all_cols.append(col(field.name))

    df_flat = df.select(*all_cols)

    # Generar hashes de fotos en Python
    photo_urls = record.get("property", {}).get("photoUrls", [])
    photo_hashes = [string_to_sha256(u) for u in photo_urls]
    df_final = df_flat.withColumn("property_photoHash", lit(photo_hashes))

    # Serializar y validar con esquema oficial
    json_flat = df_final.toJSON().first()
    df_valid = spark.read.schema(accommodation_struct).json(spark.sparkContext.parallelize([json_flat]))
    return df_valid.collect()[0].asDict()

# --- Procesar registro de clima con Spark flatten y luego aplicar esquema ---
def process_weather_record(raw: dict, schema: dict) -> dict:
    weather_schema_dict = json.loads(open('weather_schema.json').read())
    weather_struct = build_struct(weather_schema_dict)
    raw_hourly = raw['hourly']
    raw_hourly['timestamp'] = raw_hourly['time']
    raw_hourly['time'] = [datetime.fromisoformat(t).strftime('%H:%M') for t in raw_hourly['time']]
    
    data_rows = [dict(zip(raw_hourly.keys(), values)) for values in zip(*raw_hourly.values())]

    # 4. Crear el DataFrame con esquema aplicado
    df_valid = spark.createDataFrame(data_rows, schema=weather_struct)
    all_rows = [row.asDict() for row in df_valid.collect()]

    return all_rows

def process_accommodation_images(photo_urls: list, fs_client, city: str) -> None:
    for url in photo_urls:
        sha = string_to_sha256(url)
        path = f"landing_zone/accommodation_images/{city}/{sha}.jpg"
        trusted_path = f"trusted_zone/accommodation_images/{city}/{sha}.jpg"
        if file_exists(fs_client, path) and file_exists(fs_client, trusted_path):
            continue
        if file_exists(fs_client, path):
            data = fs_client.get_file_client(path).download_file().readall()
        else:
            res = req.get(url, stream=True)
            try:
                res.raise_for_status()
            except:
                continue
            data = res.content
            upload_file(fs_client, path, data)
        compressed = compress_image(data, max_width=800, quality=70)
        upload_file(fs_client, trusted_path, compressed)

BACKFILL_LOG = "/exploitation_zone/backfilled_files.txt"


def image_bytes_to_embedding(
    image_bytes: bytes
) -> list:

    img = Image.open(io.BytesIO(image_bytes)).convert('L')
    img = img.resize((32, 32))
    arr = np.array(img).astype(np.float32).flatten()
    arr /= 255.0
    norm = np.linalg.norm(arr)
    if norm > 0:
        arr /= norm
    
    return arr



def get_or_create_collection(
    collection_name: str = "accommodation_images",
    dim: int = 1024,
    index_params: dict = None
) -> Collection:

    
    # 3.2 Define schema if needed
    if not utility.has_collection(collection_name):
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, is_primary=True, auto_id=False, max_length=100),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim)
        ]
        schema = CollectionSchema(fields, description=f"{collection_name} image embeddings")
        collection = Collection(name=collection_name, schema=schema)
        # create index
        idx = index_params or {
            "index_type": "IVF_FLAT",
            "params": {"nlist": 128},
            "metric_type": "L2"
        }
        collection.create_index(field_name="embedding", index_params=idx)
    else:
        collection = Collection(collection_name)
    
    # 3.3 Load into memory for speed
    collection.load()
    return collection


def insert_embedding(
    collection: Collection,
    id: int,
    vector: list
) -> None:
    collection.insert([[id], [vector]])
    collection.flush()
    


def query_embeddings(
    collection: Collection,
    expr: str = "",
    output_fields: list = None,
    limit: int = 100
) -> list:
    output_fields = output_fields or ["id", "embedding"]
    results = collection.query(expr=expr, output_fields=output_fields, limit=limit)
    return results

def _load_backfill_log() -> set:
    """
    Ensure the backfill_log table exists, then load all file_key values into a set.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS backfill_log (
                file_key TEXT PRIMARY KEY,
                inserted_at TIMESTAMP NOT NULL
            )
        """))
        rows = conn.execute(text("SELECT file_key FROM backfill_log")).all()
    return {r.file_key for r in rows}



def _append_to_backfill_log(log_key: str) -> None:
    """
    Insert a new entry into backfill_log (or do nothing if already present).
    """
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO backfill_log (file_key, inserted_at)
            VALUES (:file_key, :inserted_at)
            ON CONFLICT (file_key) DO NOTHING
        """), {
            "file_key": log_key,
            "inserted_at": datetime.utcnow()
        })

def _write_to_postgres(df, table_name: str) -> None:
    """
    Write a Spark DataFrame to Postgres, creating the table if it doesn't exist.
    If the table is created, the schema and a sample of the data are shown.
    After creation, the function confirms the table exists before writing.
    Raises an error if table creation failed.
    """
    inspector = inspect(engine)

    if not inspector.has_table(table_name):
        print(f"🛠️ Table '{table_name}' does not exist. Creating it...")

        try:
            # Create the table (schema only)
            df.limit(0) \
              .write \
              .format("jdbc") \
              .option("url", POSTGRES_DRIVER_URL) \
              .option("dbtable", table_name) \
              .mode("overwrite") \
              .save()

            # Confirm creation
            inspector = inspect(engine)
            if not inspector.has_table(table_name):
                raise RuntimeError(f"❌ Failed to create table '{table_name}'.")

            print("✅ Table created. Schema:")
            print("📊 Sample rows:")
            df.show(5, truncate=False)

        except Exception as e:
            print(df.show())
            raise RuntimeError(f"❌ Error creating table '{table_name}': {e}")

    # Append data
    try:
        df.write \
          .format("jdbc") \
          .option("url", POSTGRES_DRIVER_URL) \
          .option("dbtable", table_name) \
          .mode("append") \
          .save()

    except Exception as e:
        print(df.show())
        raise RuntimeError(f"❌ Error writing to table '{table_name}': {e}")


def _reverse_geocode_one(lat, lon):
    """
    Reverse‐geocode a single (lat, lon) with retries.
    Returns (address:str or None, postcode:str or None).
    """
    if lat is None or lon is None:
        return None, None

    geolocator = Nominatim(user_agent="partitioned_reverse_geocoder")
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            location = reverse((lat, lon), exactly_one=True)
            if location and location.raw and "address" in location.raw:
                address = location.address
                postcode = location.raw["address"].get("postcode")
                return address, postcode
            return None, None
        except (GeocoderTimedOut, GeocoderUnavailable):
            time.sleep(RETRY_DELAY_SECONDS)
        except Exception:
            break
    return None, None


def geocode_partition(partition_iterator, lat_col, lon_col):
    """
    For each Row in this partition, call _reverse_geocode_one(lat, lon),
    then yield a new Row that adds "address" and "postcode" fields.
    """
    for row in partition_iterator:
        lat = row[lat_col]
        lon = row[lon_col]
        addr, zipc = _reverse_geocode_one(lat, lon)

        # Convert Row to a dict, add the new fields, and re‐emit as Row
        row_dict = row.asDict()
        row_dict["property_address"] = addr
        row_dict["property_postalcode"] = zipc
        yield Row(**row_dict)


def add_address_columns(df, lat_col="property_latitude", lon_col="property_longitude"):
    """
    Given a Spark DataFrame `df` with columns `lat_col` and `lon_col`, 
    returns a new DataFrame with two extra columns: "address" and "postcode",
    filled by doing reverse geocoding partition‐wise.
    """
    # 1) Transform via mapPartitions
    rdd_with_geocode = df.rdd.mapPartitions(
        lambda partition: geocode_partition(
            partition, lat_col, lon_col
        )
    )

    # 2) Build new schema (old fields + address + postcode)
    old_schema = df.schema

    new_fields = old_schema.fields + [
        StructField("property_address", StringType(), True),
        StructField("property_postalcode", StringType(), True),
    ]
    new_schema = StructType(new_fields)
    return df.sparkSession.createDataFrame(rdd_with_geocode, schema=new_schema)


def _fetch_files(coll: str) -> list[int]:
    """
    Connects to an existing Milvus collection and returns a list of all primary-key IDs.
    """
    # How many entities are stored?
    total = coll.num_entities
    if total == 0:
        return []
    results = coll.query(
        expr="", 
        output_fields=["id"], 
        limit=total
    )
    # Extract and return the id values
    return [row["id"] for row in results]



def get_and_sync_accommodation(
    fs,
    spark: SparkSession,
    start: datetime,
    end: datetime,
    cities: dict,
    query_template: dict,
    headers:dict,
    schema_file: str
):
    schema = load_json_schema(schema_file)
    delta = timedelta(days=1)
    log_entries = _load_backfill_log()
    coll = get_or_create_collection()
    vector_ids = _fetch_files(coll)

    for single_date in tqdm([start + i * delta for i in range((end - start).days + 1)]):
        arrival = single_date.strftime('%Y-%m-%d')
        departure = (single_date + delta).strftime('%Y-%m-%d')
        for city, dest_id in tqdm(cities.items()):
            landing_path = f"landing_zone/accommodation/{city}/{arrival}_{departure}.json"
            trusted_path = f"trusted_zone/accommodation/{city}/{arrival}_{departure}.json"
            
            log_key = f"accommodation/{city}/{arrival}_{departure}.json"
            # filtro: si existe en landing y trusted, nada que hacer
            if file_exists(fs, landing_path) and file_exists(fs, trusted_path) and log_key in log_entries and False:
                continue

            # obtener JSON: de landing o API
            if file_exists(fs, landing_path):
                raw = fs.get_file_client(landing_path).download_file().readall().decode('utf-8')
                data = json.loads(raw)
            else:
                params = dict(query_template, dest_id=dest_id, arrival_date=arrival, departure_date=departure)
                res = req.get(accommodation_endpoint, params=params, headers=headers)
                time.sleep(10)
                res.raise_for_status()
                data = res.json()
                upload_file(fs, landing_path, json.dumps(data).encode('utf-8'))
            

            if file_exists(fs, trusted_path):
                trusted = fs.get_file_client(trusted_path).download_file().readall().decode('utf-8')
                docs = json.loads(trusted)
                photo_hashes = [doc['property_photoHash'] for doc in docs]
                photo_urls = [u for h in data['data']['hotels'] for u in h['property']['photoUrls']]

            else:
                photo_urls = [u for h in data['data']['hotels'] for u in h['property']['photoUrls']]
                process_accommodation_images(photo_urls, fs, city)
                docs = [process_accommodation_record(r, schema) for r in data['data']['hotels']]
                photo_hashes = [doc['property_photoHash'] for doc in docs]
                upload_file(fs, trusted_path, json.dumps(docs).encode('utf-8'))
            
            for hashes, urls in zip(photo_hashes, photo_urls):
                    for hash, url in zip(hashes, urls):
                        img_path = f"trusted_zone/accommodation_images/{city}/{hash}.jpg"
                        if not file_exists(fs, img_path):
                            process_accommodation_images([url], fs, city)
                            data_bytes = fs.get_file_client(img_path).download_file().readall()
                            with engine.begin() as conn:
                                conn.execute(text("""
                                    INSERT INTO accommodation_images (id, image_bytes)
                                    VALUES (:id, :img)
                                    ON CONFLICT (id) DO NOTHING
                                """), {"id": hash, "img": data_bytes})
                        
                        if hash not in vector_ids:
                            data_bytes = fs.get_file_client(img_path).download_file().readall()
                            emb = image_bytes_to_embedding(data_bytes)
                            coll = get_or_create_collection(collection_name="accommodation_images", dim=len(emb))
                            insert_embedding(coll, id=hash, vector=emb)
                            _append_to_backfill_log(img_path)

            exploitation_df = spark.read.json(spark.sparkContext.parallelize([json.dumps(d) for d in docs]))
            exploitation_df = exploitation_df.drop('property_priceBreakdown_benefitBadges')
            exploitation_df = exploitation_df.withColumn("property_priceBreakdown_excludedPrice_value",exploitation_df["property_priceBreakdown_excludedPrice_value"].cast(DoubleType()))
            exploitation_df = exploitation_df.withColumn("city_code_name", lit(city))
            exploitation_df = add_address_columns(exploitation_df)
            _write_to_postgres(exploitation_df, table_name='accommodation')
            _append_to_backfill_log(log_key)
            

            

def get_and_sync_weather(
    fs,
    spark: SparkSession,
    start: datetime,
    end: datetime,
    coords: dict,
    query_template: dict,
    schema_file: str
):
    schema = load_json_schema(schema_file)
    delta = timedelta(days=1)
    log_entries = _load_backfill_log()
    
    for single_date in tqdm([start + i * delta for i in range((end - start).days + 1)]):
        prev_start = (single_date - timedelta(days=365)).strftime('%Y-%m-%d')
        for city, coord in coords.items():
            landing_path = f"landing_zone/weather/{city}/{prev_start}.json"
            trusted_path = f"trusted_zone/weather/{city}/{prev_start}.json"
            log_key = f"weather/{city}/{prev_start}.json"

            # filtro: si existe en landing y trusted, nada que hacer
            if file_exists(fs, landing_path) and file_exists(fs, trusted_path) and log_key in log_entries:
                continue

            # obtener JSON: de landing o API
            if file_exists(fs, landing_path):
                raw = fs.get_file_client(landing_path).download_file().readall().decode('utf-8')
                data = json.loads(raw)
            else:
                params = dict(query_template, latitude=coord['latitude'], longitude=coord['longitude'], start_date=prev_start, end_date=prev_start)
                res = req.get(weather_endpoint, params=params)
                time.sleep(10)
                res.raise_for_status()
                data = res.json()
                upload_file(fs, landing_path, json.dumps(data).encode('utf-8'))

            if file_exists(fs, trusted_path):
                trusted = fs.get_file_client(trusted_path).download_file().readall().decode('utf-8')
                data_rows = json.loads(trusted)
                weather_schema_dict = json.loads(open('weather_schema.json').read())
                weather_struct = build_struct(weather_schema_dict)
                df_valid = spark.createDataFrame(data_rows, schema=weather_struct)
                doc = [row.asDict() for row in df_valid.collect()]
            else:
                doc = process_weather_record(data, schema)
                upload_file(fs, trusted_path, json.dumps(doc).encode('utf-8'))
            

            exploitation_df = spark.read.json(spark.sparkContext.parallelize(doc))
            exploitation_df = exploitation_df.withColumn("city_code_name", lit(city))
            _write_to_postgres(exploitation_df, table_name='weather')
            _append_to_backfill_log(log_key)
            

def list_files(fs_client, prefix: str) -> list:
    return [p.name for p in fs_client.get_paths(path=prefix) if not p.is_directory]


def find_missing_blobs(fs, landing_prefix: str) -> list:
    missing = []
    for path in list_files(fs, 'landing_zone/' + landing_prefix):
        trusted_path = f"trusted_zone/{path.replace('landing_zone/', '', 1)}"
        if not file_exists(fs, trusted_path):
            missing.append(path)
    return missing


def backfill_trusted_zone(
    fs,
    landing_prefix: str,
    schema_file: str = None
) -> None:
    schema = load_json_schema(schema_file) if schema_file else None
    to_fill = find_missing_blobs(fs, landing_prefix)

    for landing_path in tqdm(to_fill):
        trusted_path = f"trusted_zone/{landing_path.replace('landing_zone/', '', 1)}"
        data_bytes = fs.get_file_client(landing_path).download_file().readall()

        # imágenes de alojamiento
        if landing_path.startswith('landing_zone/accommodation_images/'):
            compressed = compress_image(data_bytes, max_width=800, quality=70)
            upload_file(fs, trusted_path, compressed)
            continue

        # JSON de alojamiento
        if landing_path.startswith('landing_zone/accommodation/'):
            data = json.loads(data_bytes.decode('utf-8'))
            docs = [process_accommodation_record(r, schema) for r in data['data']['hotels']]
            upload_file(fs, trusted_path, json.dumps(docs).encode('utf-8'))
            continue

        # JSON de clima
        if landing_path.startswith('landing_zone/weather/'):
            data = json.loads(data_bytes.decode('utf-8'))
            doc = process_weather_record(data, schema)
            upload_file(fs, trusted_path, json.dumps(doc).encode('utf-8'))

def backfill_exploitation_zone(
    dl_client,
    spark: SparkSession,
    data_type: str,
):
    # Load log entries once
    log_entries = _load_backfill_log()
    coll = get_or_create_collection()
    vector_ids = _fetch_files(coll)
    # List all files under trusted_zone
    all_paths = list_files(dl_client, "trusted_zone/")
    # Filter relevant paths
    qualified = []  # tuples of (full_path, city)
    for path in all_paths:
        parts = path.split('/')
        d_type, city = parts[-3], parts[-2]
        if d_type != data_type:
            continue
        full_path = f"trusted_zone/{data_type}/{city}/{parts[-1]}"
        qualified.append((full_path, city))

    if data_type in ("accommodation", "weather"):
        for full_path, city in tqdm(qualified):
            file_name = full_path.split('/')[-1]
            log_key = f"{data_type}/{city}/{file_name}"
            if log_key in log_entries:
                continue

            # read JSON from trusted
            data_bytes = dl_client.get_file_client(full_path).download_file().readall()
            docs = json.loads(data_bytes.decode('utf-8'))

            # build an RDD of JSON strings (list for accommodation, single dict for weather)
            if isinstance(docs, list):
                rdd = spark.sparkContext.parallelize([json.dumps(d) for d in docs])
            else:
                rdd = spark.sparkContext.parallelize([json.dumps(docs)])

            exploitation_df = spark.read.json(rdd)
            if data_type == 'accommodation':
                exploitation_df = exploitation_df.drop('property_priceBreakdown_benefitBadges')
                exploitation_df = exploitation_df.withColumn("property_priceBreakdown_excludedPrice_value",exploitation_df["property_priceBreakdown_excludedPrice_value"].cast(DoubleType()))
            exploitation_df = exploitation_df.withColumn("city_code_name", lit(city))
            exploitation_df = add_address_columns(exploitation_df)
            _write_to_postgres(exploitation_df, table_name=data_type)
            _append_to_backfill_log(log_key)


    elif data_type == "accommodation_images":
        # ensure images table
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS accommodation_images (
                    id TEXT PRIMARY KEY,
                    image_bytes BYTEA NOT NULL
                )
            """))

        for full_path, city in tqdm(qualified):
            file_name = full_path.split('/')[-1]
            image_id = os.path.splitext(file_name)[0]
            log_key = f"{data_type}/{city}/{file_name}"
            
            if log_key not in log_entries:
                data_bytes = dl_client.get_file_client(full_path).download_file().readall()
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO accommodation_images (id, image_bytes)
                        VALUES (:id, :img)
                        ON CONFLICT (id) DO NOTHING
                    """), {"id": image_id, "img": data_bytes})
            
            if file_name not in vector_ids:
                data_bytes = dl_client.get_file_client(full_path).download_file().readall()
                emb = image_bytes_to_embedding(data_bytes)
                coll = get_or_create_collection(collection_name="accommodation_images", dim=len(emb))
                insert_embedding(coll, id=image_id, vector=emb)
            
            _append_to_backfill_log(log_key)


destination_ids = {
    "Barcelona": "-372490",
    "Rome": "-126693",
    "Madrid": "-390625",
    "Paris": "-1456928"
}

destination_coords = {
    'Barcelona': {'latitude': 41.3874, 'longitude': 2.1686},
    'Paris': {'latitude': 48.8575, 'longitude': 2.3514},
    'Madrid': {'latitude': 40.4167, 'longitude': 3.7033},
    'Rome': {'latitude': 41.8967, 'longitude': 12.4822}
}

accommodation_endpoint = "https://booking-com15.p.rapidapi.com/api/v1/hotels/searchHotels"
weather_endpoint = 'https://archive-api.open-meteo.com/v1/archive'

headers = {
    "x-rapidapi-key": os.environ["RAPID_API_KEY"],
    "x-rapidapi-host": os.environ["RAPID_API_HOST"]
}

accommodation_query = {
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

weather_metrics = 'temperature_2m,rain,snowfall,precipitation,cloud_cover,wind_speed_10m,sunshine_duration'

weather_query = {
    'latitude': '',
    'longitude': '',
    'hourly': weather_metrics,
    'start_date': '',
    'end_date': ''
}

if __name__ == '__main__':

    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    os.environ["PYSPARK_PYTHON"]        = "/opt/miniconda3/envs/tpc-di/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/miniconda3/envs/tpc-di/bin/python"

    spark = SparkSession.builder \
        .appName("BDMProject") \
        .master("spark://localhost:7077")\
        .config("spark.jars", JDBC_JAR) \
        .config("spark.driver.extraClassPath", JDBC_JAR) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    # Initialize DataLakeServiceClient once
    datalake_client = DataLakeServiceClient.from_connection_string(connection_string)
    file_system_client = datalake_client.get_file_system_client(storage_container_name)
    engine = create_engine(POSTGRES_URL)
    connections.connect(alias="default", host='localhost', port='19530')


    # parameters
    start = datetime.strptime('2025-07-03', '%Y-%m-%d')
    end = datetime.strptime('2025-07-04', '%Y-%m-%d')
    cities = list(destination_ids.keys()) 

    # sync on the fly
    get_and_sync_accommodation(file_system_client, spark, start, end, destination_ids, accommodation_query, headers, 'accommodation_schema.json')
    get_and_sync_weather(file_system_client, spark, start, end, destination_coords, weather_query, 'weather_schema.json')

    backfill_trusted_zone(file_system_client, 'accommodation/', 'accommodation_schema.json')
    backfill_trusted_zone(file_system_client, 'accommodation_images/')
    backfill_trusted_zone(file_system_client, 'weather/', 'weather_schema.json')
    backfill_exploitation_zone(file_system_client, spark, 'accommodation')
    backfill_exploitation_zone(file_system_client, spark, 'accommodation_images')
    backfill_exploitation_zone(file_system_client, spark, 'weather')