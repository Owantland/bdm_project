import os
import chardet
import csv
import requests as req

# from main import azure_connection, upload_blo
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lower, trim, regexp_replace, col, when, to_timestamp, split, row_number, lit, concat, isnan
import pandas as pd
import re
import shutil
import glob
from pyspark.sql.types import DateType
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from function_ev_trus import (
    empty_pandas_df,
    fixtextformat,
    checkendpharase,
    formatdata,
    take_mail_val,
    nan_if_slash,
    fix_french_syntax, 
    clean_html,
    clean_postal_code_p,
    check_url,
    upload_file,
    check_mail,
    translated,
    clean_postal_code_b,
    textformatspa,
    clean_postal_code_m,
    clean_weekdays,
    file_exists,
    load_spark_dfs_to_postgres,
    add_category_columns
)
from email_validator import validate_email, EmailNotValidError

def smart_read_csv(filepath, encoding="utf-8"):
    with open(filepath, 'r', encoding=encoding, errors='replace') as f:
        sample = f.readline()
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        delimiter = dialect.delimiter

    try:
        df = pd.read_csv(filepath, delimiter=delimiter, encoding=encoding, on_bad_lines='skip')
    except Exception:
        for sep in [",", ";", "\t", "|"]:
            try:
                df = pd.read_csv(filepath, delimiter=sep, encoding=encoding, on_bad_lines='skip')
                if len(df.columns) > 1:
                    return df
            except:
                continue
        df = pd.read_csv(filepath, delimiter=delimiter, encoding=encoding, on_bad_lines='skip')
    return df

def landing_zone_events(fs_client, events):
    print("Landing zone of events")
    fallback_schema = None
    dfs = []
    for city, url in events.items():
        path = f"landing_zone/event_data/{city}_events.csv"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        try: 
            res = req.get(url)
            with open(path, 'wb') as f:
                f.write(res.content)
            with open(path, 'rb') as f:
                raw = f.read()
                encoding = chardet.detect(raw)['encoding'] or 'utf-8-sig'
                content = raw.decode(encoding, errors='replace')
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
            try:
                df = smart_read_csv(path)
                df['city'] = city
                print(f"{city}: loaded, shape={df.shape}")
                dfs.append(df)
                with open(path, "rb") as f:
                    data = f.read()
                upload_file(fs_client, path, data)
                if fallback_schema is None:
                    fallback_schema = df.columns.tolist()

            except Exception as e:
                print(f"Parse error {city}: {e}")
                dfs.append(empty_pandas_df(fallback_schema))
        except Exception as e:
            print(f"Download error {city}: {e}")
            dfs.append(empty_pandas_df(fallback_schema))
        finally:
            if os.path.exists(path):
                os.remove(path)
    for d in dfs:
        print(d.head(5)) 
    return dfs


import io
import pyarrow as pa
import pyarrow.parquet as pq
def upload_parquet_from_spark(fs_client, df_spark, city):
    trusted_path = f"trusted_zone/events/{city}.parquet"
    if df_spark is None or df_spark.rdd.isEmpty():
        print("DF is empty, skipping")
        return
    
    pdf = df_spark.toPandas()
    table = pa.Table.from_pandas(pdf)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    buffer.seek(0)

    upload_file(fs_client, trusted_path, buffer.read(), overwrite=True)
    print(f"the Parquet is uploaded: {trusted_path}")


def trusted_zone_events(fs_client, dfs_pandas, spark):
    print("Trusted zone for the Events")
    tables = []
    dfs_spark = []
    dfs_out = []
    dfs_spark = []
    for i, df in enumerate(dfs_pandas):
        try:
            if i == 2:
                df = df.astype(str)
            sdf = spark.createDataFrame(df)
            dfs_spark.append(sdf)
        except Exception as e:
            print(f"Error on DataFrame {i}: {e}")
            if df is not None and len(df.columns) > 0:
                try:
                    schema = df.astype(str).columns.tolist()
                    empty_df = spark.createDataFrame([], schema=schema)
                    dfs_spark.append(empty_df)
                except Exception as inner_e:
                    print(f"Fallback failed for DF {i}: {inner_e}")
                    dfs_spark.append(None)
            else:
                dfs_spark.append(None)
    ### Italy###
    if dfs_spark[0] is not None and not dfs_spark[0].rdd.isEmpty():
        italian_shops_rome = dfs_spark[0]
    
        if 'Descrizione' in italian_shops_rome.columns:
            italian_shops_rome = italian_shops_rome.withColumn(
                'Descrizione',
                formatdata(checkendpharase(col('Descrizione')))
            )
        for c in italian_shops_rome.columns:
            if c not in ['Denominazione', 'Descrizione'] and str(italian_shops_rome.schema[c].dataType) == 'StringType()':
                italian_shops_rome = italian_shops_rome.withColumn(
                    c, 
                    regexp_replace(
                        lower(trim(fixtextformat(col(c)))), r'\s+', ' '
                    )
                )
        for c in italian_shops_rome.columns:
            if str(italian_shops_rome.schema[c].dataType) == 'StringType()':
                italian_shops_rome = italian_shops_rome.withColumn(
                    c,
                    nan_if_slash(col(c))
                )
        if 'Denominazione' in italian_shops_rome.columns:
            italian_shops_rome = italian_shops_rome.dropDuplicates(['Denominazione'])
        if '_id' in italian_shops_rome.columns:
            italian_shops_rome = italian_shops_rome.drop('_id')
        if 'id_' in italian_shops_rome.columns:
            italian_shops_rome = italian_shops_rome.dropDuplicates(['id_'])
        italian_shops_rome = italian_shops_rome \
            .withColumn('Latitudine', col('Latitudine').cast('double')) \
            .withColumn('Longitudine', col('Longitudine').cast('double'))
        
        upload_parquet_from_spark(fs_client, italian_shops_rome, "rome")
        
    dfs_out.append(italian_shops_rome)
    tables.append("rome_events")
    if dfs_spark[1] is not None and not dfs_spark[1].rdd.isEmpty():
    ### France
        france_activity_paris = dfs_spark[1]
        text_columns = [
        'Titre', 'Chapeau', 'Description', 'Description de la date',
        'Nom du lieu', 'Adresse du lieu', 'Ville', 'Transport', 'Détail du prix',
        'Texte alternatif de l\'image', 'Crédit de l\'image'
        ]
        for c in text_columns:
            if c in france_activity_paris.columns:
                france_activity_paris = france_activity_paris.withColumn(
                    c, fix_french_syntax(clean_html(col(c)))
                )
        date_cols = [c for c in france_activity_paris.columns if re.search(r'date', c, re.IGNORECASE)]
        for col_name in date_cols:
            france_activity_paris = france_activity_paris.withColumn(
                col_name,
                to_timestamp(col(col_name),"yyyy-MM-dd HH:mm:ss" ) 
            )
        if 'Coordonnées géographiques' in france_activity_paris.columns:
            france_activity_paris = france_activity_paris \
                .withColumn('latitude', split(col('Coordonnées géographiques'), ',')[0].cast('double')) \
                .withColumn('longitude', split(col('Coordonnées géographiques'), ',')[1].cast('double'))
        
        pattern = re.compile(r'(id|code)', re.IGNORECASE)
        for c in france_activity_paris.columns:
            if pattern.search(c):
                france_activity_paris = france_activity_paris.withColumn(
                    c, col(c).cast('long')
                )
        
        id_columns = [c for c in france_activity_paris.columns if re.search(r'ID', c, re.IGNORECASE)]
        
        if id_columns:
            france_activity_paris = france_activity_paris.dropDuplicates(id_columns)
        exclude = ['Titre', 'Description']
        string_cols = [
            c for (c, dtype) in france_activity_paris.dtypes
            if dtype == 'string' and c not in exclude
        ]
        for c in string_cols:
            france_activity_paris = france_activity_paris.withColumn(
                c, lower(trim(col(c)))
            )
        print(" (Titre, Description):")
        (
            france_activity_paris
                .select("Titre", "Description") 
                .show(10, truncate=False)      
        )
        upload_parquet_from_spark(fs_client, france_activity_paris, "paris")
    dfs_out.append(france_activity_paris)
    tables.append("paris_events")
    from pyspark.sql.window import Window


    if dfs_spark[2] is not None and not dfs_spark[2].rdd.isEmpty():
    ### Madrid###

        spain_activity_barca = dfs_spark[2]
        
        date_cols = [c for c in spain_activity_barca.columns if re.search(r'(dat|created|modified)', c, re.IGNORECASE)]
        for col_name in date_cols:
            spain_activity_barca = spain_activity_barca.withColumn(
                col_name,
                to_timestamp(col(col_name),"yyyy-MM-dd HH:mm:ss" ) 
            )
        spain_activity_barca = spain_activity_barca.withColumn("register_id", col("register_id").cast("long"))
        window_spec = Window.partitionBy("register_id").orderBy(col("created").desc())
        spain_activity_barca = spain_activity_barca.withColumn("row_num", row_number().over(window_spec))
        spain_activity_barca = spain_activity_barca.filter(col("row_num") == 1).drop("row_num")
        for column_name in spain_activity_barca.columns:
            if re.search(r'(id|number|code)', column_name, re.IGNORECASE):
                spain_activity_barca = spain_activity_barca.withColumn(
                    column_name,
                    col(column_name).cast('long')
                )
        for col_name, dtype in spain_activity_barca.dtypes:
            if dtype == 'string':
                if 'name' in col_name.lower():
                    spain_activity_barca = spain_activity_barca.withColumn(
                        col_name, textformatspa(col(col_name))
                    )
                else:
                    spain_activity_barca = spain_activity_barca.withColumn(col_name, lower(trim((col(col_name)))))
        upload_parquet_from_spark(fs_client, spain_activity_barca, "barca")
        dfs_out.append(spain_activity_barca)
        tables.append("barca_events")
    if dfs_spark[3] is not None and not dfs_spark[3].rdd.isEmpty():
        spain_activity_madrid = dfs_spark[3]
        date_cols = [c for c in spain_activity_madrid.columns if re.search(r'(FECHA)', c, re.IGNORECASE)]
        for col_name in date_cols:
            spain_activity_madrid = spain_activity_madrid.withColumn(
                col_name,
                to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss")
            )
        if 'ID-EVENTO' in spain_activity_madrid.columns:
            italian_shops_rome = spain_activity_madrid.dropDuplicates(['ID-EVENTO'])

        pattern = re.compile(r"(TITULO|DESCRIPTION)", re.IGNORECASE)
        for colname in spain_activity_madrid.columns:
            if pattern.search(colname):
                spain_activity_madrid = spain_activity_madrid.withColumn(colname, textformatspa(col(colname)))
        for column in spain_activity_madrid.columns:
            if re.search(r'ID', column, re.IGNORECASE) or 'ID' in column.upper():
                spain_activity_madrid = spain_activity_madrid.withColumn(column, col(column).cast("long"))
            elif column.upper() in {
                'CODIGO-POSTAL-INSTALACION', 'NUM-INSTALACION',
                'COORDENADA-X', 'COORDENADA-Y', 'LATITUD', 'LONGITUD'
            }:
                spain_activity_madrid = spain_activity_madrid.withColumn(column, col(column).cast("double"))
        for col_name, dtype in spain_activity_madrid.dtypes:
            if dtype == 'string':
                if re.search(r'fecha', col_name, re.IGNORECASE) or re.search(r'hora', col_name, re.IGNORECASE):
                    continue
                if not re.search(r'titulo', col_name, re.IGNORECASE) and not re.search(r'descripcion', col_name, re.IGNORECASE):
                    spain_activity_madrid = spain_activity_madrid.withColumn(col_name, lower(trim(col(col_name))))
        upload_parquet_from_spark(fs_client, spain_activity_madrid, "madrid")
        
        
    dfs_out.append(spain_activity_madrid)
    tables.append("madrid_events")
    return dfs_out, tables

def explotation_zone_events(dfs_spark):
    print("Explotation zone for the Events")
    
    results = []
    tables = []

    if dfs_spark[0] is not None and not dfs_spark[0].rdd.isEmpty():
        df = dfs_spark[0]
        df = df.withColumn(
            'position available',
            when(col('Latitudine').isNull() | col('Longitudine').isNull() |
        isnan(col("Latitudine")) | isnan(col("Longitudine")), 'no').otherwise('yes')
        )
        df = df.filter(col('position available') == 'yes')
        
        if 'Mail' in df.columns:
            df = df.withColumn('Mail', take_mail_val(col('Mail')))
        df = df.withColumn("Modalità di gestione", translated(col("Modalità di gestione"))) \
               .withColumn("Partner", translated(col("Partner"))) \
               .withColumn("Zona Wi-Fi", when(col("Zona Wi-Fi") == "si", "yes").otherwise(col("Zona Wi-Fi"))) \
               .withColumn("Tipologia di utilizzo", translated(col("Tipologia di utilizzo"))) \
               .withColumn("Denominazione", translated(col("Denominazione"))) \
               .withColumn("Descrizione", translated(col("Descrizione")))
        cols_to_translate = {
            'id_': 'id',
            'Denominazione': 'name',
            'Descrizione': 'description',
            'Indirizzo': 'address',
            'Mail': 'email',
            'Sito web': 'website',
            'city': 'city',
            'Longitudine': 'longitude',
            'Latitudine': 'latitude'
        }
        new_columns = [cols_to_translate.get(col, col) for col in df.columns]
        df = df.toDF(*new_columns)
        print("(Rome) (Name, Description):")
        for c in df.columns:
            df = df.withColumnRenamed(c, c.lower())
        df.select("Name", "Description").show(10, truncate=False)
        df = df \
            .withColumn("start_date", to_timestamp(lit("2025-01-01 00:00:00"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("end_date", to_timestamp(lit("9999-12-31 23:59:59"), "yyyy-MM-dd HH:mm:ss"))
        # (optional) Cast a date per postgres
        
        df = df.withColumn("start_date", col("start_date").cast(DateType()))
        df = df.withColumn("end_date", col("end_date").cast(DateType()))
        ordered_cols = [
            "id", "name", "description", "address", "email", "website",
            "city", "longitude", "latitude", "start_date", "end_date"
        ]
        df = df.select(*ordered_cols)
        df = df.withColumn('Point', concat(lit("POINT("),
        col("longitude"),
        lit(" "),
        col("latitude"),
        lit(")")) )
        df = df.drop("longitude", "latitude")
        print("Lable implement")
        print(" (Name, Description): Roma")
        (
            df
                .select("name", "description") 
                .show(10, truncate=False)      
        )
        results.append(df)
        tables.append('rome_expl')

    if dfs_spark[1] is not None and not dfs_spark[1].rdd.isEmpty():
        df = dfs_spark[1]
        df = df.withColumn(
            'position available',
            when(col('latitude').isNull() | col('longitude').isNull() |
        isnan(col("latitude")) | isnan(col("longitude")), 'no').otherwise('yes')
        )
        df = df.filter(col('position available') == 'yes')
        if 'Code postal' in df.columns and 'Ville' in df.columns:
            df = df.withColumn('Code postal', clean_postal_code_p(col('Code postal'), col('Ville')))
        for colname in df.columns:
            if re.search(r'url', colname.lower()):
                df = df.withColumn(colname, check_url(col(colname)))
            if re.search(r'mail', colname.lower()):
                df = df.withColumn(colname, check_mail(col(colname)))
        exclude_keywords_regex = re.compile(r'(dat|téléphone|url|mail)', re.IGNORECASE)
        cols_to_translate = [
            column_name for column_name, dtype in df.dtypes
            if dtype == 'string' and not exclude_keywords_regex.search(column_name)
        ]
        for column_name in cols_to_translate:
            df = df.withColumn(column_name, translated(col(column_name)))
        print("(Paris) (Titre, Description):")
        df.select("Titre", "Description").show(10, truncate=False)
        paris_cols = {
            'ID': 'id',
            'Titre': 'name',
            'Description': 'description',
            'Adresse du lieu': 'address',
            'URL': 'website',
            'Ville': 'city',
            'latitude':'latitude',
            'longitude':'longitude',
            'Date de début':'start_date',
            'Date de fin':'end_date'
        }

        df = df.select(*paris_cols.keys())
        for old, new in paris_cols.items():
            df = df.withColumnRenamed(old, new)
        df = df.withColumn('email', lit('not avl'))
        final_cols = [
            "id", "name", "description", "address", "email", "website",
            "city", "longitude", "latitude", "start_date", "end_date"
        ]
        df = df.select(*final_cols)
        df = df.withColumn('Point', concat(lit("POINT("),
        col("longitude"),
        lit(" "),
        col("latitude"),
        lit(")")) )
        df = df.drop("longitude", "latitude")
        results.append(df)
        tables.append('paris_expl')
        
        print(" (Name, Description): parigi")
        (
            df
                .select("name", "description") 
                .show(10, truncate=False)      
        )
    if dfs_spark[2] is not None and not dfs_spark[2].rdd.isEmpty():
        df = dfs_spark[2]
        df = df.withColumn('position available', lit('yes'))
        if 'geo_epgs_4326_lat' in df.columns and 'geo_epgs_4326_lon' in df.columns:
            df = df.withColumn(
                'position available',
                when(col('geo_epgs_4326_lat').isNull() | col('geo_epgs_4326_lon').isNull()|
        isnan(col("geo_epgs_4326_lat")) | isnan(col("geo_epgs_4326_lon")), 'no').otherwise('yes')
            )
        df = df.withColumn(
        "addresses_zip_code",
        clean_postal_code_b(col("addresses_zip_code"), col("addresses_town"))
        )
        df = df.withColumn('name', translated(col('name')))
        print("(Barcelona) (name, modified):")
        df.select("name", "modified").show(10, truncate=False)
        barcelona = {
            'register_id': 'id',
            'name': 'name',
            'values_description': 'description',  
            'addresses_main_address': 'address',
            'city': 'city',
            'geo_epgs_4326_lon': 'longitude',
            'geo_epgs_4326_lat': 'latitude',
            'start_date':'start_date',
            'end_date':'end_date'
        }

        selected_cols = list(barcelona.keys())
        df = df.toDF(*[c.strip() for c in df.columns])
        df = df.select(*selected_cols)
        for old_name, new_name in barcelona.items():
            df = df.withColumnRenamed(old_name, new_name)

        final_cols = [
            "id", "name", "description", "address", "email", "website",
            "city", "longitude", "latitude", "start_date", "end_date"
        ]
        for missing_col in ['email', 'website']:
            if missing_col not in df.columns:
                df = df.withColumn(missing_col, lit("not avl"))
        df = df.select(*final_cols)
        df = df.withColumn("description", translated(col("description")))\
                .withColumn("name", translated(col("name")))
        print(" (Name, Description): Barca")
        (
            df
                .select("name", "description") 
                .show(10, truncate=False)      
        )
        df = df.withColumn('Point', concat(lit("POINT("),
        col("longitude"),
        lit(" "),
        col("latitude"),
        lit(")")) )
        df = df.drop("longitude", "latitude")
        results.append(df)
        tables.append('barcelona_expl')

    if dfs_spark[3] is not None and not dfs_spark[3].rdd.isEmpty():
        df = dfs_spark[3]
        df = df.withColumn("DIAS-SEMANA", clean_weekdays(col("DIAS-SEMANA"))) \
               .withColumn("CODIGO-POSTAL-INSTALACION", clean_postal_code_m(col("CODIGO-POSTAL-INSTALACION"))) \
               .withColumn("position available", lit("yes")) \
               .withColumn("position available",
                    when(col("LATITUD").isNull() | col("LONGITUD").isNull()  |
        isnan(col("LATITUD")) | isnan(col("LONGITUD")) , "no").otherwise("yes")
                )
        df = df.filter(col('position available') == 'yes')
        for colname in df.columns:
            if re.search(r'url', colname, re.IGNORECASE):
                df = df.withColumn(colname, check_url(col(colname)))
            elif re.search(r'mail', colname, re.IGNORECASE):
                df = df.withColumn(colname, check_mail(col(colname)))
        exclude_keywords_regex = re.compile(r'(via|barrio|url|mail)', re.IGNORECASE)
        cols_to_translate = [
            column_name for column_name, dtype in df.dtypes
            if dtype == 'string' and not exclude_keywords_regex.search(column_name)
        ]
        print("(Madrid) (TITULO, DESCRIPCION):")
        df.select("TITULO", "DESCRIPCION").show(10, truncate=False)
        madrid_cols = {
            'ID-EVENTO': 'id',
            'TITULO': 'name',
            'DESCRIPCION': 'description',
            'NOMBRE-VIA-INSTALACION': 'address',
            'CONTENT-URL': 'website',
            'city': 'city',
            'LATITUD':'latitude',
            'LONGITUD':'longitude',
            'FECHA':'start_date',
            'FECHA-FIN':'end_date'
        }
        
        df = df.toDF(*[c.strip() for c in df.columns])
        df = df.select(*madrid_cols.keys())
        for old, new in madrid_cols.items():
            df = df.withColumnRenamed(old, new)
        df = df.withColumn('email', lit('not avl'))
        final_cols = [
            "id", "name", "description", "address", "email", "website",
            "city", "longitude", "latitude", "start_date", "end_date"
        ]
        df = df.select(*final_cols)
        df = df.withColumn("description", translated(col("description")))\
                .withColumn("name", translated(col("name")))
        df = df.withColumn('Point', concat(lit("POINT("),
        col("longitude"),
        lit(" "),
        col("latitude"),
        lit(")")) )
        df = df.drop("longitude", "latitude")
        results.append(df)
        tables.append('madrid_expl')
        print(" (Name, Description): Madrid")
        (
            df
                .select("name", "description") 
             
                .show(10, truncate=False)      
        )
    return results, tables




from pyspark.sql import SparkSession


if __name__ == "__main__":
    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    events = {
        "rome": 'https://dati.comune.roma.it/catalog/datastore/dump/7b45b219-b511-493d-9ca4-b347fe19b62a?bom=True',
        "paris": 'https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/que-faire-a-paris-/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B',
        "barcelona": "https://opendata-ajuntament.barcelona.cat/data/dataset/2767159c-1c98-46b8-a686-2b25b40cb053/resource/3abb2414-1ee0-446e-9c25-380e938adb73/download",
        "madrid": 'https://datos.madrid.es/egob/catalogo/300107-0-agenda-actividades-eventos.csv'    }
    storage_container_name = 'bdmcontainerp1'

    datalake_client = DataLakeServiceClient.from_connection_string(connection_string)
    file_system_client = datalake_client.get_file_system_client(storage_container_name)
    spark = SparkSession.builder \
        .appName("BDMProject") \
        .master("spark://spark-master:7077")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    dfs_pandas = landing_zone_events(file_system_client, events)
    dfs_spark, tables = trusted_zone_events(file_system_client, dfs_pandas, spark)
    for df in dfs_pandas:
        del df
    dfs_explo, tbls = explotation_zone_events(dfs_spark)
    for df in dfs_spark:
        df.unpersist()
    dfs_explo_label = []
    for df in dfs_explo:
        dfs_explo_label.append(add_category_columns(df))
    for df in dfs_explo:
        df.unpersist()
    load_spark_dfs_to_postgres(dfs_explo_label, tbls)
    for df in dfs_explo_label:
        df.unpersist()
    spark.stop()
