import re
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql.functions import udf
from bs4 import BeautifulSoup
from pyspark.sql.types import StringType, DoubleType, IntegerType
import html
import validators
import psycopg2
import hashlib
from deep_translator import GoogleTranslator
from email_validator import validate_email, EmailNotValidError
from pyspark.sql.functions import col, lower, concat_ws, array, lit
from pyspark.sql import functions as F

def empty_pandas_df(columns=None):
    return pd.DataFrame(columns=columns) if columns else pd.DataFrame()

@udf(StringType())
def check_url(url):
    if url is None:
        return None
    if validators.url(url):
        return url
    else:
        return None

@udf(StringType())
def check_mail(email):
    if email is None:
        return None
    try:
        validate_email(email)
        return email
    except EmailNotValidError:
        return None

@udf(StringType())
def sha256_udf(url: str) -> str:
    return hashlib.sha256(url.encode('utf-8')).hexdigest() if url else None

@udf(StringType())
def translated(text):
    if not isinstance(text, str) or not text.strip():
        return text
    try:
        return GoogleTranslator(source='auto', target='en').translate(text=text)
    except Exception:
        return text

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

@udf(StringType())
def fixtextformat(text):
    if not isinstance(text, str):
        return text
    text = re.sub(r'\s+([.,!?;:])', r'\1', text)
    text = re.sub(r'([.,!?;:])([^\s])', r'\1 \2', text)
    text = re.sub(r'(^|[.!?]\s+)([a-záéíóúñ])', lambda m: m.group(1) + m.group(2).upper(), text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

@udf(StringType())
def checkendpharase(text):
    if not isinstance(text, str):
        return text
    text = text.strip()
    if re.search(r'[.!?]$', text):
        return text
    if re.search(r'[\W_]$', text):
        return re.sub(r'[\W_]+$', '.', text)
    if re.search(r'[A-Za-z0-9]$', text):
        return text + '.'
    return text

@udf(StringType())
def formatdata(text):
    pattern = r'\b(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\b'
    def replacer(match):
        day, month, year = match.groups()
        day = int(day)
        month = int(month)
        year = int(year)
        if year < 100:
            year += 2000
        try:
            date = datetime(year, month, day)
            return date.strftime('%-d %B %Y')  
        except ValueError:
            return match.group(0) 
    return re.sub(pattern, replacer, text)

@udf(StringType())
def take_mail_val(cell):
    if cell is None:
        return None
    mails = re.split(r'[;,\s\-]+', str(cell))
    regex = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
    val = [email for email in mails if regex.match(email)]
    if val:
        return '; '.join(val)  
    else:
        return None

@udf(StringType())
def nan_if_slash(x):
    return None if str(x).strip() == '/' else x

@udf(StringType())
def fix_french_syntax(text):
    if text is None:
        return text
    text = html.unescape(str(text)).strip()
    text = re.sub(r'\s+([.,;:!?])', r'\1', text)
    text = re.sub(r'([.,;:!?])(?=\S)', r'\1 ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = re.sub(r'([?!]){2,}', r'\1', text)
    text = re.sub(r'(^|[.!?]\s+)([a-zàâçéèêëîïôûùüÿñæœ])', lambda m: m.group(1) + m.group(2).upper(), text)
    if text:
        text = text[0].upper() + text[1:]
    return text

@udf(StringType())
def extract_lat_lon(coord_string):
    if pd.isna(coord_string):
        return np.nan, np.nan
    try:
        lat, lon = map(str.strip, coord_string.split(','))
        return float(lat), float(lon)
    except:
        return np.nan, np.nan

@udf(StringType())
def clean_html(text):
    if text is None:
        return text
    return BeautifulSoup(str(text), "html.parser").get_text(separator=" ").strip()

@udf(IntegerType())
def clean_postal_code_p(code_postal, ville):
    try:
        code = int(float(str(code_postal).strip()))
        if 75001 <= code <= 75020 and str(ville).strip().lower() == 'paris':
            return code
        else:
            return None
    except:
        return None

@udf(StringType())
def textformatspa(text):
    if text is None:
        return text
    text = str(text).strip()
    text = re.sub(r'\s+([.,;:!?])', r'\1', text)
    text = re.sub(r'([.,;:!?])(?=\S)', r'\1 ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = re.sub(r'([?!¡¿]){2,}', r'\1', text)
    text = re.sub(r'(^|[.!?]\s+)([a-záéíóúñ])', lambda m: m.group(1) + m.group(2).upper(), text)
    if text:
        text = text[0].upper() + text[1:]
    if re.search(r'\?\s*$', text) and not text.startswith('¿'):
        text = '¿' + text
    if re.search(r'!\s*$', text) and not text.startswith('¡'):
        text = '¡' + text
    return text

@udf(IntegerType())
def clean_postal_code_b(code_postal, addresses_town):
    try:
        code = int(float(str(code_postal).strip()))
        if 8001 <= code <= 8042 and str(addresses_town).strip().lower() == 'barcelona':
            return code
        else:
            return None
    except:
        return None

valid_days = {'L', 'M', 'X', 'J', 'V', 'S', 'D'}

@udf(StringType())
def clean_weekdays(value):
    if value is None:
        return None
    days = [d.strip().capitalize() for d in str(value).replace(';', ',').replace(' ', ',').split(',')]
    filtered_days = [d for d in days if d in valid_days]
    return ','.join(filtered_days) if filtered_days else None

@udf(IntegerType())
def clean_postal_code_m(val):
    try:
        code = int(float(str(val).strip()))
        if 28001 <= code <= 28080:
            return code
        else:
            return None
    except:
        return None

def load_spark_dfs_to_postgres(
    spark_dfs,
    table_names,
    pg_host="postgres-db",
    pg_port="5432",
    pg_db="your_bd",
    pg_user="your_username",
    pg_password="your_pwrd",
    pg_schema="public"
):
    pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    driver = "your_driver"
    assert len(spark_dfs) == len(table_names)

    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_db,
            user=pg_user,
            password=pg_password
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        for tablename in table_names:
            full_table = f"{pg_schema}.{tablename}"
            cur.execute(f"DROP TABLE IF EXISTS {full_table};")
    except Exception as e:
        return

    for df, tablename in zip(spark_dfs, table_names):
        full_table = f"{pg_schema}.{tablename}"
        df.write \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", full_table) \
            .option("user", pg_user) \
            .option("password", pg_password) \
            .option("driver", driver) \
            .mode("overwrite") \
            .save()
        try:
            cur.execute(f"""ALTER TABLE public.{tablename}
                            ALTER COLUMN "Point" TYPE geometry(Point, 4326)
                            USING ST_SetSRID(ST_GeomFromText("Point"), 4326);""")
        except Exception as e:
            pass

    cur.close()
    conn.close()

def add_category_columns(df):
    categories = {
        "SPORT": ['sport', 'football', 'soccer', 'tennis', 'basketball', 'gym', 'swimming'],
        "NATURE": ['park', 'garden', 'nature', 'forest', 'mountain', 'river', 'lake'],
        "CULTURE": ['museum', 'theatre', 'theater', 'art', 'history', 'exhibition', 'gallery', 'library', 'concert', 'cultural'],
        "SOCIAL_LIFE": ['bar', 'pub', 'club', 'nightlife', 'restaurant', 'social', 'event', 'party', 'festival'],
    }
    df = df.withColumn(
        "fulltext", 
        lower(concat_ws(" ", col("name"), col("description")))
    )
    for label, keywords in categories.items():
        condition = F.lit(False)
        for word in keywords:
            condition = condition | (F.instr(col("fulltext"), word) > 0)
        df = df.withColumn(label, F.when(condition, 1).otherwise(0))
    df = df.drop("fulltext")
    return df
