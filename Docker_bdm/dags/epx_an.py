

import psycopg2

import geopandas as gpd






from sqlalchemy import create_engine
def query_spatial_event(city, filter_sql=""):
    conn_str = "postgresql://user:password@postgres-db:5432/postgres"
    engine = create_engine(conn_str)

    base_query = f'SELECT * FROM public.{city}_expl'
    if filter_sql:
        base_query += f' WHERE {filter_sql}'

    print("Running:", base_query)
    gdf = gpd.read_postgis(base_query, engine, geom_col="Point")

    if gdf.empty:
        print(" No rows returned.")
        return

    output_table = f"{city}_filtered"
    gdf.to_postgis(output_table, engine, if_exists='replace', index=False)
    print(f"Saved filtered result to table: {output_table}")

