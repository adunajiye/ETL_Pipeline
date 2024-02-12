import csv
from datetime import datetime
from turtle import update
import psycopg2
from Transform import transform_new_data
from sqlalchemy import create_engine

def load_df_postgres():
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
                    host="qfa-dlp-pre-prod-postgresql-fra-do-user-13185867-0.b.db.ondigitalocean.com",
                    database="qfa_db",
                    user="doadmin",
                    password="AVNS_GeCGRs_qkQt-JHiY12N",
                    port=25060)

        cur = conn.cursor()   
         # Create a SQLAlchemy engine for easier DataFrame to PostgreSQL loading
        engine = create_engine('postgresql://doadmin:AVNS_GeCGRs_qkQt-JHiY12N@qfa-dlp-pre-prod-postgresql-fra-do-user-13185867-0.b.db.ondigitalocean.com:25060/qfa_db')
        data = transform_new_data()

        table_name = 'ETL_Table'

        data.to_sql(table_name, engine, if_exists='replace', index=False)

        print('Data loaded successfully!')

        
            # print(data)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        print('Database connection closed.')
load_df_postgres()