import pandas as pd
import sqlite3
import os

def run_storage():
    # 1. Cargar el archivo Parquet
    df = pd.read_parquet('../nuclear_outages.parquet')

    df = df.rename(columns={
        'period': 'date',
        'facility': 'facility_id',
        'facilityName': 'facility_name',
        'generator': 'generator_num',   # It is given by a number from 1 to 5
        'capacity': 'capacity_mw',  # mw = megawatts
        'outage': 'outage_mw',      # mw = megawatts
        'percentOutage': 'percent'
    })

    if os.path.exists('nuclear_outages.db'):
        os.remove('nuclear_outages.db')
    conn = sqlite3.connect('nuclear_outages.db') # Connect to SQLite and create file
    cursor = conn.cursor()

    # table creation, 'Generator' has a composite PK, 'Facility' also has a composite FK
    cursor.executescript('''
        -- Plant Catalog Table
        CREATE TABLE IF NOT EXISTS Facility (
            facility_id INTEGER PRIMARY KEY,
            facility_name TEXT
        );

        -- Generator Catalog Table 
        CREATE TABLE IF NOT EXISTS Generator (
            facility_id INTEGER,
            generator_num INTEGER,
            PRIMARY KEY (facility_id, generator_num),
            FOREIGN KEY (facility_id) REFERENCES Facility(facility_id)
        );

        -- Outage Report Table,
    CREATE TABLE IF NOT EXISTS Outage_report (
            report_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE,
            facility_id INTEGER,
            generator_num INTEGER,
            capacity_mw FLOAT,
            outage_mw FLOAT,
            percent FLOAT,
            FOREIGN KEY (facility_id, generator_num) REFERENCES Generator(facility_id, generator_num)
        );
    ''')

    # FACILITY table
    df_facility = df[['facility_id', 'facility_name']].drop_duplicates(subset=['facility_id'])

    # GENERATOR table
    df_generator = df[['facility_id', 'generator_num']].drop_duplicates(subset=['facility_id', 'generator_num'])

    # OUTAGE_REPORT table
    report_columns = ['date', 'facility_id', 'generator_num', 'capacity_mw', 'outage_mw', 'percent']
    df_report = df[report_columns]

    # upload the plants (facilities), generators, and reports.
    df_facility.to_sql('Facility', conn, if_exists='append', index=False)
    df_generator.to_sql('Generator', conn, if_exists='append', index=False)
    df_report.to_sql('Outage_report', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    run_storage()