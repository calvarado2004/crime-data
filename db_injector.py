import os
import pandas as pd
from sqlalchemy import create_engine


class DBInjector:
    def __init__(self):
        self.db_username = os.getenv('POSTGRES_USER', 'postgres')
        self.db_password = os.getenv('POSTGRES_PASSWORD', 'postgres')
        self.db_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.db_port = os.getenv('POSTGRES_PORT', '5432')
        self.db_name = os.getenv('POSTGRES_DB', 'crime_data')

    def inject_csv_to_db(self):
        df = pd.read_csv('./crime_data.csv')

        column_mappings = {
            'OBJECTID': 'object_id',
            'Report Number': 'report_number',
            'Report Date': 'report_date',
            'Offense Start Date': 'offense_start_date',
            'Offense End Date': 'offense_end_date',
            'Day of the week': 'day_of_the_week',
            'Day Number': 'day_number',
            'Zone': 'identified_zone',
            'Beat': 'identified_beat',
            'Location': 'identified_location',
            'Location Type': 'location_type',
            'NIBRS Code': 'nibrs_code',
            'NIBRS Code Name': 'nibrs_code_name',
            'Crime Against': 'crime_against',
            'Was a firearm involved?': 'was_a_firearm_involved',
            'Press Release': 'press_release',
            'Social Media': 'social_media',
            'Watch': 'watch',
            'Longitude': 'longitude',
            'Latitude': 'latitude',
            'Neighborhood': 'neighborhood',
            'NPU': 'npu',
            'Council District': 'council_district',
            'UCR Grouping': 'ucr_grouping',
            'Victim Count': 'victim_count',
            'GlobalID': 'global_id',
            'x': 'x',
            'y': 'y'
        }
        df.rename(columns=column_mappings, inplace=True)

        alchemyEngine = create_engine(
            f'postgresql+psycopg2://{self.db_username}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}',
            pool_recycle=3600)

        dbConnection = alchemyEngine.connect()

        # Create a table from the DataFrame. If the table exists, replace it.
        df.to_sql('crime_data', dbConnection, if_exists='replace', index=False)

        print("CSV Inserted into Postgres DB")

        # Close the database connection
        dbConnection.close()
