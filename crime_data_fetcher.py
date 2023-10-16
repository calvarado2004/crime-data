import time
import json
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from db_injector import DBInjector


class CrimeDataRetriever(DBInjector):
    BASE_URL = (
        "https://services3.arcgis.com/Et5Qfajgiyosiw4d/arcgis/rest/services/CrimeDataExport_2_view/FeatureServer"
        "/createReplica")

    def __init__(self):
        super().__init__()
        self.start_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")
        self.end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.filename = 'crime_data.csv'

    # retrieve crimes every hour
    def start(self):
        while True:
            print("Retrieving crimes...")
            self.retrieve_crimes()
            time.sleep(3600)
            print("Sleeping for 1 hour...")

    def get_where_clause(self):
        return (f"(((report_Date BETWEEN timestamp '{self.start_date}' AND timestamp '{self.end_date}' AND "
                f"(report_Date <> timestamp '{self.end_date}'))))")

    @staticmethod
    def construct_post_data(where_clause):
        return {
            "f": "json",
            "layers": 1,
            "layerQueries": json.dumps({
                "1": {
                    "where": where_clause,
                    "useGeometry": False,
                    "queryOption": "useFilter",
                    "fields": "nibrs_code_name,nibrs_code,UCR_Grouping,report_number,report_Date,offense_start_date,"
                              "offense_end_date,Day_of_the_week,Day_Number,e2,location,location_type,Zone,Beat,NhoodName,"
                              "NPU,DISTRICT,victim_count,nibrs_crime_against,was_firearm_invloved,press_release,"
                              "social_media,long,lat"
                }
            }),
            "transportType": "esriTransportTypeUrl",
            "returnAttachments": False,
            "returnAttachmentsDataByUrl": True,
            "async": True,
            "syncModel": "none",
            "targetType": "client",
            "syncDirection": "download",
            "attachmentsSyncDirection": "none",
            "dataFormat": "csv"
        }

    def fetch_crime_data(self):
        where_clause = self.get_where_clause()
        data = self.construct_post_data(where_clause)
        response = requests.post(self.BASE_URL, data=data)
        if response.status_code == 200:
            return response.json()
        else:
            return f"Request failed with status code: {response.status_code}"

    @staticmethod
    def get_csv_url(status_url):
        while True:
            response = requests.get(status_url)
            if response.status_code != 200:
                print("Error fetching status. Retrying in 10 seconds...")
                time.sleep(10)
                continue

            soup = BeautifulSoup(response.content, 'lxml')
            result_url_tag = soup.find(string="Result Url:")
            if result_url_tag:
                csv_url_tag = result_url_tag.find_next('a', href=True)
                if csv_url_tag and 'cancel' not in csv_url_tag['href']:
                    return csv_url_tag['href']

            time.sleep(10)

    def save_csv(self, csv_url):
        csv_content = requests.get(csv_url).text
        with open(self.filename, 'w') as f:
            f.write(csv_content)
        print(f"CSV saved to: {self.filename}")

    def retrieve_crimes(self):
        result = self.fetch_crime_data()
        print(f"Job result: {result}")
        csv_url = self.get_csv_url(result["statusUrl"])
        print(f"CSV available at: {csv_url}")
        self.save_csv(csv_url)
        self.inject_csv_to_db()
