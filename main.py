import threading
import time

from flask_api_app import FlaskAPICrimeApp
from crime_data_fetcher import CrimeDataRetriever


def send_to_rabbitmq():
    app_instance = FlaskAPICrimeApp()
    app_instance.put_crime_data_rabbitmq()


if __name__ == "__main__":

    print("Sleeping for 15 seconds to wait for RabbitMQ.")
    time.sleep(15)

    run_flask_app = FlaskAPICrimeApp().run
    run_data_fetcher = CrimeDataRetriever().start

    # Start Flask app
    flask_thread = threading.Thread(target=run_flask_app)
    flask_thread.start()

    # Start Crime Data Fetcher
    data_fetcher_thread = threading.Thread(target=run_data_fetcher)
    data_fetcher_thread.start()

    while True:
        print("Sleeping for 60 seconds to wait for data_fetcher.")
        time.sleep(60)
        print("Sending messages to RabbitMQ...")
        send_data_to_rabbitmq_thread = threading.Thread(target=send_to_rabbitmq)
        send_data_to_rabbitmq_thread.start()
        time.sleep(3600)
        print("Sleeping for 1 hour...")
