import os
import json
import pika
import psycopg2
from flask import Flask, jsonify
from flask_cors import CORS


class FlaskAPICrimeApp:
    def __init__(self):
        self.app = Flask(__name__)
        CORS(self.app)
        self.setup_routes()
        self.db_username = os.getenv('POSTGRES_USER', 'postgres')
        self.db_password = os.getenv('POSTGRES_PASSWORD', 'postgres')
        self.db_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.db_port = os.getenv('POSTGRES_PORT', '5432')
        self.db_name = os.getenv('POSTGRES_DB', 'crime_data')
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = os.getenv('RABBITMQ_PORT', 5672)
        self.rabbitmq_username = os.getenv('RABBITMQ_USERNAME', 'guest')
        self.rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')

    def setup_routes(self):
        @self.app.route('/api/crime_data', methods=['GET'])
        def get_crime_data():
            data = self.fetch_all_crime_data()
            return jsonify(data)

        @self.app.route('/api/crime_data/rabbitmq', methods=['GET'])
        def get_crime_data_rabbitmq():
            data = self.put_crime_data_rabbitmq()
            return jsonify(data)

    def run(self):
        self.app.run(debug=False, host='0.0.0.0', port=5050)

    def fetch_all_crime_data(self):

        conn = None
        try:
            # Connect to your postgres DB
            conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_username,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port
            )
            cur = conn.cursor()

            # Fetch the data from the crime_data table
            cur.execute('SELECT latitude, longitude FROM crime_data;')
            rows = cur.fetchall()

            # Convert fetched data to desired format
            data = [{'lat': row[0], 'lng': row[1]} for row in rows]
            return data
        except Exception as e:
            print(f"Error: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def put_crime_data_rabbitmq(self):

        conn = None
        try:
            # Connect to your postgres DB
            conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_username,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port
            )
            cur = conn.cursor()

            # Fetch the data from the crime_data table
            cur.execute('SELECT object_id, crime_against, nibrs_code_name, offense_start_date, latitude, '
                        'longitude, was_a_firearm_involved FROM crime_data WHERE crime_against = '
                        '\'PERSON\';')
            rows = cur.fetchall()

            # Convert fetched data to the desired format
            data = [{'crime_id': row[0], 'crime_against': row[1], 'crime_name': row[2], 'offense_start_date': row[3],
                     'latitude': row[4], 'longitude': row[5], 'firearm_involved': row[6]} for row in rows]

            # Connect to RabbitMQ
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.rabbitmq_host, port=self.rabbitmq_port,
                                          credentials=pika.PlainCredentials(username=self.rabbitmq_username,
                                                                            password=self.rabbitmq_password)))
            channel = connection.channel()

            channel.queue_declare(queue='crime_data')

            # Iterate over each item in the data list and send it as a separate message
            for item in data:
                # Check if the item is in the sent_messages table
                cur.execute(
                    'SELECT 1 FROM sent_messages WHERE crime_id = %s;',
                    (item['crime_id'],)
                )
                if cur.fetchone():
                    print(f"Already sent message for crime_id {item['crime_id']}, skipping...")
                    continue  # Skip to the next item

                # Send the item to RabbitMQ
                item_json = json.dumps(item)
                item_bytes = bytes(item_json, 'utf-8')
                channel.basic_publish(exchange='', routing_key='crime_data', body=item_bytes)
                print(f"Sent data to RabbitMQ: {item}")

                # Insert already sent message into the sent_messages table
                cur.execute(
                    '''
                    INSERT INTO sent_messages (
                        crime_against, crime_name, crime_id, 
                        latitude, longitude, offense_start_date, firearm_involved
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
                    ''',
                    (item['crime_against'], item['crime_name'], item['crime_id'],
                     item['latitude'], item['longitude'], item['offense_start_date'], item['firearm_involved'])
                )

            # Commit the changes to the database
            conn.commit()

            connection.close()
        except Exception as e:
            print(f"Error: {e}")
            return []

        finally:
            if conn:
                conn.close()

        return data
