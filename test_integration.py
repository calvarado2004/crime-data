import pytest
from flask_api_app import FlaskAPICrimeApp
from crime_data_fetcher import CrimeDataRetriever
from db_injector import DBInjector

TEST_CSV_PATH = './test_crime_data.csv'


# Integration test using the real DB and the real RabbitMQ
@pytest.fixture(scope='module')
def setup_database():
    injector = DBInjector()
    injector.inject_csv_to_db()
    yield


@pytest.fixture(scope='module')
def app_instance():
    return FlaskAPICrimeApp()


@pytest.fixture(scope='module')
def crime_data_retriever():
    return CrimeDataRetriever()


def test_database_connection(setup_database, app_instance):
    data = app_instance.fetch_all_crime_data()
    assert data is not None
    assert len(data) > 0


def test_rabbitmq_connection(app_instance):
    sample_data = {
        'crime_id': 12345,
        'crime_against': 'PERSON',
    }
    client = app_instance.app.test_client()
    response = client.get('/api/crime_data/rabbitmq?queue=crime_data_test')
    assert response.status_code == 200


def test_end_to_end_flow(setup_database, app_instance, crime_data_retriever):
    crime_data_retriever.retrieve_crimes()
    data = app_instance.fetch_all_crime_data()
    assert data is not None
    assert len(data) > 0


def test_flask_endpoints(setup_database, app_instance):
    client = app_instance.app.test_client()
    response = client.get('/api/crime_data')
    assert response.status_code == 200

    response_rabbitmq = client.get('/api/crime_data/rabbitmq')
    assert response_rabbitmq.status_code == 200
