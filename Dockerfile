FROM --platform=linux/amd64 python:3.11-slim

WORKDIR /app

COPY requirements.txt requirements.txt
COPY flask_api_app.py flask_api_app.py
COPY db_injector.py db_injector.py
COPY crime_data_fetcher.py crime_data_fetcher.py
COPY metrics.py metrics.py
COPY main.py main.py

RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "main.py"]
