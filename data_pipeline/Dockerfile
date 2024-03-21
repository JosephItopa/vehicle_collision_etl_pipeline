FROM prefecthq/prefect:2.7.7-python3.8

WORKDIR /.
COPY vehicle_collision_etl.py .
COPY deep-contact-credentials.json .
COPY requirements.txt.py .
RUN pip install -r requirements.txt

ENV PREFECT_API_URL=http://server:4200/api

CMD ["python", "vehicle_collision_etl.py"]