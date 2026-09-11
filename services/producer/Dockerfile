# services/producer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gdelt_producer.py .

CMD ["python", "-u", "gdelt_producer.py"]
