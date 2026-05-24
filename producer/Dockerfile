FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install kafka-python pandas
COPY . .
CMD ["python", "producer.py"]