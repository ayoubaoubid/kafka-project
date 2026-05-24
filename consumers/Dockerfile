FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python hdfs pandas
COPY . .
CMD ["python", "${CONSUMER_SCRIPT}"]