FROM python:3.11-slim
WORKDIR /app

# Install Java required for PySpark
RUN apt-get update && \
    apt-get install -y default-jre && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --timeout 300
COPY . .
CMD ["sh", "-c", "python \"$CONSUMER_SCRIPT\""]
