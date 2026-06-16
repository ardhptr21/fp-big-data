FROM python:3.11-slim

WORKDIR /app

# Install system dependencies + Java for PySpark
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
