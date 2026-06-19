FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV DELTA_VERSION=3.2.0 \
    SCALA_BINARY_VERSION=2.12 \
    DELTA_JARS_DIR=/opt/delta-jars \
    SPARK_DELTA_JARS=/opt/delta-jars/*

RUN mkdir -p "${DELTA_JARS_DIR}" && \
    curl -fsSL -o "${DELTA_JARS_DIR}/delta-spark_${SCALA_BINARY_VERSION}-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_BINARY_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_BINARY_VERSION}-${DELTA_VERSION}.jar" && \
    curl -fsSL -o "${DELTA_JARS_DIR}/delta-storage-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" && \
    curl -fsSL -o "${DELTA_JARS_DIR}/antlr4-runtime-4.9.3.jar" \
      "https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

CMD ["python", "-u", "consumer.py"]
