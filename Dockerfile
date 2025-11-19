FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set Java home for PyFlink
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY producer.py .
COPY consumer.py .
COPY flink_spread_job.py .
COPY dashboard.py .

# Default command (can be overridden)
CMD ["python", "producer.py"]