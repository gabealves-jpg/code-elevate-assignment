FROM apache/spark-py:latest
USER root

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt



# Copy application files
COPY . .

# Run the Spark application with Python 3
CMD ["python3", "main-docker.py"]