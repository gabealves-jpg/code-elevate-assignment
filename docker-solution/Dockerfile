FROM apache/spark-py:latest
USER root

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY main_docker.py .
COPY analysis.py .
COPY tests/ ./tests/

RUN pytest /app/tests

# Copy application files
COPY . .

# Run the Spark application with Python 3
CMD ["python3", "main_docker.py"]