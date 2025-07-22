FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH "/"

# Copy application code
COPY app/tweet_producer.py .

# Create data directory
RUN mkdir -p /app/data

CMD ["python", "tweet_producer.py"]
