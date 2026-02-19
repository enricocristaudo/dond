FROM python:3.9-slim-bookworm

# Dipendenze base per Java/Spark
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps build-essential && \
    apt-get clean;

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download Modello
RUN python -c "from transformers import pipeline; pipeline('zero-shot-classification', model='MoritzLaurer/mDeBERTa-v3-base-mnli-xnli')"

COPY processor.py .
CMD ["python", "processor.py"]