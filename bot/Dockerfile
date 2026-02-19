FROM python:3.9-slim-bookworm

# Installiamo le dipendenze per XML (necessarie per newspaper3k)
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    libxml2-dev \
    libxslt-dev \
    libjpeg-dev \
    zlib1g-dev \
    && apt-get clean;

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
CMD ["python", "main.py"]