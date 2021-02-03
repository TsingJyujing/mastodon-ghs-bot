FROM python:3.8
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*
ARG PIP_INDEX_URL
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
# Without CMD!