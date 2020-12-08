FROM python:3.7
ARG PIP_INDEX_URL
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
# Without CMD!