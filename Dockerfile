ARG PIP_INDEX_URL

FROM python:3.8 as POETRY_EXPORT
WORKDIR /app
RUN pip install poetry
COPY pyproject.toml poetry.lock /app/
RUN poetry export --without-hashes -f requirements.txt -o requirements.txt

FROM python:3.8
WORKDIR /app
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*
COPY --from=POETRY_EXPORT /app/requirements.txt /app/
RUN pip install -r requirements.txt

COPY . .

# Without CMD!