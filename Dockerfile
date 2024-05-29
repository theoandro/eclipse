FROM python:3.10

WORKDIR /app

COPY . .

RUN apt-get update
RUN apt-get install default-jdk -y
RUN pip install -e .[dev]

CMD ["eclipse-ingestion-task"]