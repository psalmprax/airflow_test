FROM apache/airflow:latest as prod
# python:3.6-buster

# RUN apt-get update -y \
#     && apt-get install -y python3-dev python3-pip build-essential \
#     && apt-get install gcc -y \
#     && apt-get install sudo -y \
#     && apt-get clean

# RUN adduser --disabled-password --gecos '' airflow
USER airflow
COPY requirements.txt requirements_2.txt
RUN pip install -r requirements_2.txt
CMD ["echo", "hello world"]