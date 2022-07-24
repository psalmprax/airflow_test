# FROM selenium/standalone-chrome
# FROM python:3.6-buster
# FROM dperson/torproxy
FROM apache/airflow:latest
# apache/airflow:latest
# RUN apt-get update -y \
#     && apt-get install -y python3-dev python3-pip build-essential \
#     && apt-get install gcc -y \
#     && apt-get install sudo -y \
#     && apt-get clean

# RUN adduser --disabled-password --gecos '' airflow

# ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=3000
# ENV AIRFLOW__CELERY__OPERATION_TIMEOUT=3000
# ENV AIRFLOW__CELERY__WORKER_CONCURRENCY=6
USER root
RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get -fy install
RUN apt --fix-broken install
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN echo 'deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main' | tee /etc/apt/sources.list.d/google-chrome.list
RUN apt-get update
RUN apt install -y xvfb
# RUN apt install expect -y
# RUN apt install tor -y
RUN apt-get install -y google-chrome-stable
# RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*
RUN chmod -R 777 /opt/airflow/logs
RUN chmod -R 777 /opt/airflow/dags
RUN chmod -R 777 /opt/airflow
RUN chown -R airflow:5000 /opt/airflow/logs
RUN chown -R airflow:5000 /opt/airflow
RUN chown -R airflow:5000 /opt/airflow/dags

# RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-key FDC247B7 && \
#     echo 'deb https://repo.windscribe.com/ubuntu bionic main' | sudo tee /etc/apt/sources.list.d/windscribe-repo.list && \
#     apt-get update -Y && apt-get install windscribe-cli

# COPY dags/windscribe_2.4.8_beta_amd64.deb /opt/airflow/dags/windscribe_2.4.8_beta_amd64.deb
# CMD ["./opt/airflow/dags/windscrb.exp", "windscribe", "connect"]


USER airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN pip install 'apache-airflow[amazon]'
CMD ["echo", "hello world"]
