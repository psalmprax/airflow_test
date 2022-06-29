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
USER root
RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get -fy install \
  && apt install -y xvfb
RUN apt --fix-broken install
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN echo 'deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main' | tee /etc/apt/sources.list.d/google-chrome.list
RUN apt-get update
RUN apt-get install -y google-chrome-stable
RUN chmod -R 777 /opt/airflow/logs
RUN chmod -R 777 /opt/airflow
RUN chown -R airflow:5000 /opt/airflow/logs
RUN chown -R airflow:5000 /opt/airflow

USER airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
CMD ["echo", "hello world"]
