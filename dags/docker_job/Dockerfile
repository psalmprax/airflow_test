
FROM apache/airflow:latest

USER root
RUN  apt-get update \
  && apt-get install -y wget \
  && apt-get -y install zip unzip \
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
RUN chmod -R 777 /opt/airflow/logs
RUN chmod -R 777 /opt/airflow/dags
RUN chmod -R 777 /opt/airflow
RUN chown -R airflow:5000 /opt/airflow/logs
RUN chown -R airflow:5000 /opt/airflow
RUN chown -R airflow:5000 /opt/airflow/dags

USER airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN pip install 'apache-airflow[amazon]'
CMD ["echo", "hello world"]
