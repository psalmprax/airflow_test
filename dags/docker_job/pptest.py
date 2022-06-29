from __future__ import print_function

import random
import time
from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.hooks.postgres_hook import PostgresHook
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait


class ProductPriceData:
    driver: None

    def __init__(self, driver=None):
        self.driver = driver

    def get_product_cat_url(self, url, driver):
        """
            url: product url to get the pricing details
        Returns:
            object:
        """
        lst = list()
        self.driver = driver
        self.driver.get(url)
        time.sleep(5)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        for div in soup.findAll("div", {"class": "uk-width-1-2"}):
            lst.append(div.find("a", href=True).get('href'))
        self.driver.quit()

        return lst

    @classmethod
    def clean_list(cls, lst, string):
        """
            (1) take out 'gebraucht-kaufen' because we only want sourcing (verkaufen not kaufen)
            (2) take out '//'
            (3) Using format()
            (4) append https so the string functions as a url
        Returns:
            object:
        """
        lst = [string + '{0}'.format(i) for i in [ele.strip('//')
                                                  for ele in [x for x in lst if "gebraucht-kaufen" not in x]]]
        return lst

    def get_prices(self, driver, clickables):
        """
        Returns:
            object:
        """
        self.driver = driver
        prices, anbieters = list(), list()
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        get_prices = soup.findAll("span", {"class": "uk-hidden-small uk-h1 uk-text-primary pricetag"})
        device = soup.find("h1", class_="handy_name").text
        text = self.driver.find_element_by_xpath(clickables).get_attribute('textContent').strip()

        if "AM" in datetime.today().strftime("%D:%M:%Y:%H:%M %p"):
            time_of_day = "AM"
        else:
            time_of_day = "PM"

        for href in get_prices:
            anbieter = href.find("a", href=True)
            prices.append(anbieter.text)
            anbieters.append(anbieter.get('href'))

        dct = {'Prices': prices,
               'Anbieter': anbieters,
               'Device': [device] * len(anbieters),
               'Condition': [text] * len(anbieters),
               'Created_at': [date.today()] * len(anbieters),
               'Am_Pm': [time_of_day] * len(anbieters)}
        return dct

    # def (self, urls, clickable_links, driver, xpath ):
    def scrape_phones(self, urls, driver, clickable_links, xpath):
        dfs = list()
        self.driver = driver
        clickable_links = clickable_links
        xpath = xpath
        for url in urls:
            try:
                for val_class in [2, 3, 4, 5, 1]:
                    if val_class == 2:
                        # sleep for a random time between 2 & 5 secs before scraping next url
                        time.sleep(random.uniform(2, 5))
                        self.driver.get(url)

                    # open dropdown menu again
                    WebDriverWait(self.driver, 20).until(
                        expected_conditions.element_to_be_clickable((By.XPATH, clickable_links))).click()

                    # click conditiona status (sehr gut,gut, ****)
                    self.driver.find_element_by_xpath(xpath.format(val_class)).click()
                    time.sleep(5)
                    status = self.get_prices(self.driver, clickable_links)
                    dfs.append(status)
            except Exception as ex:
                print(ex)

        self.driver.quit()

        return dfs


def run_from_class(**kwargs):
    # from docker_job.pricing.vars import options, clickables, xpath, iphones, driver
    from airflow.hooks.postgres_hook import PostgresHook
    from webdriver_manager.chrome import ChromeDriverManager

    pg_hook = PostgresHook(postgres_conn_id='pricing')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    ppd = ProductPriceData()
    result = list()
    print(kwargs["url"])
    phone_data = ppd.scrape_phones([kwargs["url"]],
                                   kwargs["driver"],
                                   kwargs["clickables"], kwargs["xpath"])
    result += phone_data
    results = [(a.strip("€"), b, c, d, str(e), f) for row in result
               for a, b, c, d, e, f in zip(row['Prices'],
                                           row['Anbieter'],
                                           row['Device'],
                                           row['Condition'],
                                           row['Created_at'],
                                           row['Am_Pm'])]
    insert_query = "insert into analytics_objects.obj_product_price_stage (price,anbieter,device,condition," \
                   "created_at) values {} "
    insert_query = insert_query.format(','.join(['%s'] * len(results)))
    cursor.execute(insert_query, results)
    cursor.execute("COMMIT")
    print(results)
    # return {"data": result}


# noinspection PyShadowingNames
def create_dag(
        dag_id,
        schedule,
        task_id,
        description,
        start_date,
        default_args,
        my_func
):
    dag = DAG(schedule_interval=schedule,
              default_args=default_args,
              description=description,
              dag_id=dag_id,
              start_date=start_date,
              catchup=False,
              template_searchpath=['/opt/airflow/dags'])
    with dag:
        # python_task = PythonOperator(
        #     task_id=task_id,
        #     provide_context=True,
        #     python_callable=my_func
        # )

        create_product_price_table = PostgresOperator(
            task_id=f"table_{task_id}_creation",
            postgres_conn_id="pricing",
            sql="""sql/create_product_price_table.sql"""
        )

        upsert_product_price_table = PostgresOperator(
            task_id=f"table_{task_id}_upsert",
            postgres_conn_id="pricing",
            sql="""sql/upsert_product_price_table.sql"""
        )

        start_clean = BashOperator(
            task_id=f'start_clean-{task_id}',
            bash_command=f"pkill chrome 2>/dev/null || exit 0 "
                         f"&& rm -rf /tmp/.* 2>/dev/null || exit 0"
                         f"&& rm -rf /tmp/* 2>/dev/null || exit 0"
                         f"&& rm -rf /tmp/.p* 2>/dev/null || exit 0"
        )

        end_clean = BashOperator(
            task_id=f'end_clean-{task_id}',
            bash_command=f"pkill chrome 2>/dev/null || exit 0 "
                         f"&& rm -rf /tmp/.* 2>/dev/null || exit 0"
                         f"&& rm -rf /tmp/* 2>/dev/null || exit 0"
                         f"&& rm -rf /tmp/.p* 2>/dev/null || exit 0"
        )
        from docker_job.pricing.vars import iphones
        from docker_job.pricing.vars import options, clickables, xpath, iphones, driver
        for url in iphones[:10]:
            ti = url.split("/")[-1]
            print(ti)
            python_task = PythonOperator(
                task_id=f"{task_id}-{ti}",
                provide_context=True,
                python_callable=my_func,
                op_kwargs={"url": url, 'driver': driver, 'clickables': clickables, 'xpath': xpath, 'options': options}
            )
            # >> update_product_price_table , create_product_price_table >> ,upsert_product_price_table >>
            start_clean >> create_product_price_table >> python_task >> upsert_product_price_table >> end_clean
    return dag


task_id = "pptest"

default_args = {
    'depends_on_past': False,
    'email': ['samuel.momoh-olle@asgoodasnew.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(seconds=2000),
}
description = f'A {task_id} DAG '
schedule_interval = timedelta(minutes=150)
dag_id = f'Job-{task_id}'
start_date = datetime(2022, 6, 15)

globals()[dag_id] = create_dag(dag_id=dag_id,
                               schedule=schedule_interval,
                               task_id=task_id,
                               description=description,
                               start_date=start_date,
                               default_args=default_args,
                               my_func=run_from_class)
