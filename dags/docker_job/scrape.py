from itertools import cycle
# from time import sleep
import time as t
# from raw_copy import *
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import ProxyType, Proxy
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from webdriver_manager.chrome import ChromeDriverManager
# from parameters import iphones
from webdriver_setup import get_webdriver_for

clickables = "//button[@id='buttonZustand']"
xpath = "//ul[@class='uk-nav uk-nav-dropdown uk-text-bold' and @id='dropdownZustand']//li[@class='{} " \
        "uk-dropdown-close'] "

def get_proxies():
    """Return the list of elite proxies
    :rtype: list
    :returns: List of proxies
    """

    url = "https://www.sslproxies.org/"

    response = requests.get(url)
    parsed_page = BeautifulSoup(response.content, "html.parser")

    ip_table = parsed_page.find(id="list")
    rows = ip_table.select("tbody > tr")

    # filter proxies whose last checked time is less than 1 hour
    rows_for_last_hour = [row for row in rows if "hour" not in row.select("td:nth-of-type(8)")[0].text]

    proxies = []
    # print(rows_for_last_hour)
    for row in rows_for_last_hour:
        # print(row.contents)
        print(row.select("td:nth-of-type(5)")[0].text)
        anonymity = row.select("td:nth-of-type(5)")[0].text

        if "elite" in anonymity:
            host = row.select("td:nth-of-type(1)")[0].text
            port = row.select("td:nth-of-type(2)")[0].text
            proxy = f"{host}:{port}"
            proxies.append(proxy)

    return proxies


def setup_proxy(browser, proxy):
    """Make proxy settings for the given browser
    :type browser: str
    :param browser: Name of the browser
    :type proxy: str
    :param proxy: Proxy string in the host:port form
    :rtype: FirefoxOptions or DesiredCapabilities
    :returns: FirefoxOptions for Firefox browser, capabilities for Chrome browser
    """

    print(f"Switching proxy to {proxy}...")

    if browser == "chrome":
        proxy_config = Proxy()
        proxy_config.proxy_type = ProxyType.MANUAL
        proxy_config.http_proxy = proxy
        proxy_config.ssl_proxy = proxy

        capabilities = DesiredCapabilities.CHROME.copy()
        proxy_config.add_to_capabilities(capabilities)

        return capabilities

    else:
        raise SystemExit("Invalid browser! Should be firefox or chrome.")


def create_webdriver(browser, proxy_config):
    """Create webdriver instance for the given browser and proxy setting
    :type browser: str
    :param browser: Name of the browser
    :type proxy_config: FirefoxOptions or DesiredCapabilities
    :param proxy_config: Proxy configuration
    :rtype: selenium.webdriver
    :returns: Firefox or Chrome webdriver instance
    """

    if browser == "firefox":
        firefox_options = proxy_config
        firefox_options.headless = True
        driver = get_webdriver_for(browser, options=firefox_options)

    elif browser == "chrome":
        # ChromeDriverManager().install()

        softwares_names = [SoftwareName.CHROME.value]
        operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
        user_agent_rotator = UserAgent(softwares_names=softwares_names,
                                       operatiing_systems=operating_systems,
                                       limit=10000000)
        user_agent = user_agent_rotator.get_random_user_agent()
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('user-agent={0}'.format(user_agent))
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument("start-maximized")

        chrome_options.add_argument("--dns-prefetch-disable")
        chrome_options.add_argument("---force-device-scale-factor=1")

        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--incognito')

        chrome_options.add_argument("log-level=3")
        # driver = get_webdriver_for(browser, options=chrome_options, desired_capabilities=proxy_config)
        driver = webdriver.Chrome(executable_path="dags/docker_job/chromedriver", options=options, desired_capabilities=proxy_config)

    return driver


def make_request_with_proxy(browser, proxy_config, urls, ppd):
    """Make a request to check new IP
    Skip if connection fails.
    :type browser: str
    :param browser: Name of the browser
    :type proxy_config: FirefoxOptions or DesiredCapabilities
    :param proxy_config: Proxy configuration
    """

    driver = create_webdriver(browser, proxy_config)
    print(proxy_config)
    t.sleep(1)

    try:

        ppd.scrape_phones(urls=urls, driver=driver, clickable_links=clickables, xpath=xpath)
        # print(f"IP: {driver.find_element(By.ID, 'ip').text}")

    except WebDriverException as ex:
        print("Connection error! Skipping...", ex)

    driver.quit()


def main(urls, ppt):

    proxies = get_proxies()
    proxy_pool = cycle(proxies)

    browser = "chrome"

    for _ in range(20):
        proxy = next(proxy_pool)
        proxy_config = setup_proxy(browser, proxy)
        make_request_with_proxy(browser, proxy_config, urls, ppt)
        t.sleep(20)

# main()