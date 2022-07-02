import random

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from pyvirtualdisplay import Display

display = Display(visible=0, size=(800, 600))
display.start()
# chrome driver config
options = webdriver.ChromeOptions()
# options.add_argument('--headless')
# user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 ' \
#              'Safari/537.36 '
# options.add_argument('user-agent={0}'.format(user_agent))
try:
    print("The main part of the extension ************************")
    options.add_extension('dags/docker_job/mjnbclmflcpookeapghfhapeffmpodij.crx')
except Exception as ex:
    print("Exception part of the extension ************************************************", ex)
    options.add_extension('mjnbclmflcpookeapghfhapeffmpodij.crx')

options.add_argument('--disable-infobars')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--no-sandbox')
options.add_argument("start-maximized")

options.add_argument("--dns-prefetch-disable")
options.add_argument("---force-device-scale-factor=1")

options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
# options.add_argument('--incognito')
options.add_argument("log-level=3")

# driver = webdriver.Chrome(executable_path="chromedriver", options=options)
# driver.Manage().Window.Maximize()
# driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
clickables = "//button[@id='buttonZustand']"
xpath = "//ul[@class='uk-nav uk-nav-dropdown uk-text-bold' and @id='dropdownZustand']//li[@class='{} " \
        "uk-dropdown-close'] "

iphones = ["https://www.handyverkauf.net/apple-iphone-x-64gb-space-grau_h_5143",
           "https://www.handyverkauf.net/apple-iphone-x-256gb-space-grau_h_5145",
           "https://www.handyverkauf.net/apple-iphone-xs-64gb-space-grau_h_6242",
           "https://www.handyverkauf.net/apple-iphone-xs-256gb-space-grau_h_6245",
           "https://www.handyverkauf.net/apple-iphone-xs-512gb-space-grau_h_6248",
           "https://www.handyverkauf.net/apple-iphone-xs-max-64gb-space-grau_h_6251",
           "https://www.handyverkauf.net/apple-iphone-xs-max-256gb-space-grau_h_6254",
           "https://www.handyverkauf.net/apple-iphone-xs-max-512gb-space-grau_h_6257",
           "https://www.handyverkauf.net/apple-iphone-xr-64gb-schwarz_h_6260",
           "https://www.handyverkauf.net/apple-iphone-xr-128gb-schwarz_h_6266",
           "https://www.handyverkauf.net/apple-iphone-xr-256gb-schwarz_h_6272",
           "https://www.handyverkauf.net/apple-iphone-11-64gb-schwarz_h_6998",
           "https://www.handyverkauf.net/apple-iphone-11-128gb-schwarz_h_7004",
           "https://www.handyverkauf.net/apple-iphone-11-256gb-schwarz_h_7010",
           "https://www.handyverkauf.net/apple-iphone-11-pro-64gb-space-grau_h_7016",
           "https://www.handyverkauf.net/apple-iphone-11-pro-256gb-space-grau_h_7020",
           "https://www.handyverkauf.net/apple-iphone-11-pro-512gb-space-grau_h_7024",
           "https://www.handyverkauf.net/apple-iphone-11-pro-max-64gb-space-grau_h_7028",
           "https://www.handyverkauf.net/apple-iphone-11-pro-max-256gb-space-grau_h_7032",
           "https://www.handyverkauf.net/apple-iphone-11-pro-max-512gb-space-grau_h_7036",
           "https://www.handyverkauf.net/apple-iphone-se-2020-64gb-schwarz_h_7637",
           "https://www.handyverkauf.net/apple-iphone-se-2020-128gb-schwarz_h_7640",
           "https://www.handyverkauf.net/apple-iphone-se-2020-256gb-schwarz_h_7643",
           "https://www.handyverkauf.net/apple-iphone-12-64gb-schwarz_h_8714",
           "https://www.handyverkauf.net/apple-iphone-12-128gb-schwarz_h_8719",
           "https://www.handyverkauf.net/apple-iphone-12-256gb-schwarz_h_8724",
           "https://www.handyverkauf.net/apple-iphone-12-mini-64gb-schwarz_h_8729",
           "https://www.handyverkauf.net/apple-iphone-12-mini-128gb-schwarz_h_8734",
           "https://www.handyverkauf.net/apple-iphone-12-mini-256gb-schwarz_h_8739",
           "https://www.handyverkauf.net/apple-iphone-12-pro-128gb-graphit_h_8744",
           "https://www.handyverkauf.net/apple-iphone-12-pro-256gb-graphit_h_8748",
           "https://www.handyverkauf.net/apple-iphone-12-pro-512gb-graphit_h_8752",
           "https://www.handyverkauf.net/apple-iphone-12-pro-max-128gb-graphit_h_8756",
           "https://www.handyverkauf.net/apple-iphone-12-pro-max-256gb-graphit_h_8760",
           "https://www.handyverkauf.net/apple-iphone-12-pro-max-512gb-graphit_h_8764",
           "https://www.handyverkauf.net/apple-iphone-13-128gb-schwarz_h_9725",
           "https://www.handyverkauf.net/apple-iphone-13-256gb-schwarz_h_9730",
           "https://www.handyverkauf.net/apple-iphone-13-512gb-schwarz_h_9735",
           "https://www.handyverkauf.net/apple-iphone-13-mini-128gb-schwarz_h_9740",
           "https://www.handyverkauf.net/apple-iphone-13-mini-256gb-schwarz_h_9745",
           "https://www.handyverkauf.net/apple-iphone-13-mini-512gb-schwarz_h_9750",
           "https://www.handyverkauf.net/apple-iphone-13-pro-128gb-graphit_h_9755",
           "https://www.handyverkauf.net/apple-iphone-13-pro-256gb-graphit_h_9759",
           "https://www.handyverkauf.net/apple-iphone-13-pro-512gb-graphit_h_9763",
           "https://www.handyverkauf.net/apple-iphone-13-pro-1tb-graphit_h_9767",
           "https://www.handyverkauf.net/apple-iphone-13-pro-max-128gb-graphit_h_9771",
           "https://www.handyverkauf.net/apple-iphone-13-pro-max-256gb-graphit_h_9775",
           "https://www.handyverkauf.net/apple-iphone-13-pro-max-512gb-graphit_h_9779",
           "https://www.handyverkauf.net/apple-iphone-13-pro-max-1tb-graphit_h_9783",
           "https://www.handyverkauf.net/apple-iphone-se-2022-64gb-mitternacht_h_10236",
           "https://www.handyverkauf.net/apple-iphone-se-2022-128gb-mitternacht_h_10239",
           "https://www.handyverkauf.net/apple-iphone-se-2022-256gb-mitternacht_h_10242"]
