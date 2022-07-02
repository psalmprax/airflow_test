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
           "https://www.handyverkauf.net/apple-iphone-se-2022-256gb-mitternacht_h_10242",
           "https://www.handyverkauf.net/samsung-galaxy-s22-128gb-schwarz_h_10095",
           "https://www.handyverkauf.net/samsung-galaxy-s22-256gb-schwarz_h_10103",
           "https://www.handyverkauf.net/samsung-galaxy-s22-128gb-enterprise-edition-schwarz_h_10235",
           "https://www.handyverkauf.net/samsung-galaxy-s22-plus-plus-128gb-schwarz_h_10111",
           "https://www.handyverkauf.net/samsung-galaxy-s22-plus-plus-256gb-schwarz_h_10119",
           "https://www.handyverkauf.net/samsung-galaxy-s22-ultra-128gb-schwarz_h_10127",
           "https://www.handyverkauf.net/samsung-galaxy-s22-ultra-256gb-schwarz_h_10134",
           "https://www.handyverkauf.net/samsung-galaxy-s22-ultra-512gb-schwarz_h_10141",
           "https://www.handyverkauf.net/samsung-galaxy-s22-ultra-1tb-schwarz_h_10148",
           "https://www.handyverkauf.net/samsung-galaxy-s22-ultra-128gb-enterprise-edition-schwarz_h_10445",
           "https://www.handyverkauf.net/samsung-galaxy-s21-fe-5g-128gb-graphit_h_9965",
           "https://www.handyverkauf.net/samsung-galaxy-s21-fe-5g-256gb-graphit_h_9969",
           "https://www.handyverkauf.net/samsung-galaxy-s21-fe-5g-128gb-enterprise-edition-schwarz_h_10387",
           "https://www.handyverkauf.net/samsung-galaxy-m52-5g-128gb-schwarz_h_9820",
           "https://www.handyverkauf.net/samsung-galaxy-m22-128gb-schwarz_h_9693",
           "https://www.handyverkauf.net/samsung-galaxy-z-flip-3-5g-128gb-schwarz_h_9613",
           "https://www.handyverkauf.net/samsung-galaxy-z-flip-3-5g-256gb-schwarz_h_9620",
           "https://www.handyverkauf.net/samsung-galaxy-z-fold-3-5g-256gb-schwarz_h_9627",
           "https://www.handyverkauf.net/samsung-galaxy-z-fold-3-5g-512gb-schwarz_h_9630",
           "https://www.handyverkauf.net/samsung-galaxy-s20-fe-sm-g780g-128gb-weiss_h_9443",
           "https://www.handyverkauf.net/samsung-galaxy-s21-5g-128gb-grau_h_8982",
           "https://www.handyverkauf.net/samsung-galaxy-s21-5g-128gb-enterprise-edition-grau_h_9426",
           "https://www.handyverkauf.net/samsung-galaxy-s21-plus-plus-5g-128gb-schwarz_h_8990",
           "https://www.handyverkauf.net/samsung-galaxy-s21-ultra-5g-128gb-schwarz_h_9000",
           "https://www.handyverkauf.net/samsung-galaxy-s21-ultra-5g-256gb-schwarz_h_9005",
           "https://www.handyverkauf.net/samsung-galaxy-s21-ultra-5g-512gb-schwarz_h_9010",
           "https://www.handyverkauf.net/samsung-galaxy-z-fold-2-5g-256gb-schwarz_h_8462",
           "https://www.handyverkauf.net/samsung-galaxy-z-fold-2-5g-256gb-schwarz-scharnier-silber_h_8464",
           "https://www.handyverkauf.net/samsung-galaxy-note-20-256gb-grau_h_8387",
           "https://www.handyverkauf.net/samsung-galaxy-note-20-5g-256gb-grau_h_8390",
           "https://www.handyverkauf.net/samsung-galaxy-note-20-ultra-5g-256gb-schwarz_h_8393",
           "https://www.handyverkauf.net/samsung-galaxy-note-20-ultra-5g-512gb-schwarz_h_8396",
           "https://www.handyverkauf.net/samsung-galaxy-z-flip-5g-256gb-grau_h_8401",
           "https://www.handyverkauf.net/samsung-galaxy-s20-128gb-grau_h_7491",
           "https://www.handyverkauf.net/samsung-galaxy-s20-128gb-enterprise-edition-grau_h_7683",
           "https://www.handyverkauf.net/samsung-galaxy-s20-5g-128gb-grau_h_7494",
           "https://www.handyverkauf.net/samsung-galaxy-s20-plus-plus-128gb-schwarz_h_7497",
           "https://www.handyverkauf.net/samsung-galaxy-s20-plus-plus-5g-128gb-schwarz_h_7500",
           "https://www.handyverkauf.net/samsung-galaxy-s20-plus-plus-5g-512gb-schwarz_h_7503",
           "https://www.handyverkauf.net/samsung-galaxy-s20-ultra-5g-128gb-schwarz_h_7504",
           "https://www.handyverkauf.net/samsung-galaxy-s20-ultra-5g-512gb-schwarz_h_7506",
           "https://www.handyverkauf.net/samsung-galaxy-z-flip-256gb-schwarz_h_7513",
           "https://www.handyverkauf.net/apple-ipad-air-5-2022-64gb-space-grau_t_3165",
           "https://www.handyverkauf.net/apple-ipad-air-5-2022-256gb-space-grau_t_3170",
           "https://www.handyverkauf.net/apple-ipad-air-5-2022-64gb-5g-space-grau_t_3175",
           "https://www.handyverkauf.net/apple-ipad-air-5-2022-256gb-5g-space-grau_t_3180",
           "https://www.handyverkauf.net/apple-ipad-102-zoll-2021-64gb-space-grau_t_2983",
           "https://www.handyverkauf.net/apple-ipad-102-zoll-2021-256gb-space-grau_t_2985",
           "https://www.handyverkauf.net/apple-ipad-102-zoll-2021-64gb-4g-space-grau_t_2987",
           "https://www.handyverkauf.net/apple-ipad-102-zoll-2021-256gb-4g-space-grau_t_2989",
           "https://www.handyverkauf.net/apple-ipad-mini-6-2021-64gb-space-grau_t_2991",
           "https://www.handyverkauf.net/apple-ipad-mini-6-2021-256gb-space-grau_t_2995",
           "https://www.handyverkauf.net/apple-ipad-mini-6-2021-64gb-5g-space-grau_t_2999",
           "https://www.handyverkauf.net/apple-ipad-mini-6-2021-256gb-5g-space-grau_t_3003",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2021-128gb-space-grau_t_2912",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2021-256gb-space-grau_t_2914",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2021-512gb-space-grau_t_2916",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2021-1tb-space-grau_t_2918",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2021-2tb-space-grau_t_2920",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2021-128gb-5g-space-grau_t_2922",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2021-256gb-5g-space-grau_t_2924",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2021-512gb-5g-space-grau_t_2926",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2021-1tb-5g-space-grau_t_2928",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2021-2tb-5g-space-grau_t_2930",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2021-128gb-space-grau_t_2932",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2021-256gb-space-grau_t_2934",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2021-512gb-space-grau_t_2936",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2021-1tb-space-grau_t_2938",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2021-2tb-space-grau_t_2940",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2021-128gb-5g-space-grau_t_2942",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2021-256gb-5g-space-grau_t_2944",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2021-512gb-5g-space-grau_t_2946",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2021-1tb-5g-space-grau_t_2948",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2021-2tb-5g-space-grau_t_2950",
           "https://www.handyverkauf.net/apple-ipad-air-4-2020-64gb-space-grau_t_2810",
           "https://www.handyverkauf.net/apple-ipad-air-4-2020-256gb-space-grau_t_2815",
           "https://www.handyverkauf.net/apple-ipad-air-4-2020-64gb-4g-space-grau_t_2820",
           "https://www.handyverkauf.net/apple-ipad-air-4-2020-256gb-4g-space-grau_t_2825",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2020-128gb-space-grau_t_2582",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2020-256gb-space-grau_t_2584",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2020-512gb-space-grau_t_2586",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2020-1tb-space-grau_t_2588",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2020-128gb-4g-space-grau_t_2590",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2020-256gb-4g-space-grau_t_2592",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2020-512gb-4g-space-grau_t_2594",
           "https://www.handyverkauf.net/apple-ipad-pro-11-zoll-2020-1tb-4g-space-grau_t_2596",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2020-128gb-space-grau_t_2598",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2020-256gb-space-grau_t_2600",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2020-512gb-space-grau_t_2602",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2020-1tb-space-grau_t_2604",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2020-128gb-4g-space-grau_t_2606",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2020-256gb-4g-space-grau_t_2608",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2020-512gb-4g-space-grau_t_2610",
           "https://www.handyverkauf.net/apple-ipad-pro-129-zoll-2020-1tb-4g-space-grau_t_2612"
           ]
