import pandas as pd
import time
import requests
from bs4 import BeautifulSoup
import logging


logger = logging.getLogger('data_acquisition')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('log/data_acquisition_failure.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

soccer_requests = requests.get("https://www.football-data.co.uk/englandm.php")
assert soccer_requests.status_code == 200, 'There is something wrong with the connection, check the URL!'

soccer = BeautifulSoup(soccer_requests.text)

main_url = 'https://www.football-data.co.uk/'
match_type = [
    'Premier League',
    'Championship',
    'League 1',
    'League 2',
    'Conference',
    'Division 1',
    'Division 2',
    'Division 3'
]

for m_type in match_type:
    links = soccer.find_all('a', text=m_type)
    for link in links:
        sub_url = link["href"]
        path = 'data/raw_data/' + m_type + '_' + sub_url[-11: -7] + '.csv'
        try:
            pd.read_csv(main_url + sub_url, error_bad_lines=False).to_csv(path)
        except:
            logger.debug(f'Fail to get: {m_type}_{sub_url[-11: -7]}.csv')
        time.sleep(1)
