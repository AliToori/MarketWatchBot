#!/usr/bin/env python3
"""
    *******************************************************************************************
    MarketWatchBot.
    Author: Ali Toori, Python Developer [Bot Builder]
    Website: https://boteaz.com
    YouTube: https://youtube.com/@AliToori
    *******************************************************************************************
"""
import concurrent.futures
import datetime
import logging.config
import os
import time
from pathlib import Path
from time import sleep
import pandas as pd
import requests
import random
import numpy as np
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    'formatters': {
        'colored': {
            '()': 'colorlog.ColoredFormatter',  # colored output
            # --> %(log_color)s is very important, that's what colors the line
            'format': '[%(asctime)s] %(log_color)s%(message)s'
        },
    },
    "handlers": {
        "console": {
            "class": "colorlog.StreamHandler",
            "level": "INFO",
            "formatter": "colored",
            "stream": "ext://sys.stdout"
        }
    },
    "root": {
        "level": "INFO",
        "handlers": [
            "console"
        ]
    }
})

LOGGER = logging.getLogger()


class MWatchBot:

    def __init__(self):
        self.PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
        self.PROJECT_ROOT = Path(self.PROJECT_ROOT)
        self.file_data_1 = self.PROJECT_ROOT / 'MWatchRes/Data1.csv'
        self.file_data_2 = self.PROJECT_ROOT / 'MWatchRes/Data2.csv'
        self.delays = [0.5, 1]

    # Get user-agent
    def get_user_agent(self):
        file_uagents = self.PROJECT_ROOT / 'MWatchRes/user_agent.txt'
        with open(file_uagents) as f:
            content = f.read().strip()
        return content

    # Get driver with proxy and user-agent
    def get_driver(self, headless=True):
        LOGGER.info("--------------------------------------------------")
        LOGGER.info("Starting browser")
        DRIVER_BIN = str(self.PROJECT_ROOT / "MWatchRes/bin/chromedriver_win32.exe")
        options = webdriver.ChromeOptions()
        # options.add_argument("--proxy-server={}".format(proxy))
        options.add_argument("--start-maximized")
        options.add_argument(f'--user-agent={self.get_user_agent()}')
        options.add_argument("--log-level=3")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-blink-features")
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        if headless:
            options.add_argument("--headless")
        proxy_driver = webdriver.Chrome(executable_path=DRIVER_BIN, options=options, service_log_path=os.devnull)
        LOGGER.info("--------------------------------------------------")
        return proxy_driver

    # Method to shutdown the browser
    def finish(self, driver):
        try:
            driver.close()
            driver.quit()
        except WebDriverException as exc:
            LOGGER.info('Problem occurred while closing the WebDriver instance ...', exc.args)

    def wait_until_visible(self, driver, xpath=None, element_id=None, name=None, class_name=None, tag_name=None, css_selector=None, duration=10000, frequency=0.01):
        if xpath:
            WebDriverWait(driver, duration, frequency).until(EC.visibility_of_element_located((By.XPATH, xpath)))
        elif element_id:
            WebDriverWait(driver, duration, frequency).until(EC.visibility_of_element_located((By.ID, element_id)))
        elif name:
            WebDriverWait(driver, duration, frequency).until(EC.visibility_of_element_located((By.NAME, name)))
        elif class_name:
            WebDriverWait(driver, duration, frequency).until(EC.visibility_of_element_located((By.CLASS_NAME, class_name)))
        elif tag_name:
            WebDriverWait(driver, duration, frequency).until(EC.visibility_of_element_located((By.TAG_NAME, tag_name)))
        elif css_selector:
            WebDriverWait(driver, duration, frequency).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, css_selector)))

    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_analyst_estimates(self, symbols_list):
        ANALYST_PASS_CRITERIA = 0.75
        # drivers = [self.get_driver(), self.get_driver()]
        driver = self.get_driver()
        master_estimates = dict()
        # symbols_chunks = [symbols_list[i:i + 2] for i in range(0, len(symbols_list), 2)]
        for i, symbol in enumerate(symbols_list):
            # symbols_list = list(zip(drivers, symbol_chunk))
            # print(f"Processing chunk : {str(i)}: {symbol_chunk}")
            try:
                print(f"{str(i)} Getting analyst estimates for : {symbol}")
                # print(f"{index} Analyst estimates for : {symbol} : {estimates}")
                estimates = self.get_analyst_estimates_for_symbol(driver=driver, symbol=symbol)
                master_estimates[symbol] = estimates.copy()
            except:
                print(f"Exception for analyst estimates : {symbol}")
                continue
            # with concurrent.futures.ProcessPoolExecutor() as executor:
            #     for index, symbol, estimates in zip(range(len(symbols_list)), symbols_list, executor.map(self.get_analyst_estimates_for_symbol, symbols_list)):
            #         try:
            #             print(f"{str(i * int(index[0]))} Getting analyst estimates for : {symbol}")
            #             # print(f"{index} Analyst estimates for : {symbol} : {estimates}")
            #             master_estimates[symbol] = estimates.copy()
            #         except:
            #             print(f"Exception for analyst estimates : {symbol}")
            #             continue

        estimates_df = pd.DataFrame(master_estimates).transpose()
        estimates_df = estimates_df.replace('N/A', 0)
        estimates_df = estimates_df.astype(int)
        estimates_df = estimates_df.reset_index().rename(columns={'index': 'ticker'})
        estimates_df['3m_perc'] = estimates_df[['3m_buy', '3m_overweight']].astype(int).sum(axis=1) / \
                                  estimates_df[[i for i in estimates_df.columns if '3m_' in i]].astype(int).sum(axis=1)
        estimates_df['1m_perc'] = estimates_df[['1m_buy', '1m_overweight']].astype(int).sum(axis=1) / \
                                  estimates_df[[i for i in estimates_df.columns if '1m_' in i]].astype(int).sum(axis=1)
        estimates_df['curr_perc'] = estimates_df[['curr_buy', 'curr_overweight']].astype(int).sum(axis=1) / \
                                    estimates_df[[i for i in estimates_df.columns if 'curr_' in i]].astype(int).sum(
                                        axis=1)
        estimates_df['analyst_result'] = np.where((estimates_df['3m_perc'] > ANALYST_PASS_CRITERIA) &
                                                  (estimates_df['1m_perc'] > ANALYST_PASS_CRITERIA) &
                                                  (estimates_df['curr_perc'] > ANALYST_PASS_CRITERIA), 'PASS', 'FAIL')
        # for driver in drivers:
        self.finish(driver=driver)
        return estimates_df.fillna(0)

    def get_analyst_estimates_for_symbol(self, driver, symbol):
        delay = random.choice(self.delays)
        print(f"Waiting for {delay} secs")
        sleep(delay)
        # driver = symbol[0]
        # symbol = symbol[1]
        symbol = str(symbol).replace('-', '.')
        url = f'https://www.marketwatch.com/investing/stock/{symbol.lower()}/analystestimates?mod=mw_quote_tab'
        try:
            driver.get(url=url)
            html_soup = BeautifulSoup(driver.page_source, 'html.parser')
            data_response = html_soup.find_all('div', attrs={'class': 'bar-chart'})
            if len(data_response) == 0:
                return
            Buy_3M_AGO = data_response[0].text.strip()
            Buy_1M_AGO = data_response[1].text.strip()
            Buy_CURRENT = data_response[2].text.strip()

            Overweight_3M_AGO = data_response[3].text.strip()
            Overweight_1M_AGO = data_response[4].text.strip()
            Overweight_CURRENT = data_response[5].text.strip()

            Hold_3M_AGO = data_response[6].text.strip()
            Hold_1M_AGO = data_response[7].text.strip()
            Hold_CURRENT = data_response[8].text.strip()

            Underweight_3M_AGO = data_response[9].text.strip()
            Underweight_1M_AGO = data_response[10].text.strip()
            Underweight_CURRENT = data_response[11].text.strip()

            Sell_3M_AGO = data_response[12].text.strip()
            Sell_1M_AGO = data_response[13].text.strip()
            Sell_CURRENT = data_response[14].text.strip()

            data = {'3m_buy': Buy_3M_AGO, '3m_overweight': Overweight_3M_AGO, '3m_hold': Hold_3M_AGO,
                    '3m_underweight': Underweight_3M_AGO, '3m_sell': Sell_3M_AGO,
                    '1m_buy': Buy_1M_AGO, '1m_overweight': Overweight_1M_AGO, '1m_hold': Hold_1M_AGO,
                    '1m_underweight': Underweight_1M_AGO, '1m_sell': Sell_1M_AGO,
                    'curr_buy': Buy_CURRENT, 'curr_overweight': Overweight_CURRENT, 'curr_hold': Hold_CURRENT,
                    'curr_underweight': Underweight_CURRENT, 'curr_sell': Sell_CURRENT}

            data1 = {'3m_buy': [Buy_3M_AGO], '3m_overweight': [Overweight_3M_AGO], '3m_hold': [Hold_3M_AGO],
                    '3m_underweight': [Underweight_3M_AGO], '3m_sell': [Sell_3M_AGO],
                    '1m_buy': [Buy_1M_AGO], '1m_overweight': [Overweight_1M_AGO], '1m_hold': [Hold_1M_AGO],
                    '1m_underweight': [Underweight_1M_AGO], '1m_sell': [Sell_1M_AGO],
                    'curr_buy': [Buy_CURRENT], 'curr_overweight': [Overweight_CURRENT], 'curr_hold': [Hold_CURRENT],
                    'curr_underweight': [Underweight_CURRENT], 'curr_sell': [Sell_CURRENT]}
            df = pd.DataFrame(data1)
            # if file does not exist write headers
            if not os.path.isfile(self.file_data_1):
                df.to_csv(self.file_data_1, index=False)
            else:  # else if exists so append without writing the header
                df.to_csv(self.file_data_1, mode='a', header=False, index=False)
            # self.finish(driver=driver)
            return data.copy()
        except:
            print(f'TimeOut exception for {symbol}')





def main():
    PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
    PROJECT_ROOT = Path(PROJECT_ROOT)
    file_path_account = PROJECT_ROOT / "MWatchRes/symbols_list.csv"
    # Create MWatchBot instance
    mwatch = MWatchBot()
    symbols_list = pd.read_csv('symbols_list.csv')['symbol'].tolist()
    estimates = mwatch.get_analyst_estimates(symbols_list)
    df = pd.DataFrame(estimates)
    # if file does not exist write headers
    if not os.path.isfile(mwatch.file_data_2):
        df.to_csv(mwatch.file_data_2, index=False)
    else:  # else if exists so append without writing the header
        df.to_csv(mwatch.file_data_2, mode='a', header=False, index=False)
    print(estimates)


if __name__ == '__main__':
    main()
