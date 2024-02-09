from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from database import DatabaseHelper
from query import Query
import time
import json 
import pandas as pd
from datetime import datetime
from webdriver_manager.chrome import ChromeDriverManager
import subprocess





class Scrape:

    def __init__(self):
        self.database = DatabaseHelper()
        self.new_df = None 

    def data(self):
        # Setup Selenium Webdriver
        df = self.database.fetch(Query.urls)

        df['swiggy_url'] = df['platform_data'].apply(lambda x: 
            next((item.get('swiggy', {}).get('url', None) for item in json.loads(x) if isinstance(item, dict)), None)
            if isinstance(json.loads(x), list) else json.loads(x).get('swiggy', {}).get('url', None)
        )

        df['zomato_url'] = df['platform_data'].apply(lambda x: 
            next((item.get('zomato', {}).get('url', None) for item in json.loads(x) if isinstance(item, dict)), None)
            if isinstance(json.loads(x), list) else json.loads(x).get('zomato', {}).get('url', None)
        )

        url_data = df[['id','name' ,'swiggy_url', 'zomato_url']]
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # Run Chrome in headless mode
        options.add_argument('--no-sandbox')  # Bypass OS security model, required on many Linux systems
        options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems
        options.add_argument('--disable-gpu')  # Disable GPU hardware acceleration, if not applicable
        options.add_argument('--window-size=1920,1080')  # Specify window size
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36")
        # chromedriver_path = '/usr/bin/local/chromedriver'
        # options.binary_location = chromedriver_path
        service = Service(ChromeDriverManager().install())
        swiggy_url_driver = webdriver.Chrome(service=service,options=options)
        zomato_url_driver = webdriver.Chrome(service=service,options=options)


        for index, row in url_data.iterrows():
            # URL of the webpage to be scraped
            name =row['name']
            print(f'scrapeing for : {name}')
            swiggy_url = row['swiggy_url']
            zomato_url = row['zomato_url']
            today = datetime.today()
            now = datetime.now()

            # Format the date and time as a string
            formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")

            if swiggy_url and swiggy_url.startswith(('http://', 'https://')):
                time.sleep(5)
                swiggy_url_driver.get(swiggy_url)
                # Wait for JavaScript to load
                time.sleep(7)
                # Extract Swiggy rating
                swiggy_xpath = '//*[@id="root"]/div[1]/div[1]/div/div/div[2]/div[2]/div[1]/button/span[1]/span[2]'
                swiggy_rating_element = swiggy_url_driver.find_element(By.XPATH, swiggy_xpath)
                swiggy_rating_value = swiggy_rating_element.text
                if swiggy_rating_value in ['--', 'new','NEW']:
                    swiggy_rating_value = 0
                print(f"Swiggy Rating {name}: {float(swiggy_rating_value)}")
                id = row["id"]
                insert_data = {
                    "store_id": id,
                    "aggregator": "Swiggy",
                    "rating": float(swiggy_rating_value),
                    "date": today,
                    "created_at": formatted_now,
                    "updated_at": formatted_now
                }

                insert_data_tuple = tuple(insert_data.values())

                try:
                    # Assuming self.database.insertData handles the connection and cursor
                    insert_result = self.database.insertData(Query.insert_query, parameters=insert_data_tuple)
                    print("Data inserted successfully")
                except Exception as e:
                    print(f"An error occurred: {e}")
            else:
                print(f"Skipping Swiggy URL for {name} due to invalid URL.")

            # Check and open Zomato URL
            if zomato_url and zomato_url.startswith(('http://', 'https://')):
                time.sleep(5)
                zomato_url_driver.get(zomato_url)
                # Wait for JavaScript to load
                time.sleep(7)
                # Extract Zomato rating
                zomato_xpath = '//*[@id="root"]/div/main/div/section[3]/section/section/div/div/div/section/div[3]/div[1]/div/div/div[1]'
                zomato_rating_element = zomato_url_driver.find_element(By.XPATH , zomato_xpath)
                zomato_rating_value = zomato_rating_element.text
                if zomato_rating_value in ['--', 'NEW','new']:
                    zomato_rating_value = 0
                print(f"Zomato Rating {name}: {zomato_rating_value}")
                insert_data = {
                    "store_id": id,
                    "aggregator": "Zomato",
                    "rating": zomato_rating_value,
                    "date": today,
                    "created_at": formatted_now,
                    "updated_at": formatted_now
                }

                insert_data_tuple = tuple(insert_data.values())

                try:
                    # Assuming self.database.insertData handles the connection and cursor
                    insert_result = self.database.insertData(Query.insert_query, parameters=insert_data_tuple)
                    print("Data inserted successfully")
                except Exception as e:
                    print(f"An error occurred: {e}")
                
                
            else:
                print(f"Skipping Zomato URL for {name} due to invalid URL.")


        


if __name__ == '__main__':
    dg = Scrape()
    chrome_version = webdriver.__version__
    print(f"Chrome Version: {chrome_version}")

 
