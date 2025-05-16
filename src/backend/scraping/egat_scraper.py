import time
import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import os
import re
from config.path_config import EGAT_SCRAPE_URL, EGAT_LOCAL_RAW_CSV_FILE

class EGATRealTimeScraper:
    def __init__(self, url=EGAT_SCRAPE_URL, output_file=EGAT_LOCAL_RAW_CSV_FILE):
        self.url = url
        self.output_file = str(output_file)
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--log-level=0")
        chrome_options.set_capability('goog:loggingPrefs', {'browser': 'ALL'})
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    def extract_data_from_console(self):
        for log in self.driver.get_log('browser'):
            if 'updateMessageArea:' in log.get('message', ''):
                match = re.search(r'updateMessageArea:\s*(\d+)\s*,\s*(\d{1,2}:\d{2})\s*,\s*([\d,]+\.\d+)\s*,\s*(\d+\.\d+)', log['message'])
                if match:
                    return {
                        'scrape_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'display_date_id': match.group(1).strip(),
                        'display_time': match.group(2).strip(),
                        'current_value_MW': float(match.group(3).replace(',', '')),
                        'temperature_C': float(match.group(4))
                    }
        return None

    def save_to_csv(self, data):
        if data:
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            pd.DataFrame([data]).to_csv(self.output_file, 
                mode='a' if os.path.exists(self.output_file) else 'w',
                header=not os.path.exists(self.output_file),
                index=False)

    def scrape_once(self):
        self.driver.get(self.url)
        time.sleep(10)
        data = self.extract_data_from_console()
        if data:
            self.save_to_csv(data)
            return self.output_file
        return None

    def scrape_continuously(self, interval_minutes=5):
        while True:
            start_time = time.time()
            self.scrape_once()
            time.sleep(max(0, (interval_minutes * 60) - (time.time() - start_time)))

    def close(self):
        if self.driver:
            self.driver.quit()
            self.driver = None