import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import re

class EGATRealTimeScraper:
    def __init__(self, url="https://www.sothailand.com/sysgen/egat/"):
        self.url = url
        self.driver = None

    def _initialize_driver(self):
        if not self.driver:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--log-level=0")
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def extract_data_from_page(self):
        logs = self.driver.get_log('browser')
        for log_entry in logs:
            message = log_entry.get('message', '')
            if 'updateMessageArea:' in message:
                match = re.search(r'updateMessageArea:\s*(\d+)\s*,\s*(\d{1,2}:\d{2})\s*,\s*([\d,]+\.\d+)\s*,\s*(\d+\.\d+)', message)
                if match:
                    display_date_id, display_time_str = match.group(1).strip(), match.group(2).strip()
                    current_value_mw = float(match.group(3).replace(',', '').strip())
                    temperature_c = float(match.group(4).strip())
                    return {
                        'scrape_datetime_utc': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'source_date_id': display_date_id,
                        'source_time_str': display_time_str,
                        'current_value_mw': current_value_mw,
                        'temperature_c': temperature_c,
                        'data_source_url': self.url
                    }
        return None

    def scrape_once(self):
        if not self.driver:
            self._initialize_driver()
        self.driver.get(self.url)
        self.driver.implicitly_wait(5)
        data = self.extract_data_from_page()
        return pd.DataFrame([data]) if data else pd.DataFrame()

    def close_driver(self):
        if self.driver:
            self.driver.quit()
            self.driver = None