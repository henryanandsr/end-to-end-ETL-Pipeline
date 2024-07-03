import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup

from dags.base.base_scrapper import BaseScrapper

class TiketScrapper(BaseScrapper):
    def __init__(self, url, title, location, tags):
        super().__init__(url, title, location, tags)
    
    def scrape(self) -> pd.DataFrame:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--remote-debugging-port=9222')
        
        # Connect to container
        driver = webdriver.Remote(
            command_executor='http://selenium:4444/wd/hub',
            options=options
        )
        driver.get(self.url)

        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'card-title'))
        )

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'card-title'))
        )

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.quit()

        titles = [item.text.strip() for item in soup.find_all(self.title, class_='card-title line-clamp-2 levertitle')]
        locations = []
        tags = [item.text.strip() for item in soup.find_all(self.tags, class_='badge bg-warning')]

        job_type_loc = soup.find_all(self.location, class_= 'd-flex flex-row')

        for job_item in job_type_loc:
            location_divs = job_item.find_all('div', class_='mb-2')
            location = location_divs[1].text.strip() if location_divs else ''
            locations.append(location)

        # Create DataFrame
        data = {
            'title': titles,
            'location': locations,
            'tags': tags
        }
        df = pd.DataFrame(data)
        return df