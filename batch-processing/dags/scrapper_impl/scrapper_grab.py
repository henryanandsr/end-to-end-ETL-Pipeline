import pandas as pd
import requests
from bs4 import BeautifulSoup

from base.base_scrapper import BaseScrapper

class GrabScrapper(BaseScrapper):
    def __init__(self, url, title, location, tags):
        super().__init__(url, title, location, tags)
    
    def scrape(self) -> pd.DataFrame:
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Scrap Title, Location, and Tags
        titles = [item.text.strip() for item in soup.find_all(self.title, class_='stretched-link js-view-job')]
        locations = []
        tags = []
        job_meta = soup.find_all(self.location, class_='list-inline job-meta')
        
        for job_item in job_meta:
            meta_item = job_item.find_all('li', class_='list-inline-item')
            location = meta_item[0].text.strip()
            locations.append(location)
            tag = meta_item[1].text.strip()
            tags.append(tag)

        # Create DataFrame
        data = {
            'title': titles,
            'location': locations,
            'tags': tags
        }
        df = pd.DataFrame(data)
        return df