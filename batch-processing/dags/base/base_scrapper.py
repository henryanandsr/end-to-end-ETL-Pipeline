import pandas as pd
from abc import ABC, abstractmethod

class BaseScrapper(ABC):
    def __init__(self, url, title, location, tags):
        self.url = url
        self.title = title
        self.location = location
        self.tags = tags
    
    @abstractmethod 
    def scrape(self) -> pd.DataFrame:
        """
        Scrape data from source and return it as DataFrame

        Returns:
            pd.DataFrame: Dataframe contains data source
        """
        pass
