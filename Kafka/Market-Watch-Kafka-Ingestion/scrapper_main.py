import os
import time
import json
import pytz
import requests
from tqdm import tqdm
from datetime import datetime
from bs4 import BeautifulSoup

from time import sleep
from json import dumps
from kafka import KafkaProducer

tz = pytz.timezone('America/New_York')

min_price = 0.01
max_price = 5
time_interval = 5

MW_BASE_URL = "https://www.marketwatch.com"
BASE_URL = """https://www.marketwatch.com/tools/stockresearch/screener/results.asp?submit=Screen&Symbol=true&Symbol=false&ChangePct=true&ChangePct=false&FiftyTwoWeekHigh=True&FiftyTwoWeekLow=True&CompanyName=true&CompanyName=false&Volume=true&Volume=false&PERatio=false&Price=true&Price=false&LastTradeTime=false&MarketCap=false&Change=true&Change=false&FiftyTwoWeekHigh=false&MoreInfo=true&MoreInfo=false&SortyBy=Symbol&SortDirection=Ascending&ResultsPerPage=OneHundred&TradesShareEnable=true&TradesShareMin=MINPRICE&TradesShareMax=MAXPRICE&PriceDirEnable=true&PriceDir=Up&PriceDirPct=5&LastYearEnable=false&LastYearAboveHigh=&TradeVolEnable=false&TradeVolMin=&TradeVolMax=&BlockEnable=false&BlockAmt=&BlockTime=&PERatioEnable=false&PERatioMin=&PERatioMax=&MktCapEnable=false&MktCapMin=&MktCapMax=&MovAvgEnable=false&MovAvgType=Outperform&MovAvgTime=FiftyDay&MktIdxEnable=false&MktIdxType=Outperform&MktIdxPct=&MktIdxExchange=&Exchange=All&IndustryEnable=false&Industry=Accounting&PagingIndex=PID"""
URL = BASE_URL.replace('MINPRICE', str(min_price))
URL = URL.replace('MAXPRICE', str(int(max_price)))


#producer object
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
value_serializer=lambda x:dumps(x).encode('utf-8'))


class Stock:
     """
     Object of this class will keep info of a single stock
     """
     def __init__(self, url):
          self.url = url
          self.public_float = ''
          self.volumes = []

class Screener:
     """"
     Object of this class will work for one day's stock data
     """
     def __init__(self):
          """
          data: will keep all stocks data
          today: date, to be used to stop every 5 sec volume fetch on day end
          """
          self.data = dict()
          self.today = datetime.now(tz).date()
     
     def fetch_page(self, url, rtype='requests'):
          """
          To fetch a page using url and returns its soup object.
          """
          try:
               if rtype == 'requests':
                    page = requests.get(url)
                    html = page.content

          except Exception as e:
               exit(f'Exception: {e}')

          soup = BeautifulSoup(html, 'html.parser')
          return soup

     def numeric_cleaner(self, number):
          """
          To Clean numerics like - 11,34.79M ---> 1134790000
          """
          if number != 'n/a' and number!='N/A' and number!='':
               n=1
               if ',' in  number:
                    number = ''.join(number.split(','))
          
               if 'K' in number:
                    number = number[:-1]
                    n = 10 ** 3
                    
               if 'M' in number:
                    number = number[:-1]
                    n = 10**6
               
               if 'B' in number:
                    number = number[:-1]
                    n = 10**9
                    
               number = float(number)*n
          return number
     

     def find_stocks_data(self, soup, to_fetch):
          """
          Fetches stocks data from list page
          to_fetch - 'list' when only name of stocks and n_matches is required
          to_fetch - 'vol' used to fetch volume multiple times
          """
          table = soup.find('tbody')

          if table is not None:
               rows = table.find_all('tr')
               if rows is not None:
                    for row in rows:
                         data = row.find_all('td')
                         stock_name = data[0].text

                         if to_fetch=='list':
                              stock_url = MW_BASE_URL + data[0].find('a', href=True)['href']
                              stock = Stock(stock_url)
                              self.data[stock_name] = stock
                         
                         elif to_fetch=='vol':
                              stock_volume = data[5].text
                              stock_volume = self.numeric_cleaner(stock_volume)
                              if stock_volume != 'n/a':
                                   self.ingestion_function(stock_name, int(stock_volume))

                              try:
                                   self.data[stock_name].volumes.append(stock_volume)
                              except:
                                   continue

          
     def ingestion_function(self,stock_name, stock_volume):
          ingestion_map = {}

          ingestion_map['Stock Symbol'] = stock_name
          ingestion_map['Stock Volume'] = stock_volume

          producer.send('StockValues', value = ingestion_map)
          time.sleep(5)          


     def fetch_stocks(self):
          """
          Fetches stocks from all possible pages based on n_matches
          """
          print("Fetching Stocks ...")
          paging_idx = 0
          n_matches = 1
          while len(self.data) < n_matches:
               soup = self.fetch_page(URL.replace('PID', str(paging_idx)))
               if n_matches == 1:
                    n_matches = int((soup.find_all("div", class_="floatleft results")[0].text).split()[4])

               self.find_stocks_data(soup, 'list')
               paging_idx+=100
          
          print(f'Number of Stocks found: {len(self.data)}')
     
     def fetch_volumes(self):
          """
          Fetches volumes from all possible pages
          """
          paging_idx = 0
          while paging_idx < len(self.data):
               soup = self.fetch_page(URL.replace('PID', str(paging_idx)))
               self.find_stocks_data(soup, 'vol')
               paging_idx += 100
     
     def fetch_volume_regularly(self, t=time_interval):          
          print('='*50)
          print(f"Sending Stock Data in Consumer API Every {time_interval} Seconds..")
          
          while True:
               if datetime.now(tz).date() != self.today:
                    break
               self.fetch_volumes()
               time.sleep(t)
     
     def save_hashmap(self):
          """
          Hashmap will be saved from here later
          """
          dict_to_save = {k: [v.public_float, v.volumes] for k, v in self.data.items()}
          with open('saved_data.json', 'w') as sd:
               json.dump(dict_to_save, sd)

     def start_fetching(self):
          """
          main function to start fetching data in sequential manner of function calls
          """
          print(f'Fetching on {self.today}')
          self.fetch_stocks()
          if len(self.data) == 0:
               print(f"Found 0 Stocks! Stopping Script...")
               return False

          # self.fetch_public_floats()
          self.fetch_volume_regularly()
          self.save_hashmap()
          os.system('cls' if os.name == 'nt' else 'clear')
          return True

fetch_response = True
while fetch_response:
     fetch_response = Screener().start_fetching()