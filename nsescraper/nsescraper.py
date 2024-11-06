# Importing Necessary Libraries
import pandas as pd
import requests
from datetime import datetime, timedelta
from pytz import timezone
import pickle
import pathlib
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from io import StringIO
import dateutil.parser as parser

# Getting the file path
HERE = pathlib.Path(__file__).parent.resolve()

# Company symbol finder
class ValueError(Exception):
    pass

class Stock():
    def __init__(self, identifier:str):
        """For scrapping stock historical informations.

        Args:
            stock_name (str): Listed Company/Stock/Index
        """
        self.identifier       = identifier
        self.max_retries      = 10
        self.backoff_factor   = 0.5
        self.status_forcelist = [500, 502, 503, 504]
        self.head             = {
                                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                                              "Chrome/87.0.4280.88 Safari/537.36 "}
        self.date_format      = "%d-%b-%Y"
        self.HERE             = pathlib.Path(__file__).parent.resolve()
        self.retry            = Retry(total             = self.max_retries,
                                      backoff_factor    = self.backoff_factor,
                                      status_forcelist  = self.status_forcelist)
        self.adapter          = (HTTPAdapter(max_retries=self.retry))
        self.search_url       = 'https://www.nseindia.com/api/search/autocomplete?q={}'
        self.get_details      = 'https://www.nseindia.com/api/quote-equity?symbol={}'
    
    def identifier_finder(self):
        name = self.identifier.replace(' ', '')
        session = requests.Session()
        session.mount('https://',
                      self.adapter)
        try:
            session.get('https://www.nseindia.com/',
                        headers = self.head)
            search_results = session.get(url     = self.search_url.format(name),
                                         headers = self.head)
            search_result  = search_results.json()['symbols'][0]['symbol']
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        except (IndexError, KeyError) as e:
            raise ValueError("Error: Symbol not found or invalid response from server. Please try again.",e) from None
        try:
            company_details = session.get(url     = self.get_details.format(search_result),
                                          headers = self.head)
            identifier = company_details.json()['info']['identifier']
            return identifier
        except KeyError as e:
            raise ValueError("Error: Unable to retrieve company identifier from server response.\nPlease try again with valid stock name",e) from None
        finally:
            session.close()
    
    def symbol_finder(self):
        company_name   = self.identifier.replace(' ', '')
        session        = requests.Session()
        session.mount('https://',
                      self.adapter)
        try:
            session.get('https://www.nseindia.com/',
                        headers = self.head)
            search_results = session.get(url     = self.search_url.format(company_name),
                                         headers = self.head)
            search_result  = search_results.json()['symbols'][0]['symbol']
            return str(search_result)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        except (IndexError, KeyError) as e:
            raise ValueError("Error: Symbol not found or invalid response from server. Please try again.") from None
        finally:
            session.close()
    
    def historical_ohlc(self,
                        from_date:str = (datetime.today().date() 
                                         - timedelta(days=365)).strftime("%d-%m-%Y"),
                        to_date:str   =  datetime.today().date().strftime("%d-%m-%Y")
                       )-> pd.DataFrame:
        """This function scraps historical stock data from NSE. Maximum historical data will be one year.

        Args:
            from_date ("DD-MM-YYYY", optional): Starting date in "DD-MM-YYYY" format. Defaults to today's date.
            to_date ("DD-MM-YYYY", optional): Ending date in "DD-MM-YYYY" format. Defaults to exact one year.

        Returns:
            pd.DataFrame: Daily candlestick data.
        """
        try:
            from_date = parser.parse(from_date,
                                     dayfirst= True)
            to_date   = parser.parse(to_date,
                                     dayfirst= True)
        except Exception as e:
            raise ValueError("Error: Invalid date format. Please use 'DD-MM-YYYY'.",e)
        if not (from_date <= to_date):
            raise ValueError("Error: Invalid date range. Starting date (from_date) should be earlier than ending date (to_date).")
        from_date = from_date.strftime('%d-%m-%Y')
        to_date   = to_date.strftime('%d-%m-%Y')
        company        = self.symbol_finder()
        session        = requests.Session()
        session.mount('https://',
                      self.adapter)
        try:
            session.get("https://www.nseindia.com",
                        headers = self.head)
            session.get("https://www.nseindia.com/get-quotes/equity?symbol=" 
                        + company, 
                        headers = self.head)
            session.get("https://www.nseindia.com/api/historical/cm/equity?symbol="
                        +company,
                        headers = self.head)
            url     = ("https://www.nseindia.com/api/historical/cm/equity?symbol=" 
                       + company 
                       + "&series=[%22EQ%22]&from=" 
                       + from_date 
                       + "&to=" 
                       + to_date 
                       + "&csv=true")
            webdata = session.get(url     = url,
                                  headers = self.head)
            company_historical_dataframe         = pd.read_csv(StringIO(webdata.text[3:]))
            company_historical_dataframe.columns = [str(x).lower().replace(' ','') for x in company_historical_dataframe.columns]
            company_historical_dataframe['date'] = pd.to_datetime(company_historical_dataframe['date'],
                                                                  format   = self.date_format)
            company_historical_dataframe[['volume', 'value','nooftrades']] = company_historical_dataframe[['volume',
                                                                                                           'value',
                                                                                                           'nooftrades']
                                                                                                          ].apply(
                                                                                                                    lambda x: pd.to_numeric(x.str.replace(',', '')
                                                                                                                                            )
                                                                                                                )
            company_historical_dataframe.loc[:,'symbol'] = company                                                                                  
            return company_historical_dataframe
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        finally:
            session.close()
    
    def intraday_ohlc(self,
                      tick:bool = False,
                      candlestick: int = 1) -> pd.DataFrame:
        """This function scrapes current date's listed companies spot data for the given stock name.

        Args:
            tick (bool, optional): If True returns per second tick price data . Defaults to False.
            candlestick (int, optional): Candle period in Minutes . Defaults to 1 Minute.

        Returns:
            pd.DataFrame: Intra Day stock data
        """
        stock_name = self.identifier_finder()
        session = requests.Session()
        session.mount('https://', self.adapter)
        try:
            session.get("https://www.nseindia.com",
                        headers=self.head)
            company_spot_data = pd.DataFrame(session.get(f"https://www.nseindia.com/api/chart-databyindex?index={str.upper(stock_name)}",
                                                         headers= self.head).json()['grapthData'])
            session.close()
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        company_spot_data.rename({0:"timestamp",1:"ltp"},
                                 axis    = 1,
                                 inplace = True)
        company_spot_data['timestamp'] = pd.to_datetime(company_spot_data['timestamp'],
                                                        unit='ms',
                                                        origin='unix')
        if tick:
            company_spot_data.loc[:,'symbol'] = stock_name.replace('EQN','')
            return company_spot_data
        else:
            company_spot_data = company_spot_data.set_index(company_spot_data['timestamp'])
            company_spot_data = company_spot_data[['ltp']]
            company_spot_data = company_spot_data['ltp'].resample(f'{candlestick}Min').ohlc()
            company_spot_data.loc[:,'symbol'] = stock_name.replace('EQN','')
            return company_spot_data.reset_index()
        
    def trade_reports(self,
                      from_date:str = (datetime.today().date() 
                                      - timedelta(days=100)).strftime("%d-%m-%Y"),
                      to_date:str   =  datetime.today().date().strftime("%d-%m-%Y")) -> pd.DataFrame:
        """This function scrapes the Security-wise Price volume & Deliverable position data from NSE website.

        Args:
            from_date (str, optional): Starting date in "DD-MM-YYY" format. Defaults to today's date.
            to_date (str, optional): Ending date in "DD-MM-YYY" format. Defaults to exact one year.

        Returns:
            pd.DataFrame
        """
        try:
            from_date = parser.parse(from_date,
                                     dayfirst= True)
            to_date   = parser.parse(to_date,
                                     dayfirst= True)
        except Exception as e:
            raise ValueError("Error: Invalid date format. Please use 'DD-MM-YYYY'.",e)
        if not (from_date <= to_date):
            raise ValueError("Error: Invalid date range. Starting date (from_date) should be earlier than ending date (to_date).")
        from_date    = from_date.strftime('%d-%m-%Y')
        to_date      = to_date.strftime('%d-%m-%Y')
        stock_symbol = self.symbol_finder()
        session      = requests.Session()
        session.mount('https://',
                      self.adapter)
        try:
            session.get("https://www.nseindia.com",
                        headers = self.head)
            session.get("https://www.nseindia.com/all-reports", 
                        headers = self.head)
            session.get("https://www.nseindia.com/report-detail/eq_security",
                        headers = self.head)
            url = f"https://www.nseindia.com/api/historical/securityArchives?from={from_date}&to={to_date}&symbol={stock_symbol}&dataType=priceVolumeDeliverable&series=EQ"
            res = session.get(url     = url,
                              headers = self.head).json()
            res = pd.DataFrame(res['data'])
            res.rename(columns= {'CH_SYMBOL':'symbol',
                                 'CH_TIMESTAMP':'date',
                                 'COP_DELIV_QTY':'deliverable_qty',
                                 'COP_DELIV_PERC': '%dly_qt_to_traded_qty',
                                 'CH_OPENING_PRICE':'open',
                                 'CH_TRADE_HIGH_PRICE':'high',
                                 'CH_TRADE_LOW_PRICE': 'low',
                                 'CH_CLOSING_PRICE': 'close',
                                 'CH_LAST_TRADED_PRICE':'ltp',
                                 'CH_PREVIOUS_CLS_PRICE':'prev_close',
                                 'CH_52WEEK_HIGH_PRICE':'52week_high',
                                 'CH_52WEEK_LOW_PRICE':'52week_low',
                                 'CH_TOT_TRADED_QTY':'total_traded_qty',
                                 'CH_TOT_TRADED_VAL':'turnover',
                                 'CH_TOTAL_TRADES':'total_trades',
                                 'VWAP':'vwap'},inplace= True)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        finally:
            session.close()
        return res[['symbol',
                    'date',
                    'deliverable_qty',
                    '%dly_qt_to_traded_qty',
                    'open',
                    'high',
                    'low',
                    'close',
                    'ltp',
                    'prev_close',
                    '52week_high',
                    '52week_low',
                    'total_traded_qty',
                    'turnover',
                    'total_trades',
                    'vwap']]
    
    def bulk_deals(self,from_date:str = (datetime.today().date() 
                                      - timedelta(days=100)).strftime("%d-%m-%Y"),
                        to_date:str   =  datetime.today().date().strftime("%d-%m-%Y")
                  ) -> pd.DataFrame:
        """This fucntion scraps the bulk deal/block deal data from NSE website.

        Args:
            from_date (str, optional): Starting date in "DD-MM-YYY" format. Defaults to today's date.
            to_date (str, optional): Ending date in "DD-MM-YYY" format. Defaults to exact one year.

        Returns:
            pd.DataFrame
        """
        try:
            from_date = parser.parse(from_date,
                                     dayfirst= True)
            to_date   = parser.parse(to_date,
                                     dayfirst= True)
        except Exception as e:
            raise ValueError("Error: Invalid date format. Please use 'DD-MM-YYYY'.",e)
        if not (from_date <= to_date):
            raise ValueError("Error: Invalid date range. Starting date (from_date) should be earlier than ending date (to_date).")
        from_date    = from_date.strftime('%d-%m-%Y')
        to_date      = to_date.strftime('%d-%m-%Y')
        stock_symbol = self.symbol_finder()
        session      = requests.Session()
        session.mount('https://',
                      self.adapter)
        try:
            session.get("https://www.nseindia.com",
                        headers = self.head)
            session.get("https://www.nseindia.com/all-reports", 
                        headers = self.head)
            session.get("https://www.nseindia.com/report-detail/display-bulk-and-block-deals",
                        headers = self.head)
            url = f"https://www.nseindia.com/api/historical/bulk-deals?symbol={stock_symbol}&from={from_date}&to={to_date}"
            res = session.get(url     = url,
                              headers = self.head).json()
            res = pd.DataFrame(res['data'])
            if len(res) <= 0:
                raise ValueError(f"Data not found in between {from_date} to {to_date}")
            res.rename(columns= {'BD_DT_DATE':'date',
                                       'BD_SYMBOL':'symbol',
                                       'BD_SCRIP_NAME':'security_name',
                                       'BD_CLIENT_NAME':'client_name',
                                       'BD_BUY_SELL':'buy/sell',
                                       'BD_QTY_TRD':'quantity_traded',
                                       'BD_TP_WATP':'traded_price',
                                       'BD_REMARKS':'remarks'
                                       }, inplace= True)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        finally:
            session.close()
        res['date'] = pd.to_datetime(res['date'],
                                     format   = self.date_format)
        return res[['date',
                    'symbol',
                    'security_name',
                    'client_name',
                    'buy/sell',
                    'quantity_traded',
                    'traded_price',
                    'remarks']]
    
    def announcements(self,
                      from_date:str = (datetime.today().date() 
                                      - timedelta(days=100)).strftime("%d-%m-%Y"),
                      to_date:str   =  datetime.today().date().strftime("%d-%m-%Y")
                     ) -> pd.DataFrame:
        """This function scraps the announcements from the NSE website.
        
        Args:
            from_date (str, optional): Starting date in "DD-MM-YYY" format. Defaults to today's date.
            to_date (str, optional): Ending date in "DD-MM-YYY" format. Defaults to exact one year.
            
        Returns:
            pd.DataFrame
        """
        try:
            from_date = parser.parse(from_date,
                                     dayfirst= True)
            to_date   = parser.parse(to_date,
                                     dayfirst= True)
        except Exception as e:
            raise ValueError("Error: Invalid date format. Please use 'DD-MM-YYYY'.",e)
        if not (from_date <= to_date):
            raise ValueError("Error: Invalid date range. Starting date (from_date) should be earlier than ending date (to_date).")
        from_date    = from_date.strftime('%d-%m-%Y')
        to_date      = to_date.strftime('%d-%m-%Y')
        stock_symbol = self.symbol_finder()
        session      = requests.Session()
        session.mount('https://',
                      self.adapter)
        try:
            session.get("https://www.nseindia.com",
                        headers = self.head)
            session.get("https://www.nseindia.com/all-reports", 
                        headers = self.head)
            session.get("https://www.nseindia.com/report-detail/display-bulk-and-block-deals",
                        headers = self.head)
            url_ = f"https://www.nseindia.com/api/corporate-announcements?index=equities&from_date={from_date}&to_date={to_date}&symbol={stock_symbol}"
            res_ = session.get(url     = url_,
                              headers = self.head).json()
            res_ = pd.DataFrame(res_)
            if len(res_) <= 0:
                raise ValueError(f"Data not found in between {from_date} to {to_date}")
            res_.rename(columns={'sort_date':'timestamp',
                        'desc':'subject',
                        'sm_name':'company_name',
                        'sm_isin':'isin',
                        'smIndustry':'industry',
                        'attchmntText':'details',
                        'attchmntFile':'attachment',
                        }, inplace= True)
            return res_[['symbol',
                        'timestamp',
                        'subject',
                        'company_name',
                        'isin',
                        'industry',
                        'details',
                        'attachment']]
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        finally:
            session.close()




# Intra Day Index Data Scrapper
def intraday_index(index_name:str,
                   tick = False,
                   candlestick = 1)->pd.DataFrame:
    """This function scrapes current dates index spot data for the given nse index_name

    Args:
        index_name (str): NSE Index name (For Example:- NIFTY 50, NIFTY BANK, NIFTY NEXT 50, NIFTY FINANCIAL SERVICES, NIFTY MIDCAP SELECT.)
        tick (bool, optional): If True returns per second tick price data . Defaults to False.
        candlestick (int, optional): Candle period in Minutes . Defaults to 1.

    Returns:
        pd.DataFrame: Intra Day index data
    """
    # Loading the Nifty Indices list
    with open( HERE /'nifty_indices.pickle', 'rb') as file:
        nifty_indices = pickle.load(file)
    if index_name.upper() in nifty_indices:
        try:
            session = requests.Session()
            max_retries = 10
            backoff_factor = 0.5
            retry = Retry(total=max_retries, backoff_factor=backoff_factor, status_forcelist=[500, 502, 503, 504])
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('https://', adapter)
            head = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/87.0.4280.88 Safari/537.36 "
            }
            session.get("https://www.nseindia.com",
                        headers=head)
            index_dataframe = pd.DataFrame(session.get(f"https://www.nseindia.com/api/chart-databyindex?index={str.upper(index_name)}&indices=true",
                                                       headers= head).json()['grapthData'])
            session.close()
            index_dataframe.rename({0:"timestamp",1:"ltp"},
                                   axis= 1 ,
                                   inplace= True)
            index_dataframe['timestamp'] = pd.to_datetime(index_dataframe['timestamp'],unit='ms', origin='unix')
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        if tick:
            return index_dataframe[["timestamp","ltp"]]
        else:
            index_dataframe = index_dataframe.set_index(index_dataframe['timestamp'])
            index_dataframe = index_dataframe[['ltp']]
            index_dataframe = index_dataframe['ltp'].resample(f'{candlestick}Min').ohlc()
            return index_dataframe.reset_index()
    else:
        print(f"""Ignoring further execution for '{index_name}'.
              Not a valid index name !!!!!.
              Please try amonng these: {sorted(nifty_indices)}""")

# Intraday stock data scrapper
def intraday_stock(stock_name:str,
                   tick = False,
                   candlestick:int = 1)->pd.DataFrame:
    """This function scrapes current date's listed companies spot data for the given stock name

    Args:
        index_name (str): NSE listed stock name (For Example:- TCS, LICI, SBIN, RELIANCE etc.)
        tick (bool, optional): If True returns per second tick price data . Defaults to False.
        candlestick (int, optional): Candle period in Minutes . Defaults to 1.

    Returns:
        pd.DataFrame: Intra Day stock data
    """
    def identifier_finder(name: str) -> str:
        name = name.replace(' ', '')
        session = requests.Session()
        max_retries = 3
        backoff_factor = 0.5
        retry = Retry(total=max_retries, backoff_factor=backoff_factor, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('https://', adapter)
        head = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/87.0.4280.88 Safari/537.36 "
        }
        search_url = 'https://www.nseindia.com/api/search/autocomplete?q={}'
        get_details = 'https://www.nseindia.com/api/quote-equity?symbol={}'
        try:
            session.get('https://www.nseindia.com/', headers=head)
            search_results = session.get(url=search_url.format(name), headers=head)
            search_result = search_results.json()['symbols'][0]['symbol']
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        except (IndexError, KeyError) as e:
            raise ValueError("Error: Symbol not found or invalid response from server. Please try again.") from None
        finally:
            session.close()
        company_details = session.get(url=get_details.format(search_result), headers=head)
        try:
            identifier = company_details.json()['info']['identifier']
        except KeyError as e:
            raise ValueError("Error: Unable to retrieve company identifier from server response.\nPlease try again with valid stock name") from None
        return identifier
    stock_name = identifier_finder(stock_name)
    session = requests.Session()
    max_retries = 10
    backoff_factor = 0.5
    retry = Retry(total=max_retries, backoff_factor=backoff_factor, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    head = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/87.0.4280.88 Safari/537.36 "
    }
    try:
        session.get("https://www.nseindia.com", headers=head)
        company_spot_data = pd.DataFrame(session.get(f"https://www.nseindia.com/api/chart-databyindex?index={str.upper(stock_name)}", headers= head).json()['grapthData'])
        session.close()
    except requests.exceptions.RequestException as e:
            raise SystemExit(e)
    company_spot_data.rename({0:"timestamp",1:"ltp"}, axis= 1 , inplace= True)
    company_spot_data['timestamp'] = pd.to_datetime(company_spot_data['timestamp'],unit='ms', origin='unix')
    if tick:
        return company_spot_data
    else:
        company_spot_data = company_spot_data.set_index(company_spot_data['timestamp'])
        company_spot_data = company_spot_data[['ltp']]
        company_spot_data = company_spot_data['ltp'].resample(f'{candlestick}Min').ohlc()
        return company_spot_data.reset_index()
    
    
def historical_stock(stock_name:str,
                     from_date:str = (datetime.today().date() 
                                      - timedelta(days=365)).strftime("%d-%m-%Y"),
                     to_date:str   =  datetime.today().date().strftime("%d-%m-%Y")
                     ) -> pd.DataFrame:
    """This function scraps historical stock data from NSE. Maximum historical data will be one year.

    Args:
        stock_name (str): Company/Stock name
        from_date (str, optional): Starting date in "DD-MM-YYY" format. Defaults to today's date.
        to_date (str, optional): Ending date in "DD-MM-YYY" format. Defaults to exact one year.

    Returns:
        pd.DataFrame:  Daily candlestick data for the input "stock_name".
    """
    def symbol_finder(company_name:str) -> str:
        company_name   = company_name.replace(' ', '')
        session        = requests.Session()
        max_retries    = 5
        backoff_factor = 0.5
        retry          = Retry(total             = max_retries,
                               backoff_factor    = backoff_factor,
                               status_forcelist  = [500, 502, 503, 504])
        adapter        = HTTPAdapter(max_retries = retry)
        session.mount('https://', adapter)
        head = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/87.0.4280.88 Safari/537.36 "
        }
        search_url = 'https://www.nseindia.com/api/search/autocomplete?q={}'
        try:
            session.get('https://www.nseindia.com/',
                        headers = head)
            search_results = session.get(url     = search_url.format(company_name),
                                         headers = head)
            search_result  = search_results.json()['symbols'][0]['symbol']
            return str(search_result)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        except (IndexError, KeyError) as e:
            raise ValueError("Error: Symbol not found or invalid response from server. Please try again.") from None
        finally:
            session.close()
    company        = symbol_finder(stock_name)
    session        = requests.Session()
    max_retries    = 10
    backoff_factor = 0.5
    retry          = Retry(total             = max_retries,
                           backoff_factor    = backoff_factor,
                           status_forcelist  = [500, 502, 503, 504])
    adapter        = HTTPAdapter(max_retries = retry)
    session.mount('https://', adapter)
    head = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/87.0.4280.88 Safari/537.36 "
    }
    try:
        session.get("https://www.nseindia.com",
                    headers = head)
        session.get("https://www.nseindia.com/get-quotes/equity?symbol=" 
                    + company, 
                    headers = head)  # to save cookies
        session.get("https://www.nseindia.com/api/historical/cm/equity?symbol="
                    +company,
                    headers = head)
        url     = ("https://www.nseindia.com/api/historical/cm/equity?symbol=" 
                   + company 
                   + "&series=[%22EQ%22]&from=" 
                   + from_date 
                   + "&to=" 
                   + to_date 
                   + "&csv=true")
        webdata = session.get(url = url,
                              headers = head)
        company_historical_dataframe         = pd.read_csv(StringIO(webdata.text[3:]))
        company_historical_dataframe.columns = [str(x).lower().replace(' ','') for x in company_historical_dataframe.columns]
        company_historical_dataframe['date'] = pd.to_datetime(company_historical_dataframe['date'],
                                                              format="%d-%b-%Y")
        company_historical_dataframe[['volume', 'value','nooftrades']] = company_historical_dataframe[['volume',
                                                                                                       'value',
                                                                                                       'nooftrades']
                                                                                                      ].apply(
                                                                                                                lambda x: pd.to_numeric(x.str.replace(',', '')
                                                                                                                                        )
                                                                                                            )
        return company_historical_dataframe
    except requests.exceptions.RequestException as e:
            raise SystemExit(e)

def historical_index(index_name:str,
                     from_date = str((datetime.today().date() - timedelta(days=365)).strftime("%d-%m-%Y")),
                     to_date = str((datetime.today().strftime("%d-%m-%Y"))))->pd.DataFrame:
    """This function scraps historical index data from NSE. Maximum historical data will be one year.

    Args:
        index_name (str): NSE Index name (For Example:- NIFTY 50, NIFTY BANK, NIFTY NEXT 50, NIFTY FINANCIAL SERVICES, NIFTY MIDCAP SELECT.)
        from_date (str, optional): Starting date in "DD-MM-YYY" format. Defaults to today's date.
        to_date (str, optional): Ending date in "DD-MM-YYY" format. Defaults to exact one year.

    Returns:
        pd.DataFrame:  Daily candlestick data for the input "index_name".
    """
    # Loading the Nifty Indices list
    with open( HERE /'nifty_indices.pickle', 'rb') as file:
        nifty_indices = pickle.load(file)
    if index_name.upper() in nifty_indices:
        try:
            session = requests.Session()
            max_retries = 5
            backoff_factor = 0.5
            retry = Retry(total=max_retries, backoff_factor=backoff_factor, status_forcelist=[500, 502, 503, 504])
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('https://', adapter)
            head = {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/87.0.4280.88 Safari/537.36 "
            }
            index_name = index_name.upper()
            index_name = index_name.replace(' ', '%20')
            index_name = index_name.replace('-', '%20')
            session.get("https://www.nseindia.com", headers=head)
            index_data_json = session.get(
                url="https://www.nseindia.com/api/historical/indicesHistory?indexType=" + index_name +
                    "&from=" + from_date + "&to=" + to_date,
                headers=head)
            output_dataframe = pd.DataFrame(index_data_json.json()['data']['indexCloseOnlineRecords'])
            output_dataframe.rename({'EOD_INDEX_NAME':'index_name',
                                     'EOD_OPEN_INDEX_VAL':'open',
                                     'EOD_HIGH_INDEX_VAL':'high',
                                     'EOD_LOW_INDEX_VAL':'low',
                                     'EOD_CLOSE_INDEX_VAL':'close',
                                     'EOD_TIMESTAMP':'date'},
                                    axis=1,
                                    inplace= True)
            output_dataframe['date'] = pd.to_datetime(output_dataframe['date'],
                                                      format="%d-%b-%Y")
            return output_dataframe.drop(['_id','TIMESTAMP'], 
                                         axis= 1)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
    else:
        print(f"""Ignoring further execution for '{index_name}'. Not a valid index name !!!!!.
              Please try amonng these: {sorted(nifty_indices)}""")