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
# Getting the file path
HERE = pathlib.Path(__file__).parent.resolve()

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
        print(f"""Ignoring further execution for '{index_name}'. Not a valid index name !!!!!.\nPlease try amonng these: {sorted(nifty_indices)}""")
        
# Company symbol finder
class ValueError(Exception):
    pass

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
        print(f"""Ignoring further execution for '{index_name}'. Not a valid index name !!!!!.\nPlease try amonng these: {sorted(nifty_indices)}""")