# Importing Necessary Libraries
import pandas as pd
import requests
from datetime import datetime 
from pytz import timezone
import pickle
import pathlib
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
            # Creating a session object
            session = requests.session()
            # Initializing the header
            head = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/87.0.4280.88 Safari/537.36 "
            }
            # Connecting to nse india website
            session.get("https://www.nseindia.com",
                        headers=head)
            # Downloading the current tick data
            index_dataframe = pd.DataFrame(session.get(f"https://www.nseindia.com/api/chart-databyindex?index={str.upper(index_name)}&indices=true",
                                                       headers= head).json()['grapthData'])
            # Closing the connection
            session.close()
            # Renaming the index names
            index_dataframe.rename({0:"DATETIME",1:"Tick"},
                                   axis= 1 ,
                                   inplace= True)
            # Creating the datetime column
            index_dataframe['DATETIME'] = index_dataframe['DATETIME'].apply(lambda x : datetime.fromtimestamp(x/1000 - 6*3600+30*60))
        # For error handeling
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        # Return conditions
        if tick:
            return index_dataframe
        else:
            index_dataframe = index_dataframe.set_index(index_dataframe['DATETIME'])
            index_dataframe = index_dataframe[['Tick']]
            index_dataframe = index_dataframe['Tick'].resample(f'{candlestick}Min').ohlc()
            return index_dataframe.reset_index()
    # Printing the errors  
    else:
        print(f"""Ignoring further execution for '{index_name}'. Not a valid index name !!!!!.\nPlease try amonng these: {sorted(nifty_indices)}""")
        
# Company symbol finder
class ValueError(Exception):
    pass

def identifier_finder(name: str) -> str:
    name = name.replace(' ', '')
    session = requests.session()
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
        raise ValueError(f"Error: Unable to retrieve company identifier from server response. Please try again.")

    return identifier

# Intraday stock data scrapper
def intraday_stock(stock_name:str,
                   tick = False,
                   candlestick = 1)->pd.DataFrame:
    """This function scrapes current date's listed comapanies spot data for the given stock name

    Args:
        index_name (str): NSE listed stock name (For Example:- TCS, LICI, SBIN, RELIANCE etc.)
        tick (bool, optional): If True returns per second tick price data . Defaults to False.
        candlestick (int, optional): Candle period in Minutes . Defaults to 1.

    Returns:
        pd.DataFrame: Intra Day stock data
    """
    stock_name = identifier_finder(stock_name)
    session = requests.session()
    head = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/87.0.4280.88 Safari/537.36 "
    }
    try:
        session.get("https://www.nseindia.com", headers=head)
        comapny_spot_data = pd.DataFrame(session.get(f"https://www.nseindia.com/api/chart-databyindex?index={str.upper(stock_name)}", headers= head).json()['grapthData'])
        session.close()
    except requests.exceptions.RequestException as e:
            raise SystemExit(e)
    comapny_spot_data.rename({0:"DATETIME",1:"Tick"}, axis= 1 , inplace= True)
    comapny_spot_data['DATETIME'] = comapny_spot_data['DATETIME'].apply(lambda x : datetime.fromtimestamp(x/1000 - 6*3600+30*60))
    if tick:
        return comapny_spot_data
    else:
        comapny_spot_data = comapny_spot_data.set_index(comapny_spot_data['DATETIME'])
        comapny_spot_data = comapny_spot_data[['Tick']]
        comapny_spot_data = comapny_spot_data['Tick'].resample(f'{candlestick}Min').ohlc()
        return comapny_spot_data.reset_index()