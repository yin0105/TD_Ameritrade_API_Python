import requests
import csv
from mysql.connector import connect, Error
from pprint import pprint
from datetime import datetime, timedelta
import time
from os.path import join, dirname
from dotenv import load_dotenv
import os


# Read env
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
database_name = os.environ.get('DATABASE_NAME')
user_name = os.environ.get('USER_NAME')
db_password = os.environ.get('DB_PASSWORD')
api_key = os.environ.get('API_KEY')
work_time = int(os.environ.get('WORK_TIME'))
rest_time = int(os.environ.get('REST_TIME'))


url_instruments = "https://api.tdameritrade.com/v1/instruments"
url_quotes = "https://api.tdameritrade.com/v1/marketdata/quotes"
url_price_history = "https://api.tdameritrade.com/v1/marketdata/"
conn = None
tickers = []
start_time = datetime.now().timestamp()

# Read tickers from csv file
def read_tickers():
    global tickers
    with open('Tickers.csv', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in rows:
            tickers.append(row[0]) 

        if tickers[0].lower() == "tickers" :
            tickers.pop(0)


# Connect mysql database
def mysql_con():
    global conn
    """ Connect to MySQL database """
    try:
        conn = connect(host='localhost',
                        database=database_name,
                        user=user_name,
                        password=db_password)
        if conn.is_connected():
            print('Connected to MySQL database')
        else:
            print("Warnig: Can't connect to MySQL database")

    except Error as e:
        print("Warnig: Can't connect to MySQL database")
        print(e)


# Get instruments
def get_instruments(ticker):
    response = requests.get(url_instruments + "?apikey=" + api_key + "&symbol=" + ticker + "&projection=fundamental")
    if response.status_code == 200 :
        json_data = response.json()
        json_data = json_data[ticker]
        j_data = [ticker]
        j_data.append(json_data["assetType"])
        j_data.append(json_data["cusip"])
        j_data.append(json_data["description"])
        j_data.append(json_data["exchange"])
        j_fundamental = json_data["fundamental"]
        key_arr = ["beta", "bookValuePerShare", "currentRatio", "divGrowthRate3Year", "dividendAmount", "dividendDate", "dividendPayAmount", "dividendPayDate", "dividendYield", "epsChange", "epsChangePercentTTM", "epsChangeYear", "epsTTM", "grossMarginMRQ", "grossMarginTTM", "high52", "interestCoverage", "low52", "ltDebtToEquity", "marketCap", "marketCapFloat", "netProfitMarginMRQ", "netProfitMarginTTM", "operatingMarginMRQ", "operatingMarginTTM", "pbRatio", "pcfRatio", "peRatio", "pegRatio", "prRatio", "quickRatio", "returnOnAssets", "returnOnEquity", "returnOnInvestment", "revChangeIn", "revChangeTTM", "revChangeYear", "sharesOutstanding", "shortIntDayToCover", "shortIntToFloat", "totalDebtToCapital", "totalDebtToEquity", "vol10DayAvg", "vol1DayAvg", "vol3MonthAvg"]
        for k in key_arr:
            j_data.append(j_fundamental[k])
        j_data.append(datetime.now())
        args = tuple(j_data)
        query = "INSERT INTO instruments (symbol, assetType, cusip, description, exchange, beta, bookValuePerShare, currentRatio, divGrowthRate3Year, dividendAmount, dividendDate, dividendPayAmount, dividendPayDate, dividendYield, epsChange, epsChangePercentTTM, epsChangeYear, epsTTM, grossMarginMRQ, grossMarginTTM, high52, interestCoverage, low52, ltDebtToEquity, marketCap, marketCapFloat, netProfitMarginMRQ, netProfitMarginTTM, operatingMarginMRQ, operatingMarginTTM, pbRatio, pcfRatio, peRatio, pegRatio, prRatio, quickRatio, returnOnAssets, returnOnEquity, returnOnInvestment, revChangeIn, revChangeTTM, revChangeYear, sharesOutstanding, shortIntDayToCover, shortIntToFloat, totalDebtToCapital, totalDebtToEquity, vol10DayAvg, vol1DayAvg, vol3MonthAvg, fetchedDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor = conn.cursor()
        cursor.execute(query, args)
        conn.commit()
    else :
        print("Warnig: Can't get instruments.  status code = " + str(response.status_code))


# Get quotes
def get_quotes(ticker):
    response = requests.get(url_quotes + "?apikey=" + api_key + "&symbol=" + ticker)
    if response.status_code == 200 :
        json_data = response.json()
        try:
            json_data = json_data[ticker]
        except:
            return
        j_data = [ticker]
        key_arr = ["52WkHigh", "52WkLow", "askId", "askPrice", "askSize", "assetMainType", "assetType", "bidId", "bidPrice", "bidSize", "bidTick", "closePrice", "cusip", "delayed", "description", "digits", "divAmount", "divDate", "divYield", "exchange", "exchangeName", "highPrice", "lastId", "lastPrice", "lastSize", "lowPrice", "marginable", "mark", "markChangeInDouble", "markPercentChangeInDouble", "nAV", "netChange", "netPercentChangeInDouble", "openPrice", "peRatio", "quoteTimeInLong", "regularMarketLastPrice", "regularMarketLastSize", "regularMarketNetChange", "regularMarketPercentChangeInDouble", "regularMarketTradeTimeInLong", "securityStatus", "shortable", "totalVolume", "tradeTimeInLong", "volatility"]
        for k in key_arr:
            j_data.append(json_data[k])
        j_data.append(datetime.now())
        args = tuple(j_data)
        query = "INSERT INTO quotes (symbol, 52WkHigh, 52WkLow, askId, askPrice, askSize, assetMainType, assetType, bidId, bidPrice, bidSize, bidTick, closePrice, cusip, delayed_, description, digits, divAmount, divDate, divYield, exchange, exchangeName, highPrice, lastId, lastPrice, lastSize, lowPrice, marginable, mark, markChangeInDouble, markPercentChangeInDouble, nAV, netChange, netPercentChangeInDouble, openPrice, peRatio, quoteTimeInLong, regularMarketLastPrice, regularMarketLastSize, regularMarketNetChange, regularMarketPercentChangeInDouble, regularMarketTradeTimeInLong, securityStatus, shortable, totalVolume, tradeTimeInLong, volatility, fetchedDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor = conn.cursor()
        cursor.execute(query, args)
        conn.commit()
    else :
        print("Warnig: Can't get quotes.  status code = " + str(response.status_code))


# Get price history
def get_price_history(ticker):
    # Rest
    if datetime.now().timestamp() - start_time > work_time :
        print("rest . . .")
        time.sleep(rest_time)
    cursor = conn.cursor()

    # Calulate end_date (now)
    end_date= str(int(datetime.now().timestamp() * 1000))

    # Calulate start_date
    query = "SELECT date_time FROM price_history WHERE symbol='" + ticker + "' ORDER BY date_time DESC LIMIT 1"
    cursor.execute(query)
    row = cursor.fetchone()
    start_date = datetime.now() - timedelta(days=1096)
    if row is not None:
        print(row[0])
        last_date_time = row[0]
        if last_date_time > datetime.now() - timedelta(days=1096) :
            start_date = last_date_time + timedelta(days=1)
    start_date = str(int(start_date.timestamp() * 1000))

    # Call API & Save data
    response = requests.get(url_price_history + ticker + "/pricehistory?apikey=" + api_key + "&periodType=year&period=1&frequencyType=daily&frequency=1&endDate=" + end_date + "&startDate=" + start_date)
    if response.status_code == 200 :
        json_data = response.json()        
        j_data = [ticker]
        j_data.append(json_data["empty"])
        j_data.append(datetime.now())
        key_arr = ["close", "datetime", "high", "low", "open", "volume"]
        
        for one_day_data in json_data["candles"]:            
            j_data_2 = j_data[:]
            for k in key_arr:
                if k == "datetime":
                    j_data_2.append(timedelta(seconds=one_day_data[k]/1000) + datetime.utcfromtimestamp(0))
                else:
                    j_data_2.append(one_day_data[k])
            args = tuple(j_data_2)
            query = "INSERT INTO price_history (symbol, empty, fetchedDate, close, date_time, high, low, open, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            
            cursor.execute(query, args)
            conn.commit()
        
    else :
        print("Warnig: Can't get price history.  status code = " + str(response.status_code))

def main():
    global tickers, conn

    mysql_con()
    if conn == None :
        print("Hi")
        exit()

    # Read tickers from csv file
    read_tickers()

    for ticker in tickers:
        print(ticker)
        get_instruments(ticker)
        get_quotes(ticker)
        get_price_history(ticker)
        

    


if __name__ == '__main__':
    main()

