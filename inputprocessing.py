import bs4 as bs
import datetime as dt
import matplotlib.pyplot as plt
from matplotlib import style
import numpy as np
#to create new directories
import os
import pandas as pd
from pandas_datareader import data as pdr
#pickle serializes any python object (can save any object) we will save s&p500 list
import pickle
import requests
import yfinance as yf
from pytrends.request import TrendReq

style.use("ggplot")

#get the sp500 ticker symbols
def save_sp500_tickers():
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, "lxml")
    table = soup.find('table', {'id':'constituents'})
    tickers = []
    #first row is table header so skip it and start at second
    #table row
    for row in table.findAll('tr')[1:]:
        #table data (each column)
        ticker = row.findAll('td')[0].text
        ticker = ticker.replace("\n", "")
        tickers.append(ticker)

    #save the tickers to file
    with open("sp500tickers.pickle", "wb") as file:
        pickle.dump(tickers, file)

    print ("Tickers saved to sp500tickers.pickle")

    return tickers

def get_data_from_yahoo(reload_sp500=False):

    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as file:
            tickers = pickle.load(file)

    #for each stock, put data in csv file so we don't have to keep fetching from yahoo
    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')
    # get all data since 2005
    start = dt.datetime(2005, 1, 1)
    end = dt.datetime.now()

    for ticker in tickers:
        ticker = ticker.replace(".", "-")
        #get date, high, low, open, close, volume, adjusted close
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
            df = pdr.get_data_yahoo(ticker, start, end)
            df.reset_index(inplace=True)
            df.set_index("Date", inplace=True)
            df.to_csv('stock_dfs/{}.csv'.format(ticker))
        #else:
            #print("Already have {}".format(ticker))

    print ("Historical data for each ticker saved")


def compile_data():
    with open("sp500tickers.pickle", "rb") as file:
        tickers = pickle.load(file)

    main_df_closes = pd.DataFrame()

    #get adjusted closes
    for count,ticker in enumerate(tickers):
        ticker = ticker.replace(".", "-")
        df = pd.read_csv('stock_dfs/{}.csv'.format(ticker))
        df.set_index('Date', inplace=True)

        df.rename(columns = {"Adj Close": ticker}, inplace=True)
        df.drop(['Open', 'High', 'Low', 'Close', 'Volume'], 1, inplace=True)

        if main_df_closes.empty:
            main_df_closes = df

        else:
            main_df_closes = main_df_closes.join(df, how='outer')
    main_df_closes.to_csv("sp500_joined_closes.csv")

    print ("Adjusted close data for each ticker symbol saved")



    main_df_volumes = pd.DataFrame()
    #get volume
    for count,ticker in enumerate(tickers):
        ticker = ticker.replace(".", "-")
        df = pd.read_csv('stock_dfs/{}.csv'.format(ticker))
        df.set_index('Date', inplace=True)

        df.rename(columns = {"Volume": ticker}, inplace=True)
        df.drop(['Open', 'High', 'Low', 'Close', 'Adj Close'], 1, inplace=True)

        if main_df_volumes.empty:
            main_df_volumes = df

        else:
            main_df_volumes = main_df_volumes.join(df, how='outer')
    main_df_volumes.to_csv("sp500_joined_volumes.csv")

    print ("Volume data for each ticker symbol saved")

def calculateSMA():
    #get 100 day simple moving avg
    smawindow = 100

    with open("sp500tickers.pickle", "rb") as file:
        tickers = pickle.load(file)
    main_df_smas = pd.DataFrame()

    for count,ticker in enumerate(tickers):
        ticker = ticker.replace(".", "-")
        df = pd.read_csv('stock_dfs/{}.csv'.format(ticker))
        df.set_index('Date', inplace=True)
        


        #calculate sma
        df[str(smawindow) + 'ma'] = df['Adj Close'].rolling(window=smawindow).mean()
        # the first 100 days will not have values for 100ma
        # so dr.dropna will drop the first smawindow days
        df.dropna(inplace=True)
        df.rename(columns = {str(smawindow) + 'ma': ticker}, inplace=True)
        df.drop(['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'], 1, inplace=True)


        if main_df_smas.empty:
            main_df_smas = df

        else:
            main_df_smas = main_df_smas.join(df, how='outer')

    main_df_smas.to_csv("sp500_joined_smas.csv")

    print ("SMA data for each ticker symbol saved")



def calculateRSI():
    rsiwindow = 14

    with open("sp500tickers.pickle", "rb") as file:
        tickers = pickle.load(file)
    main_df_rsi = pd.DataFrame()

    for count,ticker in enumerate(tickers):
        ticker = ticker.replace(".", "-")
        df = pd.read_csv('stock_dfs/{}.csv'.format(ticker))
        df.set_index('Date', inplace=True)

        #Calculate the rsi
        delta = df['Adj Close'].diff()
        #Make the positive gains (up) and negative gains (down) Series
        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0
        down[down > 0] = 0

        #Calculate the 14SMA for rsi
        roll_up = up.rolling(rsiwindow).mean()
        roll_down = down.abs().rolling(rsiwindow).mean()
 
        # Calculate the RSI based on SMA
        RS = roll_up / roll_down
        RSI = 100.0 - (100.0 / (1.0 + RS))
 
        df['rsi'] = RSI
 
        df.reset_index(inplace=True)
        df.set_index("Date", inplace=True)
        
        df.rename(columns = {'rsi': ticker}, inplace=True)
        df.drop(['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'], 1, inplace=True)

 

        if main_df_rsi.empty:
            main_df_rsi = df

        else:
            main_df_rsi = main_df_rsi.join(df, how='outer')

    main_df_rsi.to_csv("sp500_joined_rsis.csv")
    
    print ("RSI data for each ticker symbol saved")

      #fig, ax = plt.subplots()
      #ax.plot(RSI)
      #ax.hlines(y=30, xmin=0, xmax=365, linewidth=.5, color='g', linestyles=":")
      #ax.hlines(y=70, xmin=0, xmax=365, linewidth=.5, color='r', linestyles=":")
 
      #rsiTest(0, RSI, df)
 
      #plt.legend(['RSI'])
      #currentRSI = df.tail(1)['rsi']
      #convert from series obj to str, rsi starts at index 19 so take substring until delimeter
      #print (df.tail())
      #currentRSI = float(str(currentRSI)[19:].split("\n")[0])
      #plt.show()
 

def get_google_trends():


    if not os.path.exists('google_trends'):
        os.makedirs('google_trends')
        
    with open("sp500tickers.pickle", "rb") as file:
        tickers = pickle.load(file)
 
    pytrends = TrendReq(hl='en', backoff_factor=0.4, retries = 3)
    

    for count,ticker in enumerate(tickers):
        new_ticker = '$' + ticker
        #get data from pass 8 months so that way we can have daily data rather than weekly data

        try:
            pytrends.build_payload([new_ticker], timeframe= '2020-03-28 2020-11-28')
            interest_over_time_df = pytrends.interest_over_time()

            df = interest_over_time_df
            df.drop(['isPartial'], 1, inplace=True)


            if not os.path.exists('google_trends/{}.csv'.format(ticker)):
                df = interest_over_time_df
                df.reset_index(inplace=True)
                df.set_index("date", inplace=True)

                df.to_csv('google_trends/{}.csv'.format(ticker))


            print ("successful ", new_ticker)


        except:
            print(new_ticker, "couldn't be done")
    
def join_google_trends():

    df = pd.read_csv('sp500_joined_closes.csv', index_col=0)
    with open("sp500tickers.pickle", "rb") as file:
        tickers = pickle.load(file)

    main_df_trends = pd.DataFrame()

    for count,ticker in enumerate(tickers):
        ticker = ticker.replace(".", "-")
        
        #some stocks did not have enough data, make sure there is a google trend file for the specific stock
        if os.path.exists('google_trends/{}.csv'.format(ticker)):
            df = pd.read_csv('google_trends/{}.csv'.format(ticker))
            df.set_index('date', inplace=True)
            df.rename(columns = {'$' + ticker: ticker}, inplace=True)

            if main_df_trends.empty:
                main_df_trends = df

            else:
                main_df_trends = main_df_trends.join(df, how='outer')
        
    main_df_trends.to_csv("sp500_joined_trends.csv")
    print ("Trend data joined")


#uncomment to load datasets yourself
#save_sp500_tickers()
#get_data_from_yahoo()
#compile_data() #contains adj close and volume data
#calculateSMA()
#calculateRSI()
#get_google_trends()
#join_google_trends()


