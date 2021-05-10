from binance.client import Client
from binance.websockets import BinanceSocketManager
import pandas as pd
from datetime import datetime,timedelta
import time
import os
from multiprocessing import Pool


class BinanceData():
    def __init__(self,api_key, api_secret):
        '''
        This class is used to find all pairs in Binance exchange and get
        short period historical data from it. This class is based on python-binance library

        parameters
        =======================================
        api_key: string
            Binance account api key 
        
        api_secret: string
            Binance account api secret

        Example:
        =======================================
        obj = BinanceData(api_key, api_secret)
        obj.get_trading_pair(quote_asset = 'BTC')
        obj.multiprocess_task_download(pool_num = 8)
        obj.aggregate_and_export_data(folder_path = 'Data')

        '''
        client = Client(api_key,api_secret)
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = client
        self.symbol_list = None
        self.output_data = []
        self.time_range = '4 day ago UTC'
        self.interval = '1d'
        self.aggregate_data = None
       

    def get_trading_pair(self, quote_asset = 'BTC'):

        '''
        This function is used to get all trading pairs for quote asset
        parameters
        =====================================
        quote_asset: string  default = 'BTC'
            filter trading pair to get only pairs trading use quote asset. Default is bitcoin.

        return
        =====================================
        symbol_list: list
            a list of trading pairs in Binance exchange traded using quote asset

        '''
        quote_asset_lenth = len(quote_asset)
        response = self.client.get_exchange_info()
        self.symbol_list = [item['symbol'] for item in response['symbols']]
        self.symbol_list = [symbol for symbol in self.symbol_list if symbol[-quote_asset_lenth:] == quote_asset]
        return self.symbol_list

    def __interpret_historical_prices(self,klines, symbol):
        '''

        parameters:
        =====================================
        klines: list
            a list of historical prices for a given trading pair. Results retrived from get_historical_klines method in python-binance library.
        
        symbol: string
            a trading pair in Binance exchange

        return:
        ====================================
        daily_price: dataframe
            a dataframe contains historical price data, including time, open, high, low, close, volume, ticker information

        '''
        df_list = []
        for i in range(0,len(klines)):
            daily_data = {}
            daily_data['time'] = datetime.fromtimestamp(klines[i][0]/1000)
            daily_data['open'] = klines[i][1]
            daily_data['high'] = klines[i][2]
            daily_data['low'] = klines[i][3]
            daily_data['close'] = klines[i][4]
            daily_data['volume'] = klines[i][5]
            df_list.append(pd.DataFrame(daily_data,index = [0]))
        daily_price = pd.concat(df_list)
        daily_price['ticker'] = symbol
        daily_price= daily_price.reset_index(drop = True)
       
        return daily_price

    def __select_relevant_prices(self,daily_price):

        '''
        select only price data at time (23:30 - 00:00) interval and move a date one day backward

        Example: select prices at 2021-01-12 00:00:00 and treat it as prices at 2021-01-11 23:59:59


        parameters:
        =====================================
        daily_price: dataframe
            a dataframe contains prices data every 30 minutes

        return:
        =====================================
        daily_close_price: dataframe
            a dataframe only contains prices at 23:30 - 00:00 daily and adjust date

        '''
        ## Aggregate daily volume
        daily_price['volume'] = daily_price['volume'].astype('float')
        daily_price['date']= daily_price['time'].dt.date
        daily_volume = daily_price.groupby('date')[['volume']].sum()
        daily_price.drop('volume',axis = 1, inplace = True)
        daily_price = pd.merge(daily_price,daily_volume, on = 'date') 
        daily_price.drop('date',axis = 1, inplace = True)

        daily_close_price = daily_price[(daily_price['time'].dt.hour == 23) & (daily_price['time'].dt.minute == 30)]
        #adjust_date= daily_close_price['time'].dt.date +timedelta(days = -1)
        adjust_date= daily_close_price['time'].dt.date
        daily_close_price = daily_close_price.assign(time = adjust_date.values) 
        
        return daily_close_price

    def get_historical_prices(self,symbol,client, interval,time_range):
        '''
        This function is used to get prices from Binance exchange and select only price in 23:30 - 00:00 daily.

        parameter:
        ===================================
        symbol: string
            a traging pair in Binance exchange to get prices

        client: object
            a python-binance library Client object instance

        time_range: string  default = '4 day ago UTC'
            a time period to get historical data. To change it, change attributes before calling this method.

        return:
        ===================================
        single_prices: dataframe
            a dataframe contains price information for a given trading pair
        '''

        print("getting {0} historical data...".format(symbol))
        klines = client.get_historical_klines(symbol, self.interval,self.time_range)
        ##check if retrived data is empty or not
        if len(klines)>0:
            single_prices = self.__interpret_historical_prices(klines, symbol)
            #single_prices = self.__select_relevant_prices(single_prices)
        else:
            return None
        return single_prices

    def __store_data(self,single_prices):

        '''
        This is used to store returned data in multiprocessing queries

        parameter
        ==================================
        single_prices: dataframe
            a dataframe get from each single process

        '''
        self.output_data.append(single_prices)

    def aggregate_and_export_data(self,folder_path):
        
        '''
        This method is used to aggregate all the data get from multiprocessing query and export it in as an excel file

        parameter:
        ==================================
        folder_path: string
            a folder to store the excel file. If don't already exist, will automatically create one.

        return:
        ==================================
        aggregate_data: dataframe
            a dataframe contains all trading pairs' historical prices in 23:30 - 00:00 interval. 

        '''
        self.aggregate_data = pd.concat(self.output_data)

        ## change format
        columns = ['open','high','low','close','volume']
        for column in columns:
            self.aggregate_data[column] = self.aggregate_data[column].astype('float')

        ##export data
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)

        name = datetime.now().strftime('%Y%m%d%H%M')
        self.aggregate_data.to_excel(folder_path + '/'+name +'.xlsx',index = False)
        return self.aggregate_data
    
    def multiprocess_task_download(self, pool_num):
        '''
        This is used to send requests to Binance api use multiprocessing techniques

        parameter:
        ==================================
        pool_num: int
            number of logic processors to use to send queries.

        '''

        pool = Pool(pool_num)
        for symbol in self.symbol_list:
            pool.apply_async(self.get_historical_prices, args = (symbol, self.client, self.interval,self.time_range), callback = self.__store_data)
        pool.close()
        start = time.time()
        pool.join()
        end = time.time()
        print('It takes {} seconds to get the data'.format(end-start))
        


if __name__ == "__main__":
    
    import os
    from dotenv import load_dotenv
    
    load_dotenv()

    api_key = os.getenv('api_key')
    api_secret = os.getenv('api_secret')

    obj = BinanceData(api_key, api_secret)
    #obj.get_trading_pair(quote_asset = 'BTC')
    obj.symbol_list = ['BTCUSDT','ETHUSDT']
    obj.time_range = '1100 day ago UTC'
    obj.multiprocess_task_download(pool_num = 8)
    data = obj.aggregate_and_export_data(folder_path = 'data')
    print(data.dtypes)
    
    