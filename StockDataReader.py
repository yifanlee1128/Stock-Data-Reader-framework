# Editor:Yifan Li
# Contact: yl6977@nyu.edu
# Last edit on: 12/04/2020

import pandas as pd
import numpy as np
import yahoo_fin.stock_info as si
import matplotlib.pyplot as plt
import os
import warnings
from operator import itemgetter
from apscheduler.schedulers.background import BackgroundScheduler
import copy

class PeriodicReporter(object):
    """
    This class is used to periodically get real-time market data from Yahoo finance database,
    the accessed data will not be saved at local,
    please use this with my Stock Data Reader class
    """
    def __init__(self, function):
        """

        :param function: function of the arranged job, in this case,
        it should be the getCurrentMarketData function
        """
        self._scheduler = None
        self.function = function
        self.existingJob = False
        self.start()

    def start(self):
        """
        method to start the reporter
        :return: None
        """
        self._scheduler = BackgroundScheduler()
        self._scheduler.start()

    def addJob(self, interval, *args):
        """
        add a reporter
        :param interval: the interval between two reports, 20 means 20 seconds, etc...
        :param args: tickerList,like:["AAPL","IBM","JPM"]
        :return: None
        """
        if not self.existingJob:
            self._scheduler.add_job(self.function, trigger='interval', seconds=interval, args=args)
            self.existingJob = True
        else:
            warnings.warn("Existing job will be removed!")
            self._scheduler.remove_all_jobs()
            self._scheduler.add_job(self.function, trigger='interval', seconds=interval, args=args)

    def removeJob(self):
        """
        remove the current reporter
        :return: None
        """
        self._scheduler.remove_all_jobs()
        self.existingJob = False

    def pause(self):
        """
        pause the current reporter
        :return: None
        """
        self._scheduler.pause()

    def resume(self):
        """
        resume the paused reporter
        :return: None
        """
        self._scheduler.resume()

    def getJob(self):
        """
        print the details of the current reporter
        :return: None
        """
        self._scheduler.print_jobs()

    def shutDown(self):
        """
        shut down the reporter
        :return: None
        """
        self._scheduler.shutdown()


class SpecificTimeReporter(object):
    """
    This class is used to get the real-time market data at specific time everyday from yahoo finance database,
    the accessed data will not be saved at local,
    please use this with my Stock Data Reader class
    """
    def __init__(self, function):
        """
        :param function: function of the arranged job, in this case,
        it should be the getCurrentMarketData function
        """
        self._scheduler = None
        self.function = function
        self.count = 1
        self._all_job = {}
        self.start()

    def start(self):
        """
        start the reporter
        :return: None
        """
        self._scheduler = BackgroundScheduler()
        self._scheduler.start()

    def convertInt2Time(self, hour, minute, second):
        """
        You do not need to call this method, you can treat this as a private method
        :param hour: integer ranging from 0 to 23
        :param minute: integer ranging from 0 to 59
        :param second: integer ranging from 0 to 59
        :return: string format of time
        """
        ans = ""
        if hour < 10:
            ans = ans + "0" + str(hour)
        else:
            ans = ans + str(hour)
        if minute < 10:
            ans = ans + "0" + str(minute)
        else:
            ans = ans + str(minute)
        if second < 10:
            ans = ans + "0" + str(second)
        else:
            ans = ans + str(second)
        return ans

    def addJob(self, hour, minute, second, *args):
        """
        add a reporter
        :param hour: integer ranging from 0 to 23
        :param minute: integer ranging from 0 to 59
        :param second: integer ranging from 0 to 59
        :param args: tickerList,like:["AAPL","IBM","JPM"]
        :return: None
        """
        timeString = self.convertInt2Time(hour, minute, second)

        if timeString not in self._all_job:
            self._all_job[timeString] = str(self.count)
            self._scheduler.add_job(self.function, trigger='cron', hour=hour, minute=minute, second=second, args=args,
                                    id=str(self.count))
            self.count = self.count + 1
        else:
            self._scheduler.reschedule_job(self._all_job[timeString], trigger='cron', hour=hour, minute=minute,
                                           second=second)

    def removeJob(self, hour, minute, second):
        """
        remove a reporter
        :param hour: integer ranging from 0 to 23
        :param minute: integer ranging from 0 to 59
        :param second: integer ranging from 0 to 59
        :return: None
        """
        timeString = self.convertInt2Time(hour, minute, second)
        if timeString not in self._all_job:
            warnings.warn("Job not found!")
        else:
            self._scheduler.remove_job(self._all_job[timeString])

    def removeAllJobs(self):
        """
        remove all reporters
        :return: None
        """
        self._scheduler.remove_all_jobs()

    def pause(self):
        """
        pause all reporters
        :return: None
        """
        self._scheduler.pause()

    def resume(self):
        """
        resume the paused reporters
        :return: None
        """
        self._scheduler.resume()

    def getAllJobs(self):
        """
        print the information of all reporters
        :return: None
        """
        self._scheduler.print_jobs()

    def shutDown(self):
        """
        shut down all reporters
        :return: None
        """
        self._scheduler.shutdown()

class StockDataReader(object):
    def __init__(self,localPath):
        """
        This class is used to extract, organise and analyze data from yahoo finance database
        :param localPath: the path of the folder for saving data, you can simply set it as "./data" for convenience
        """
        if not os.path.isdir(localPath):
            warnings.warn("No such directory found: {}! A new directory is automatically created.".format(localPath))
            os.mkdir(localPath)

        self._local_path = localPath

    def getCurrentMarketData(self,tickerList):
        """
        get real-time market data, you can use this with my two reporters
        :param tickerList: list of tickers, like: ["AAPL","IBM","JPM"]
        :return: dataframe of accessed data
        """
        ans = pd.DataFrame(
            columns=["Best Ask Price", "Best Ask Amount", "Best Bid Price", "Best Bid Amount", "Quote Price", "Open",
                     "Previous Close", "Volume"]
        )
        temp_dict = {}
        keys = ["Ask", "Bid", "Open", "Previous Close", "Quote Price", "Volume"]

        for ticker in tickerList:
            try:
                temp_dict[ticker] = si.get_quote_table(ticker)
            except:
                warnings.warn("{}'s market data not found!".format(ticker))

        for ticker, data in temp_dict.items():
            values = list(itemgetter(*keys)(data))
            organized_values = list(map(float, values[0].split("x"))) + \
                               list(map(float, values[1].split("x"))) + \
                               [round(values[4], 4)] + \
                               values[2:4] + \
                               values[5:]
            ans.loc[ticker] = organized_values
        with pd.option_context('display.max_rows', None, 'display.max_columns',None):
            print(ans)
        return ans

    def getPeriodicReporter(self):
        """
        :return: return the reporter to the user fur future use
        """
        reporter=PeriodicReporter(self.getCurrentMarketData)
        return reporter

    def getSpecificTimeReporter(self):
        """
        :return: return the reporter to the user for future use
        """
        reporter=SpecificTimeReporter(self.getCurrentMarketData)
        return reporter

    def getHistoricalData(self,tickerList, startDate=None, endDate=None, readFromLocal=False, update=False):
        """
        this method is used to get historical data,you can access your own data from the folder or yahoo finance,
        and you can update the new accessed data to local folder
        :param tickerList: list of tickers,like ["AAPL","IBM"]
        :param startDate: like "2015-12-03"
        :param endDate: like "2019-12-03"
        :param readFromLocal: if true, it will read data from local folder, otherwise it read data from yahoo finance
        :param update: if true, it will update the file at local by our new accessed data
        :return: dict of dict, the keys of outer dict are tickers, the values of outer dict are inner dicts;
                 the keys of inner dict is "data", "start date", and "end date"
        """
        ans = {}
        if readFromLocal:
            for ticker in tickerList:
                subdict = {}
                filePath = self._local_path + ticker + ".csv"
                if os.path.isfile(filePath):
                    tempdata = pd.read_csv(filePath, index_col=0)
                    subdict["data"] = tempdata
                    subdict["start date"] = tempdata.index[0]
                    subdict["end date"] = tempdata.index[-1]
                    ans[ticker] = subdict
                else:
                    warnings.warn("{}'s data not found!".format(ticker))
            return ans
        else:
            for ticker in tickerList:
                subdict = {}
                tempdata=pd.DataFrame()
                try:
                    tempdata = si.get_data(ticker, start_date=startDate, end_date=endDate)
                    subdict["data"] = tempdata
                    subdict["start date"] = tempdata.index[0]
                    subdict["end date"] = tempdata.index[-1]
                    ans[ticker] = subdict
                except:
                    warnings.warn(
                        "Errors happened when processing {}'s data, please note that it will not be included in the data!".format
                            (ticker)
                    )
                filePath = self._local_path + ticker + ".csv"
                if update:
                    tempdata.to_csv(filePath)
            return ans

    def transform_Date_Value(self,ticker, startDate=None, endDate=None, readFromLocal=False, update=False):
        """
        this method is used to organise the data, it returns a DataFrame in which the index is date,the column
        is name of values like: "close", "open", "volume" etc., and the contents are the values of a specific stock
        :param ticker: like "IBM"
        :param startDate: like "2015-12-03"
        :param endDate: like "2019-12-03"
        :param readFromLocal: if true, it will read data from local folder, otherwise it read data from yahoo finance
        :param update: if true, it will update the file at local by our new accessed data
        :return: DataFrame
        """
        if not isinstance(ticker, str):
            raise ValueError("Ticker is not a valid string!")
        if readFromLocal:
            filePath = self._local_path + ticker + ".csv"
            if os.path.isfile(filePath):
                data = pd.read_csv(filePath, index_col=0)
                return data
            else:
                warnings.warn("{}'s data not found!".format(ticker))
        else:
            data = pd.DataFrame()
            try:
                data = si.get_data(ticker, start_date=startDate, end_date=endDate)

            except:
                warnings.warn(
                    "Errors happened when processing {}'s data, please note that it will not be included in the data!".format(
                        ticker)
                )
            filePath = self._local_path + ticker + ".csv"
            if update:
                data.to_csv(filePath)
            return data

    def transform_Date_Ticker(self,raw_data, value="close"):
        """
        this method is used to organise the data, it returns a DataFrame in which the index is date,the column
        is name of stocks, and the contents are the values of a specific values (like "close" data) for stocks
        :param raw_data: dict of dict, it can be the returned value from getHistoricalData
        :param value: like "close","open", etc.
        :return: DataFrame
        """
        if not isinstance(raw_data, dict):
            raise ValueError("raw data is not a valid dictionary!")
        if not value in ["open", "high", "low", "close", "adjclose", "volume"]:
            raise ValueError("Only following values are supported: open, high,low, close, adjclose, volume")
        ans = pd.DataFrame()
        for key, subdict in raw_data.items():
            tempdata = copy.deepcopy(subdict["data"])
            tempValueData = tempdata[[value]]
            ans = pd.concat([ans, tempValueData], join='outer', axis=1)
        ans.columns = raw_data.keys()
        return ans

    def transform_Ticker_Value(self,raw_data, date):
        """
        this method is used to organise the data, it returns a DataFrame in which the index is ticker,the column
        is name of values like: "close", "open", "volume" etc., and the contents are the values of a specific stock
        at a specific date
        :param raw_data: dict of dict, it can be the returned value from getHistoricalData
        :param date: like "2019-12-03"
        :return: DataFrame
        """
        if not isinstance(raw_data, dict):
            raise ValueError("raw data is not a valid dictionary!")
        try:
            date = np.datetime64(date)
        except:
            raise ValueError("date is not a valid string of time!")
        ans = pd.DataFrame(columns=["open", "high", "low", "close", "adjclose", "volume"])
        for key, subdict in raw_data.items():
            if not (date <= subdict["end date"] and date >= subdict["start date"]):
                raise ValueError("inout data of {} does not contain the data on the required date!")
            elif not date in subdict["data"].index.values:
                raise ValueError("date is not a valid trading date!")
            else:
                ans.loc[key] = copy.deepcopy(subdict["data"].loc[date].values[:-1])
        return ans

    def transform_resample(self,df_data, freq, value=None):
        """
        this method is used to resample the daily input data into the data with other frequency
        :param df_data: DataFrame
        :param freq: if you use "day" as the unit of frequency, use "5B","10B" instead of "5D" or "10D"
        :param value: specify the input data, like "close", "adjclose"
        :return: DataFrame
        """
        if not isinstance(df_data, pd.DataFrame):
            raise ValueError("input data is not a valid DataFrame!")
        ans = pd.DataFrame()

        fullColumn = ["open", "high", "low", "close", "adjclose", "volume", "ticker"]
        if not value == None:
            if not value in ["open", "high", "low", "close", "adjclose", "volume"]:
                raise ValueError("Value is not valid!")
            if value in ["close", "adjclose"]:
                ans = df_data.resample(freq).last()
            elif value == "open":
                ans = df_data.resample(freq).first()
            elif value == "high":
                ans = df_data.resample(freq).max()
            elif value == "low":
                ans = df_data.resample(freq).min()
            elif value == "volume":
                ans = df_data.resample(freq).sum()
            return ans
        else:
            indexSample = df_data.index.values[0]
            if not isinstance(indexSample, np.datetime64):
                try:
                    new_index = [np.datetime64(date) for date in df_data.index.values]
                    df_data.index = new_index
                except:
                    raise ValueError("invalid index!")
            columns = df_data.columns.values.tolist()
            if not set(columns) <= set(fullColumn):
                raise ValueError("the column of data has invalid value!")
            if "open" in columns:
                ans["open"] = df_data["open"].resample(freq).first()
            if "close" in columns:
                ans["close"] = df_data["close"].resample(freq).last()
            if "adjclose" in columns:
                ans["adjclose"] = df_data["adjclose"].resample(freq).last()
            if "high" in columns:
                ans["high"] = df_data["high"].resample(freq).max()
            if "low" in columns:
                ans["low"] = df_data["low"].resample(freq).min()
            if "volume" in columns:
                ans["volume"] = df_data["volume"].resample(freq).sum()
            return ans

    def getStockStatistics(self,df_data):
        """
        this method is used to summarize the basic statistics of given data
        :param df_data: DataFrame
        :return: DataFrame
        """
        if not isinstance(df_data, pd.DataFrame):
            raise ValueError("input data is not a valid DataFrame!")
        indexSample = df_data.index.values[0]
        if not isinstance(indexSample, np.datetime64):
            try:
                new_index=[np.datetime64(date) for date in df_data.index.values]
                df_data.index=new_index
            except:
                raise ValueError("invalid index!")
        stats = df_data.pct_change().describe()
        stats.loc["skew"] = df_data.pct_change().skew()
        stats.loc["kurt"] = df_data.pct_change().kurt()
        return stats

    def getMovingAverage(self,df_data, window_length):
        """
        this method is used to calculate the moving average index of given data
        :param df_data: DataFrame
        :param window_length: integer standing for the length of the calculation window
        :return: DataFrame
        """
        if not isinstance(df_data, pd.DataFrame):
            raise ValueError("input data is not a valid DataFrame!")
        indexSample = df_data.index.values[0]
        if not isinstance(indexSample, np.datetime64):
            try:
                new_index = [np.datetime64(date) for date in df_data.index.values]
                df_data.index = new_index
            except:
                raise ValueError("invalid index!")
        return df_data.rolling(window_length).mean().dropna()

    def getEMA(self,df_data, span=20):
        """
        this method is used to calculate the EMA index of given data
        :param df_data: DataFrame
        :param span: integer standing for the length of the calculation window
        :return: DataFrame
        """
        if not isinstance(df_data, pd.DataFrame):
            raise ValueError("input data is not a valid DataFrame!")
        indexSample = df_data.index.values[0]
        if not isinstance(indexSample, np.datetime64):
            try:
                new_index = [np.datetime64(date) for date in df_data.index.values]
                df_data.index = new_index
            except:
                raise ValueError("invalid index!")
        return df_data.ewm(span=span, min_periods=span, adjust=False, ignore_na=False).mean()

    def getMACD(self,df_data):
        """
        this method is used to calculate the MACD index of given data
        :param df_data: DataFrame
        :return: DataFrame
        """
        DIF = self.getEMA(df_data, 12) - self.getEMA(df_data, 26)
        return self.getEMA(DIF, 9)

    def getRSI(self,df_data, window_length):
        """
        this method is used to calculate the RSI index of given data
        :param df_data: DataFrame
        :param window_length: integer standing for the length of the calculation window
        :return: DataFrame
        """
        U = df_data.diff().clip(lower=0)
        D = -1 * df_data.diff().clip(upper=0)
        RS = self.getEMA(U, window_length) / self.getEMA(D, window_length)
        RSI = 1 - 1 / (1 + RS)
        return RSI

    def plotCov(self,df_data):
        """
        this method is used to plot cov matrix of given data
        :param df_data: DataFrame
        :return: None
        """
        if not isinstance(df_data, pd.DataFrame):
            raise ValueError("input data is not a valid DataFrame!")
        indexSample = df_data.index.values[0]
        if not isinstance(indexSample, np.datetime64):
            try:
                new_index = [np.datetime64(date) for date in df_data.index.values]
                df_data.index = new_index
            except:
                raise ValueError("invalid index!")
        f = plt.figure(figsize=(10, 5))
        corr = df_data.pct_change().corr()
        plt.matshow(corr, fignum=f.number)
        plt.xticks(range(df_data.shape[1]), df_data.columns, fontsize=14, rotation=45)
        plt.yticks(range(df_data.shape[1]), df_data.columns, fontsize=14)
        cb = plt.colorbar()
        cb.ax.tick_params(labelsize=14)
        plt.title('Correlation Matrix', fontsize=16)
        plt.show()

    def plotTendency(self,df_data, value):
        """
        this method is used to plot tendencies of given data
        :param df_data: DataFrame
        :param value: like "close", "adjclose"
        :return: None
        """
        df_data.plot(title=value)
        df_data.pct_change().plot(title="{}'s daily percenatage change".format(value))


