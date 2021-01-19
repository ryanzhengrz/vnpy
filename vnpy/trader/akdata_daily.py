import akshare as ak
import pandas as pd
import sqlite3
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from datetimerange import DateTimeRange
from vnpy.trader.database import database_manager
from vnpy.trader.object import BarData
from vnpy.trader.constant import Exchange, Interval

# The progress function shows the stock data loading progress in percentage bar
def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r\n' % (bar, percents, '%', status))
    sys.stdout.flush()
  
# The comp_bar function compare the daily stock prices (from a bar type and a dataframe type), and return match or not matched
def comp_bar(bar_var, df_var):
    lastDayDate_ex = bar_var.datetime.replace(tzinfo=None)

    bar_open = bar_var.open_price
    df_open = df_var.loc[lastDayDate_ex].open
    bar_close = bar_var.close_price
    df_close = df_var.loc[lastDayDate_ex].close
    bar_high = bar_var.high_price
    df_high = df_var.loc[lastDayDate_ex].high
    bar_low = bar_var.low_price
    df_low = df_var.loc[lastDayDate_ex].low
    bar_vol = bar_var.volume
    df_vol = df_var.loc[lastDayDate_ex].volume

    flag = (bar_open == df_open) and (bar_close == df_close) and (bar_high == df_high) and (bar_low == df_low) and (bar_vol == df_vol)
    return flag


def main():
    db_conn1 = sqlite3.connect(r'D:\PythonCodes\vnpy\.vntrader\symb_list.db')   # Connect to a database that hosts all stock symbols (i.e. corporate database)
    db_conn2 = sqlite3.connect(r'D:\PythonCodes\vnpy\.vntrader\database.db')    # Connect to the main database that host all stock data (i.e. bar database)

    # dict_example = {'name':['Microsoft Corp.'], 
    #             'cname': ['微软公司'], 
    #             'symbol':['MSFT']}
    # dict_example = {'name':['Microsoft Corp.','Apple Company'], 
    #             'cname': ['微软公司', '苹果公司'], 
    #             'symbol':['MSFT', 'AAPL']}
    # dict_example = {'name':['Microsoft Corp.','Apple Company', 'Facebook'], 
    #             'cname': ['微软公司', '苹果公司', '脸书'], 
    #             'symbol':['MSFT', 'AAPL', 'FB']}
    # dict_example = {'name':['Microsoft Corp.','Apple Company', 'Facebook', 'Amazon'], 
    #             'cname': ['微软公司', '苹果公司', '脸书','亚马逊'], 
    #             'symbol':['MSFT', 'AAPL', 'FB', 'AMZN']}
    # df_example = pd.DataFrame.from_dict(dict_example)
    # df_example.to_csv(Path.cwd().joinpath('temp.csv'), encoding='utf_8_sig', index=False)
    # df_example = pd.read_csv(Path.cwd().joinpath('temp.csv'), encoding='utf_8_sig')

    # df_allstocks = ak.get_us_stock_name()     # Download all stock symbols using AkShare service. Expect to run this line and update stock symbols periodically.
    # df_allstocks.to_csv(Path.cwd().joinpath('temp.csv'), encoding='utf_8_sig', index=False)   # Save all stock symbols to a csv file. This is for testing purpose.
    df_example = pd.read_csv(Path.cwd().joinpath('temp.csv'), encoding='utf_8_sig')     # Load all stock symbols from the csv file. This is for testing purpose.
    # df_example = df_example.iloc[0:2, :]    # Only take a few lines for testing. This is for testing purpose.

    df_example.to_sql("dbcorpdata", db_conn1, if_exists='replace')      # Save all stock symbols to corporate database

    df_corpsdata_dl = pd.read_sql_query("SELECT * from dbcorpdata", db_conn1)       # Load all stock symbols from corporate database
    df_bardata_ex = pd.read_sql_query("SELECT * from dbbardata", db_conn2)      # Load all existing stock data from bardata database

    totalSymbol = len(df_corpsdata_dl.symbol)   
    procSymbol = 0
    forcedFullLoad = False

    for s in df_corpsdata_dl.symbol:        # For each symbol read from the corporate database

        try:
            procSymbol += 1
            progress(procSymbol, totalSymbol)
            bars = []

            bar_latestBarOneStock_ex = database_manager.get_newest_bar_data(s, Exchange.LOCAL, Interval.DAILY)  # Find the latest bar record for that symbol from the bar database
            df_allBarOneStock_dl = ak.stock_us_daily(symbol=s, adjust="qfq").fillna(method = 'ffill').fillna(0)  # Download the history data for that symbol using AkShare service. 
                                                                                                                # Fill NaN or Null fields with previous value, and then zero.

            if ((bar_latestBarOneStock_ex is not None) and (~forcedFullLoad)):   # If the bar database contains this symbol, and not full load we will decide if incremental data will be needed or full history data will be saved to bar database
                lastDayDate_ex = bar_latestBarOneStock_ex.datetime.replace(tzinfo=None)     # VNPY datetime is aware type, but the AkShare datetime is unaware type
                latestDayDate_dl = df_allBarOneStock_dl.index[-1]   # Be careful of the difference between last day and latest date.

                dailyDataMatched = comp_bar(bar_latestBarOneStock_ex, df_allBarOneStock_dl) # This is a simplified logic check to compare the OHLC prices and see if they are all equal.

                if dailyDataMatched:  # If the close prices from existing and new sources match, we assume data remain correct and will only incrementally update
                    time_range = DateTimeRange(lastDayDate_ex, latestDayDate_dl)    # Find the date range for incremental update
                    for dt in time_range.range(timedelta(days=1)):
                        # print(dt)
                        if dt == latestDayDate_dl:  # When last date equals latest date, there is still a day in the date range, and we need to break the loop
                            # print('I am going to break...')
                            break
                        bar = BarData(
                            symbol=s,
                            exchange=Exchange.LOCAL,
                            datetime=dt,    # Here dt is a native datetime object
                            interval=Interval.DAILY,
                            volume=df_allBarOneStock_dl.loc[dt].volume,
                            open_price=df_allBarOneStock_dl.loc[dt].open,
                            high_price=df_allBarOneStock_dl.loc[dt].high,
                            low_price=df_allBarOneStock_dl.loc[dt].low,
                            close_price=df_allBarOneStock_dl.loc[dt].close,
                            open_interest=0,
                            gateway_name='Sim'
                        )
                        bars.append(bar)   
                        # print('only add incremental updates for '+s)

                else:       # If the close prices from existing and new sources do not match, we assume data are corrupted and will fully update
                    for i, dt in enumerate(df_allBarOneStock_dl.index):
                        bar = BarData(
                            symbol=s,
                            exchange=Exchange.LOCAL,
                            datetime=dt.to_pydatetime(),    # Convert to a datetime object
                            interval=Interval.DAILY,
                            volume=df_allBarOneStock_dl.loc[dt].volume,
                            open_price=df_allBarOneStock_dl.loc[dt].open,
                            high_price=df_allBarOneStock_dl.loc[dt].high,
                            low_price=df_allBarOneStock_dl.loc[dt].low,
                            close_price=df_allBarOneStock_dl.loc[dt].close,
                            open_interest=0,
                            gateway_name='Sim'
                        )
                        bars.append(bar)
                        # print('correct database data for '+s)

            else:   # If bar database does not have this symbol, or just want to force full load,  we will fully update
                for i, dt in enumerate(df_allBarOneStock_dl.index):
                    bar = BarData(
                            symbol=s,
                            exchange=Exchange.LOCAL,
                            datetime=dt.to_pydatetime(),    # Convert to a datetime object
                            interval=Interval.DAILY,
                            volume=df_allBarOneStock_dl.loc[dt].volume,
                            open_price=df_allBarOneStock_dl.loc[dt].open,
                            high_price=df_allBarOneStock_dl.loc[dt].high,
                            low_price=df_allBarOneStock_dl.loc[dt].low,
                            close_price=df_allBarOneStock_dl.loc[dt].close,
                            open_interest=0,
                            gateway_name='Sim'
                    )
                    bars.append(bar)
                    # print('reload data for '+s)

            database_manager.save_bar_data(bars)    # Push the updates to the bar database
            print("Saved stock data of "+s+" into database.")

        except:     # When exceptoin occurs, assume it is because database buffer full, reconnect databases.
                time.sleep(5)
                print('Exception detected. Now reconnect to the databases.')
                db_conn1.close()    
                db_conn2.close()   
                db_conn1 = sqlite3.connect(r'D:\PythonCodes\vnpy\.vntrader\symb_list.db')   
                db_conn2 = sqlite3.connect(r'D:\PythonCodes\vnpy\.vntrader\database.db')    

    time.sleep(5)
    db_conn1.close()    # When done with the database, close the connection
    db_conn2.close()    # When done with the database, close the connection


if __name__ == "__main__":
    main()