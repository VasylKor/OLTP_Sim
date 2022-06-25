from funcs import *
import configparser
import logging
import os
import sys

# Setting folder as working directory          
pathname = os.path.dirname(os.path.abspath(__file__))
os.chdir(pathname)



logging.basicConfig(filename="../logs/logs.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

# getting current datetime
now = datetime.now()


config = configparser.ConfigParser()
config.read("../config/config.txt")

params = config["VARS"]
 

db_gen = params["db_gen"]
db_oltp = params["db_oltp"]
cust_per_month = int(params["cust_per_month"])
user= params["user"]
password= params["password"]
host= params["host"]
port= int(params["port"])


logging.info("Importing buys")

buy_times = import_day_purchases(cust_per_month)

# Setting the last shift for today

last_shift = now.replace(hour=21, minute=0, second=0)

logging.info("Starting OLTP")

# Stop the script run at last shift
# or when the last transaction occur
while datetime.now() < last_shift:
    to_process = buy_times.loc[buy_times['times'] < datetime.now()]
    if to_process.shape[0] != 0:
        generate_receipts(user, password, host, port, db_gen, db_oltp)
        idx = to_process.iloc[0,:]['index']
        buy_times = buy_times[buy_times['index'] != idx]  
    elif buy_times.shape[0] == 0:
        break

logging.info("Closing OLTP")
