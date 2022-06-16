from funcs import *
import configparser
import logging

logging.basicConfig(filename='logs',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logging.info("Importing buys")


config= configparser.RawConfigParser()   
configFilePath = r'config.txt'
config.read(configFilePath)

db_gen = config.get('VARS', 'db_gen')
db_oltp = config.get('VARS', 'db_oltp')
cust_per_month = int(config.get('VARS', 'cust_per_month'))
user= config.get('VARS', 'user')
password= config.get('VARS', 'password')
host= config.get('VARS', 'host')
port= int(config.get('VARS', 'port'))



cust_per_week = int(cust_per_month/7)
cust_per_day_week = int((cust_per_week * 0.6) / 5)
cust_per_day_weekend = int((cust_per_week * 0.4) / 2)

# getting current datetime
now = datetime.now()

# getting number of buys per hour today
if now.weekday() in [0,1,2,3,4]:
    buys = buyers_distr_week(cust_per_day_week)
else:
    buys = buyers_distr_weekend(cust_per_day_weekend)
    
# Obtaining list of timestamps from 
# number of buys per day distribuition.
# Linking times to today's date
buys = buys.loc[buys['count'] <2]
buys_times = np.array(buys['time'])

hours = buys_times
minutes = hours%1
hours = list((hours-minutes).astype(int))
minutes = minutes*60
seconds = minutes%1
minutes = list((minutes-seconds).astype(int))
seconds = list((seconds*60).astype(int))

buy_times = []
for i in range (len(hours)):
    new_time=now.replace(hour=hours[i], minute=minutes[i], second=seconds[i])
    buy_times.append(new_time)


buy_times = pd.Series(buy_times, name = 'times')
buy_times = buy_times.reset_index()

# Setting the last shift for today

last_shift = now.replace(hour=21, minute=0, second=0)

logging.info("Starting OLTP")

# Stop the script run at last shift
# or when the last transaction occur
while datetime.now() < last_shift:
    to_process = buy_times.loc[buy_times['times'] < now]
    if to_process.shape[0] != 0:
        generate_receipts(user, password, host, port, db_gen, db_oltp)
        idx = to_process.iloc[0,:]['index']
        buy_times = buy_times[buy_times['index'] != idx]  
    elif buy_times.shape[0] == 0:
        break
        
logging.info("Closing OLTP")
