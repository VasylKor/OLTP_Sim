import mysql.connector as connection
import mariadb

from scipy.stats import skewnorm
import pandas as pd
import numpy as np

import random
from datetime import datetime
import uuid
import sys


def generate_customers(user, pwd, host, port, db_gen, db_oltp):
    """
    Creates customers and their info in other other
    table in OLTP DB. The two DBs must be in the same
    server.
    """    
    try:
        conn = mariadb.connect(
            user=user,
            password=pwd,
            host=host,
            port=port,
            database=db_gen

        )

        # Disable Auto-Commit
        conn.autocommit = False
        
    except mariadb.Error as e: 
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    cur = conn.cursor()

    
    # generating random number to use as reference
    # to extract data from gen tables
    name_pos = random.randint(0,2000000)
    surname_pos = random.randint(0,2000000)
    nationality_pos = random.randint(0,2000000)
    address_pos = random.randint(0,2000000)


    # extracting person data
    try:
        mydb = connection.connect(host=host, database = db_gen,user=user, passwd=pwd,use_pure=True)
        
        query = f"""select n.name, n1.surname, n.sex, n2.nationality from
                (SELECT name, sex, 1 as plh FROM names where Id = {name_pos}) n 
                join (select surname, 1 as plh from names where Id = {surname_pos}) n1 on n1.plh=n.plh 
                join (select nationality , 1 as plh from names where Id = {nationality_pos}) n2 on n2.plh=n.plh ;"""
        customer = pd.read_sql(query,mydb)
        
        
        mydb.close()
    except Exception as e:
        mydb.close()
        print(str(e))


    #adding address_id to person info
    customer['address_id'] = address_pos


    #inserting in OLTP addresses and customer tables
    try:
        query = f"""insert into {db_oltp}.addresses 
                    select * from {db_gen}.addresses a where a.Id = {address_pos}
                    ON DUPLICATE KEY UPDATE id=a.id
        """
        cur.execute(query)
        
        for index, row in customer.iterrows():
            query = f"""
                    insert into {db_oltp}.customers (name, surname, sex, nationality, address_id)
                    VALUES 
                    ('{row['name']}','{row['surname']}','{row['sex']}','{row['nationality']}',{row['address_id']})
            """
            cur.execute(query)
        conn.commit()
        
        conn.close()
    except Exception as e:
        conn.close()
        print(str(e))



def generate_receipts(user, pwd, host, port, db_gen, db_oltp):
    '''
    Generate receipts and receipts lines
    for customer purchase.
    The two DBs must be in the same
    server.
    '''
    try:
        conn = mariadb.connect(
            user=user,
            password=pwd,
            host=host,
            port=port,
            database=db_gen

        )
        
        # Disable Auto-Commit
        conn.autocommit = False
        
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)


    cur = conn.cursor()

    receipt_id = str(uuid.uuid4())

    from scipy.stats import skewnorm
    import matplotlib.pyplot as plt

    # Getting number of things bought

    num_values = 10000
    max_value = 100
    skewness = 33

    rndm = skewnorm.rvs(a = skewness,loc=max_value, size=num_values) 

    rndm = rndm - min(rndm)      #Shift the set so the minimum value is equal to zero.
    rndm = rndm / max(rndm)      #Standadize all the vlues between 0 and 1. 
    rndm = rndm * max_value         #Multiply the standardized values by the maximum value.

    num_things_purchased = int(random.choice(rndm))



    # Choosing the store in which to buy stuff
    cur.execute(f"SELECT Id FROM {db_oltp}.shops ")
    shops = cur.fetchall()
    shop_id = random.choice(shops)[0]

    # Getting list of stuff they have
    cur.execute(f"SELECT CodArt, Price FROM {db_oltp}.detshop where IdList={shop_id} ")
    products = cur.fetchall()

    # Getting barcodes (because is what 
    # is read in supermarket)
    cur.execute(f"SELECT CodArt, barcode FROM {db_oltp}.barcode")
    barcodes = cur.fetchall()
    barcodes = pd.DataFrame(barcodes, columns = ['product_id','barcode'])

    # Getting customer
    cur.execute(f"SELECT Id FROM {db_oltp}.customers")
    customers = cur.fetchall()

    exec_code= random.randint(0,4)
    if exec_code == 0 or exec_code == 3:
        customer_id=-1
    else:
        # Getting customer id
        cur.execute(f"SELECT Id FROM {db_oltp}.customers")
        customers = cur.fetchall()

        if not customers:
            
            customer_id=-1
            
        else:
            customer_id=random.choice(customers)[0] 

    # Buying products
    shop_list = []

    for i in range(num_things_purchased):
        # choosing product
        product_item = random.choice(products)
        
        #removing it from list of products
        products.remove(product_item)
        
        product_id = product_item[0]
        price = product_item[1]
        
        if price < 3:
            qty = random.randint(1,3)
        elif price < 15:
            qty = random.randint(1,2)
        else:
            qty = 1
            
        
        shop_line = [product_id, qty, price]
        
        shop_list.append(shop_line)

    cart = pd.DataFrame(shop_list, columns=['product_id',
                                            'qty','price'])

    cart = cart.merge(barcodes, on='product_id')

    cart['receipt_id'] = receipt_id

    now = datetime.now()

    date = now.strftime('%Y-%m-%d')
    time = now.strftime('%H:%M:%S')

    # Creating receipt row
    total = sum(cart['price']*cart['qty'])
    receipt = pd.DataFrame({'receipt_id':[receipt_id],'shop_id':[shop_id],
                'total':[total],'customer_id':[customer_id],
                'date':[date], 'time':[time]})

    try:  
        for index, row in receipt.iterrows():
            query = f"""
                    insert into {db_oltp}.receipts (receipt_id, shop_id, total, customer_id, date, time)
                    VALUES 
                    ('{row['receipt_id']}',{row['shop_id']},{row['total']},{row['customer_id']},'{row['date']}','{row['time']}')
            """
            cur.execute(query)
        
        for index, row in cart.iterrows():
            query = f"""
                    insert into {db_oltp}.receipt_lines (receipt_id, product_id, qty, single_price, barcode)
                    VALUES 
                    ('{row['receipt_id']}','{row['product_id']}',{row['qty']},{row['price']},{row['barcode']})
            """
            cur.execute(query)
        
        conn.commit()
        
        conn.close()
    except Exception as e:
        conn.close()
        print(str(e))



def buyers_distr_week(buyers_num):
    '''
    Returns number of buys for a 
    normal day (mon-fri) per each hour.
    '''

    # Getting distribuition skewed towards the middle hours of work

    num_values = int(buyers_num/2)
    max_value = 13
    skewness = 4
    rndm = skewnorm.rvs(a = skewness,loc=max_value, size=num_values) 

    rndm = rndm - min(rndm)      #Shift the set so the minimum value is equal to zero.
    rndm = rndm / max(rndm)      #Standadize all the vlues between 0 and 1. 
    rndm = rndm * max_value         #Multiply the standardized values by the maximum value.

    num_values = int(buyers_num/2)
    max_value = 13
    skewness = -10

    # Getting distribuition skewed towards the last hours of work

    rndm1 = skewnorm.rvs(a = skewness,loc=max_value, size=num_values) 

    rndm1= rndm1 - min(rndm1)  
    rndm1 = rndm1 / max(rndm1)    
    rndm1 = rndm1 * max_value  

    rndm2 = np.append(rndm, rndm1)

    rndm2 = rndm2 + 8

    rndm2.sort()

    output = pd.DataFrame(rndm2).value_counts().sort_index()
    output = output.rename('count', inplace=True)
    output = output.reset_index()
    output = output.rename(columns={0:'time'})

    return output


def buyers_distr_weekend(buyers_num):
    
    '''
    Returns number of buys for a 
    weekend day (sat-sun) per each hour.
    '''
    
    num_values = buyers_num
    max_value = 13
    skewness = 4.5

    rndm = skewnorm.rvs(a = skewness,loc=max_value, size=num_values) 

    rndm = rndm - min(rndm)      #Shift the set so the minimum value is equal to zero.
    rndm = rndm / max(rndm)      #Standadize all the vlues between 0 and 1. 
    rndm = rndm * max_value         #Multiply the standardized values by the maximum value.

    rndm = rndm + 8

    rndm.sort()

    output = pd.DataFrame(rndm).value_counts().sort_index()
    output = output.rename('count', inplace=True)
    output = output.reset_index()
    output = output.rename(columns={0:'time'})

    return output
