import multiprocessing
import time
import datetime


import numpy as np
import pandas as pd

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)



def create_fake_data(conn,times):
    counter = times
    previous_end_time = datetime.datetime.now().time()
    while counter > 0:
        print(counter, datetime.datetime.now().time())
        df = pd.DataFrame()
        while df.memory_usage().sum() < 10000000:
            start = datetime.datetime.now().time()
            data = np.random.rand(40,10)
            data = pd.DataFrame(data)
            data['previous_end_time'] = previous_end_time
            data['start'] = start
            df = pd.concat([df,data])
            previous_end_time = datetime.datetime.now().time()
            # print('df_created')
        conn.send(df)
        print(counter, datetime.datetime.now().time(),'df sended')
        counter -= 1
    conn.send('end')
    print(counter, datetime.datetime.now().time(),'conn to be closed')
    conn.close()
    
def save_to_csv(conn):
    counter = 0
    while 1:
        df = conn.recv()
        if type(df) == str:
            print(df)
            print('save_done')
            break
        print('df received',datetime.datetime.now().time())
        df.to_csv(str(counter)+'.csv')
        print('df saved',datetime.datetime.now().time())
        counter += 1

        
if __name__ == '__main__':
    parent_conn, child_conn = multiprocessing.Pipe()

    # creating new processes
    p1 = multiprocessing.Process(target=create_fake_data, args=(parent_conn,10))
    p2 = multiprocessing.Process(target=save_to_csv, args=(child_conn,))

    # running processes
    p1.start()
    p2.start()

    # wait until processes finish
    p1.join()
    p2.join()