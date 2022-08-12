import multiprocessing
import time
import datetime as dt

def producer(q):
    numbers = iter(range(0,100))
    for number in numbers:
        print('++++++++++',number)
        q.put(number)
        print('number is put to queue')
    q.put('STOP')
        
        
def consumer(q):
    for x in iter(q.get,'STOP'):
        time.sleep(0.000001)
        print('----------',x)
    print('queue ended')
    
    
if __name__ == '__main__':
    
    q = multiprocessing.Queue()
    
    p1 = multiprocessing.Process(target=producer,args=(q,))
    p2 = multiprocessing.Process(target=consumer,args=(q,))
    
    p1.start()
    p2.start()
    
    p1.join()
    p1.join()
    

    