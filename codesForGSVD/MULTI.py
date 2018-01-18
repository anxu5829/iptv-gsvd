from collections import deque
from multiprocessing import Manager
import multiprocessing
from multiprocessing import Lock
from multiprocessing import Array
import numpy as np

def multi(q,lock):
    global dict
    while(True):
        if q:
            index = q.pop()
            with lock:
                output = open('data', 'a')
                output.write(str(index)+'str \n')
                output.close()
        else:
            break
if __name__ == "__main__":
    # manager = Manager()
    # dict = manager.dict()
    # dict['p'] = np.zeros(10)


    lock = Lock()
    mpList = []
    for i in range(10):
        mp = multiprocessing.Process(target=multi,args=(q,lock))
        mpList.append(mp)
    for i in mpList:
        i.start()
        i.join()
