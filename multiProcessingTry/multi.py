import multiprocessing
from multiprocessing import  Lock
from collections import  deque
from multiprocessing import Queue
from multiprocessing import Manager

def function(lock,a,target,b):
    print("we are running")
    index = a.pop()
    with lock:
        b[0]  = 100


if __name__ == "__main__":
    data = Manager()
    
    target = [1,2,3,4]
    a = deque(list(range(10)))
    b = [1,2,3,4]
    lock = Lock()
    mpList = []
    for i in range(4):
        mp = multiprocessing.Process(target=function,args=(lock,a,target,b))
        mpList.append(mp)
    for  i in mpList:
        i.start()
        i.join()

    print(b)


