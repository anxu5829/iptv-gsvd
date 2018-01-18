import pandas as pd
import os
import multiprocessing
from   multiprocessing import  Lock
from multiprocessing import Queue
import  time


def multiprocess(queue,fileLock):
    name  = multiprocessing.current_process().name

    while(True):
        if not queue.empty():
            # get the indexOffile

            print("name :" + str(name) + "is running !")
            indexOfFile = queue.get()
            # 如果拿到的indexOfFile是第一个，那么就清空output.txt文件
            if indexOfFile == 0 :
                os.remove("output.txt")
            df = pd.read_csv('\\data\\'+str(indexOfFile)+'.csv')
            print("data " + str(indexOfFile) +"is Loaded!")
            # sleep is a simulate of the process of generating data
            time.sleep(10)
            with fileLock:
                with open('output.txt','a') as output:
                    output.write(str(df['age'].mean())+'\t'+ str(indexOfFile) +'\n')
                    print('output.csv has changed!')

        else:
            break



if __name__ == "__main__":
    # data prepare
    os.chdir("C:\\Users\\22560\\PycharmProjects\\iptv\\multiProcessingTry")
    data = []
    taskNum  = 100
    for i in range(taskNum):
        data_i = pd.DataFrame({"name": ['name1', 'name2', 'name3'], "age": [18,19,20],})
        data.append(data_i)
    for index,data_test in enumerate(data):
        data_test.to_csv('\\data\\'+str(index)+'.csv',index= False)

    # 构造任务队列：
    queue = Queue()
    for  indexOfFile in range(taskNum):
        queue.put(indexOfFile)
    # 构造锁死文件的lock
    fileLock = Lock()

    # 使用多线程处理：
    processList = []
    for  i in range(10):
        process = multiprocessing.Process(name=str(i),target=multiprocess,args=(queue,fileLock))
        processList.append(process)
    for  process in processList:
        process.start()


    # 使用join 方法，等待子进程结束
    for  process in processList:
        process.join()

    # 继续执行下一步操作
    print("hello world !!")




