import multiprocessing
import os
import time
from   multiprocessing import Lock, Queue
import pandas as pd
import numpy as np
from latentMatrixInitalizer import randomInitalizer
import shutil
from usefulTool import clearDir,changeNameToID
from sltools import  save_pickle,load_pickle
import gc

import gc
from multiprocessForGSVD import multiprocessForUser,multiprocessForItem





if __name__ == "__main__":
    # data prepare
    os.chdir("C:\\Users\\22560\\Documents\\iptv")


    # data = []
    #
    # # divided data by user class  and by item class
    # trainSet = pd.read_csv("trainSet.csv",usecols = ['newUserID', 'name', 'MEDIACOUNT','userGroup','tag'])
    # trainSet = trainSet.dropna(axis = 0)
    #
    # # use log transform for stability
    # trainSet['MEDIACOUNT'] = np.log(trainSet['MEDIACOUNT'])
    # # encode user , item
    #
    #
    # behavior,mediaid   = changeNameToID(trainSet,'name',plan='B')
    # behavior,userid    = changeNameToID(behavior, 'newUserID', plan='B')
    # behavior,userGroup = changeNameToID(behavior,'userGroup',plan='B')
    # behavior,tag       = changeNameToID(behavior,'tag',plan='B')
    #
    # # saving original name
    #
    # save_pickle(mediaid, "mediaid.data")
    # save_pickle(userid, "userid.data")
    # save_pickle(userGroup,"userGroup.data")
    # save_pickle(tag, "tag.data")
    #
    #
    #
    # print("encode is ready! starting to split data")
    #
    # for _,data in behavior.groupby('userGroup'):
    #     data.to_csv( ".\\dividedByUser\\"+str(data.userGroup.iloc[0])+'.csv',index=False)
    #
    #
    #
    # for _,data in behavior.groupby('tag'):
    #     data.to_csv( ".\\dividedByItem\\"+str(data.tag.iloc[0])+'.csv',index=False)
    #

    print("data has splited ! ,starting to initailizing paras matrix ! ")

    # 初始化P,Q,S,T　对应的参数
    numOfUser      = load_pickle("userid.data").keys().__len__()
    numOfItem      = load_pickle("mediaid.data").keys().__len__()
    numOfUserClass = len(os.listdir(".\\dividedByUser"))
    numOfItemClass = len(os.listdir(".\\dividedByItem"))
    numOfK         = 7

    # generate data # 本步骤有待于后续开发


    userLatentFactor = randomInitalizer(numOfUser, numOfK)
    userClassLatentFactor = randomInitalizer(numOfUserClass,numOfK)
    itemLatentFactor = randomInitalizer(numOfItem, numOfK)
    itemClassLatentFactor = randomInitalizer(numOfItemClass,numOfK)


    userLatentFactor.to_csv(".\\oldPQST\\userLatentFactor.txt", sep='\t',index = False)
    userClassLatentFactor.to_csv(".\\oldPQST\\userClassLatentFactor.txt",sep = '\t',index = False)
    itemLatentFactor.to_csv(".\\oldPQST\\itemLatentFactor.txt", sep='\t', index=False)
    itemClassLatentFactor.to_csv(".\\oldPQST\\itemClassLatentFactor.txt",sep = '\t',index = False)



    ###################    构建 user 部分 的 任务 #############
    loopCount = -1

    while(True):
        userTaskList = os.listdir(".\\dividedByUser")
        userTaskNum = len(userTaskList)

        # 构造任务队列：
        userQueue = Queue()
        for indexOfFile in range(userTaskNum):
            userQueue.put(indexOfFile)

        # 构造锁死文件的lock
        userFileLock = Lock()

        # UPDATING USERLATENT FACTOR AND USERCLASS LATENT FACTOR
        # 使用多线程处理：
        lamda = 0.01
        userCsvPlace = 'C:\\Users\\22560\\Documents\\iptv\\dividedByUser\\'
        processList = []
        loopCount += 1
        # loop untial usr latent factor converge to constant
        for i in range(4):
            process = multiprocessing.Process(
                        name=('loopCountForUser : '+str(loopCount)+' thread : '+str(i)),target=multiprocessForUser,
                        args=(userQueue,userFileLock,userTaskList,userCsvPlace,lamda)
            )
            processList.append(process)
        for process in processList:
            process.start()

        # 使用join 方法，等待子进程结束
        for process in processList:
            process.join()

        # 继续执行下一步操作
        print("userDataPrepared !")

        # updating value of usrlatentFactor , userClassLatentFactor
        userLatentFactor = pd.read_csv(".\\oldPQST\\userLatentFactor.txt", sep='\t')
        userClassLatentFactor = pd.read_csv(".\\oldPQST\\userClassLatentFactor.txt", sep='\t')



        # check if we should loop again:
        latentFactorOfUserInClass = pd.read_csv(".\\updatedPQST\\lententFactor_of_user_in_class.csv",header=None)
        userList = latentFactorOfUserInClass.iloc[:,7].values
        calculatedUserLatentFactor = latentFactorOfUserInClass.iloc[:,:7].values
        originalUserLatentFactor   = userLatentFactor.values[userList,:]

        cretiria = ((calculatedUserLatentFactor - originalUserLatentFactor)**2).sum()/(numOfUser*numOfK)



        latentFactorOfUserClass   = pd.read_csv(".\\updatedPQST\\lententFactor_of_userClass.csv",header=None)
        userClassList = latentFactorOfUserClass.iloc[:,7].values
        calculatedUserClassLatentFactor = latentFactorOfUserClass.iloc[:, :7].values
        originalUserClassLatentFactor = userClassLatentFactor.values[userClassList, :]
        cretiria += ((calculatedUserClassLatentFactor-originalUserClassLatentFactor)**2).sum()/(numOfUserClass*numOfK)



        print("cretiria  is now at : ", cretiria)
        latentFactorOfUserInClass.\
                sort_values(by=7).iloc[:, :7].\
                    to_csv(".\\oldPQST\\userLatentFactor.txt",
                           sep='\t',index=False)
        latentFactorOfUserClass.\
                sort_values(by=7).iloc[:, :7].\
                    to_csv(".\\oldPQST\\userClassLatentFactor.txt",
                           sep='\t', index=False)

        if loopCount >= 0 :
            break


    ###################    构建 item 部分 的 任务 #############
    loopCount = -1
    while (True):
        itemTaskList = os.listdir(".\\dividedByItem")
        itemTaskNum = len(itemTaskList)

        # 构造任务队列：
        itemQueue = Queue()
        for indexOfFile in range(itemTaskNum):
            itemQueue.put(indexOfFile)

        # 构造锁死文件的lock
        itemFileLock = Lock()

        # UPDATING USERLATENT FACTOR AND USERCLASS LATENT FACTOR
        # 使用多线程处理：
        lamda = 0.01
        itemCsvPlace = 'C:\\Users\\22560\\Documents\\iptv\\dividedByItem\\'
        # setting process
        processList = []
        loopCount += 1
        # loop untial usr latent factor converge to constant
        for i in range(4):
            process = multiprocessing.Process(
                name=('loopCountForItem : ' + str(loopCount) + ' thread : ' + str(i)), target=multiprocessForItem,
                args=(itemQueue, itemFileLock, itemTaskList, itemCsvPlace, lamda)
            )
            processList.append(process)
        for process in processList:
            process.start()

        # 使用join 方法，等待子进程结束
        for process in processList:
            process.join()

        # 继续执行下一步操作
        print("userDataPrepared !")

        # updating value of usrlatentFactor , userClassLatentFactor

        itemLatentFactor = pd.read_csv(".\\oldPQST\\itemLatentFactor.txt", sep='\t')
        itemClassLatentFactor = pd.read_csv(".\\oldPQST\\itemClassLatentFactor.txt", sep='\t')

        # check if we should loop again:
        latentFactorOfItemInClass = pd.read_csv(".\\updatedPQST\\lententFactor_of_item_in_class.csv", header=None)
        itemList = latentFactorOfItemInClass.iloc[:, 7].values
        calculatedItemLatentFactor = latentFactorOfItemInClass.iloc[:, :7].values
        originalItemLatentFactor = itemLatentFactor.values[itemList, :]

        cretiria = ((calculatedItemLatentFactor - originalItemLatentFactor) ** 2).sum() / (numOfItem * numOfK)

        latentFactorOfItemClass = pd.read_csv(".\\updatedPQST\\lententFactor_of_itemClass.csv", header=None)
        itemClassList = latentFactorOfItemClass.iloc[:, 7].values
        calculatedItemClassLatentFactor = latentFactorOfItemClass.iloc[:, :7].values
        originalItemClassLatentFactor = itemClassLatentFactor.values[itemClassList, :]
        cretiria += ((calculatedItemClassLatentFactor - originalItemClassLatentFactor) ** 2).sum() / (numOfItemClass * numOfK)

        print("cretiria  is now at : ", cretiria)
        latentFactorOfItemInClass. \
            sort_values(by=7).iloc[:, :7]. \
            to_csv(".\\oldPQST\\itemLatentFactor.txt",
                   sep='\t', index=False)
        latentFactorOfItemClass. \
            sort_values(by=7).iloc[:, :7]. \
            to_csv(".\\oldPQST\\itemClassLatentFactor.txt",
                   sep='\t', index=False)

        if loopCount >= 1 :
            break






