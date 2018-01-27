import multiprocessing
import os
import time
from   multiprocessing import Lock, Queue
import pandas as pd
import numpy as np
from latentMatrixInitalizer import initailizerBySVD
import shutil
from usefulTool import clearDir,changeNameToID,objGroupMapGenerator
from sltools import  save_pickle,load_pickle
import gc
from sklearn.model_selection import train_test_split
from evaluate import  RMSE
from multiprocessForGSVD import multiprocessForUser,multiprocessForItem

def dataPreparation(userName, itemName, targetName,userGroupName,itemGroupName):

    data = []

    # divided data by user class  and by item class
    trainSet = pd.read_csv("./originalData/trainSet.csv",usecols=[userName, itemName, targetName, userGroupName, itemGroupName])
    trainSet = trainSet.dropna(axis = 0)

    # 对于 no_type 目前无法处理
    trainSet = trainSet[trainSet.tag != "no type"]

    # # use log transform for stability
    trainSet[targetName] = np.log(trainSet[targetName])


    # # encode user , item

    behavior,mediaid   = changeNameToID(trainSet,itemName,plan='B')
    behavior,userid    = changeNameToID(behavior, userName, plan='B')
    behavior,userGroup = changeNameToID(behavior,userGroupName,plan='B')
    behavior,tag       = changeNameToID(behavior,itemGroupName,plan='B')





    userBelong  = behavior.groupby([userName,userGroupName],as_index=False)[itemName].count()[[userName,userGroupName]]
    userBelong.to_csv("./temp/userBelong.csv",index=False)

    itemBelong  = behavior.groupby([itemName,itemGroupName],as_index=False)[userName].count()[[itemName,itemGroupName]]
    itemBelong.to_csv("./temp/itemBelong.csv",index=False)




    # saving original name

    save_pickle(mediaid, "./temp/mediaid.data")
    save_pickle(userid, "./temp/userid.data")
    save_pickle(userGroup,"./temp/userGroup.data")
    save_pickle(tag, "./temp/tag.data")


    print("encode is ready! starting to split data")


    # get a list ,contains the record of which obj in a special Group

    userdict = objGroupMapGenerator(trainSet, userGroupName,userName)
    itemdict = objGroupMapGenerator(trainSet,itemGroupName,itemName)

    save_pickle(userdict,"./temp/userdict.data")
    save_pickle(itemdict,"./temp/itemdict.data")




    behavior, test = train_test_split(behavior, test_size=0.3)

    behavior.to_csv("./originalData/behavior.csv",index=False)
    print("behavior created successfully !")
    test.to_csv("./originalData/test.csv",index=False)
    print("behavior created successfully !")



    clearDir(".\\dividedByUser\\")
    clearDir(".\\dividedByItem\\")


    for _,data in behavior.groupby(userGroupName):
        data.to_csv( ".\\dividedByUser\\"+str(data[userGroupName].iloc[0])+'.csv',index=False)


    for _,data in behavior.groupby(itemGroupName):
        data.to_csv( ".\\dividedByItem\\"+str(data[itemGroupName].iloc[0])+'.csv',index=False)


    print("data has splited ! ,starting to initailizing paras matrix ! ")

    # 初始化P,Q,S,T　对应的参数
    numOfUser      = load_pickle("./temp/userid.data").keys().__len__()
    numOfItem      = load_pickle("./temp/mediaid.data").keys().__len__()
    numOfUserClass = len(os.listdir(".\\dividedByUser"))
    numOfItemClass = len(os.listdir(".\\dividedByItem"))
    numOfK         = 5


    userLatentFactor, itemLatentFactor,\
    userClassLatentFactor, itemClassLatentFactor = initailizerBySVD(behavior, targetName, numOfUser, numOfItem, numOfK,
                          userBelong,itemBelong,
                          userName,userGroupName,
                          itemName,itemGroupName
                          )

    userLatentFactor.to_csv(".\\oldPQST\\userLatentFactor.txt", sep='\t',index = False)
    userClassLatentFactor.to_csv(".\\oldPQST\\userClassLatentFactor.txt",sep = '\t',index = False)
    itemLatentFactor.to_csv(".\\oldPQST\\itemLatentFactor.txt", sep='\t', index=False)
    itemClassLatentFactor.to_csv(".\\oldPQST\\itemClassLatentFactor.txt",sep = '\t',index = False)



if __name__ == "__main__":
    # data prepare
    os.chdir("C:\\Users\\22560\\Documents\\iptv")

    userGroupName = "userGroup"
    itemGroupName = "tag"
    userName = "newUserID"
    itemName = "TV_NAME"
    targetName = "MEDIACOUNT"


    # you do it only at first time
    dataPreparation(userName, itemName, targetName, userGroupName, itemGroupName)


    # 初始化P,Q,S,T　对应的参数
    numOfUser      = load_pickle("./temp/userid.data").keys().__len__()
    numOfItem      = load_pickle("./temp/mediaid.data").keys().__len__()
    numOfUserClass = len(os.listdir(".\\dividedByUser"))
    numOfItemClass = len(os.listdir(".\\dividedByItem"))
    numOfK         = 5


    ###################    大循环！！！          #############
    epoch = 0
    RMSE_HIS = []
    test = pd.read_csv("./originalData/test.csv")
    while True:
        epoch += 1
        lamda = 15
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

            userCsvPlace = 'C:\\Users\\22560\\Documents\\iptv\\dividedByUser\\'
            processList = []
            loopCount += 1
            # loop untial usr latent factor converge to constant
            for i in range(4):
                process = multiprocessing.Process(
                            name=('loopCountForUser : '+str(loopCount)+' thread : '+str(i)),target=multiprocessForUser,
                            args=(userQueue,userFileLock,userTaskList,userCsvPlace,lamda,
                                  userName, itemName, targetName, userGroupName, itemGroupName
                                    ,'l2'
                                  )
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
            latentFactorOfUserInClass = latentFactorOfUserInClass.set_index(numOfK).reindex(list(range(numOfUser))).fillna(0)
            userList = latentFactorOfUserInClass.index.values
            calculatedUserLatentFactor = latentFactorOfUserInClass.iloc[:,:numOfK].values
            originalUserLatentFactor   = userLatentFactor.values[userList,:]

            cretiriaUser = ((calculatedUserLatentFactor - originalUserLatentFactor)**2).sum()/(numOfUser*numOfK)



            latentFactorOfUserClass   = pd.read_csv(".\\updatedPQST\\lententFactor_of_userClass.csv",header=None)
            userClassList = latentFactorOfUserClass.iloc[:,numOfK].values
            calculatedUserClassLatentFactor = latentFactorOfUserClass.iloc[:, :numOfK].values
            originalUserClassLatentFactor = userClassLatentFactor.values[userClassList, :]
            cretiriaUser += ((calculatedUserClassLatentFactor-originalUserClassLatentFactor)**2).sum()/(numOfUserClass*numOfK)



            print("cretiria  is now at : ", cretiriaUser)

            if latentFactorOfUserInClass.shape[0] == numOfUser:
                # 考虑到这里存在冷启动问题，有30000用户没有参与到训练过程中
                latentFactorOfUserInClass.\
                    to_csv(".\\oldPQST\\userLatentFactor.txt", sep='\t',index=False)
            else:
                print("oops ! the number of user is wrong !")
                assert Exception

            if latentFactorOfUserClass.shape[0] == numOfUserClass:
                latentFactorOfUserClass.\
                        sort_values(by=numOfK).iloc[:, :numOfK].\
                            to_csv(".\\oldPQST\\userClassLatentFactor.txt",
                                   sep='\t', index=False)
            else:
                print("oops ! the number of user  class is wrong !")
                assert Exception

            ###################   rmse early stopping ##############################
            userLatentFactor = pd.read_csv(".\\oldPQST\\userLatentFactor.txt", sep='\t').values
            userClassLatentFactor = pd.read_csv(".\\oldPQST\\userClassLatentFactor.txt", sep='\t').values
            itemLatentFactor = pd.read_csv(".\\oldPQST\\itemLatentFactor.txt", sep='\t').values
            itemClassLatentFactor = pd.read_csv(".\\oldPQST\\itemClassLatentFactor.txt", sep='\t').values
            rmse = RMSE(test,
                        userLatentFactor, itemLatentFactor,
                        userClassLatentFactor, itemClassLatentFactor,
                        userName, itemName, userGroupName, itemGroupName
                        , userNum=-1)
            print("rmse at item loop : ", rmse)
            if cretiriaUser < 0.01 or loopCount >20 or rmse < 0.6:
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

            itemCsvPlace = 'C:\\Users\\22560\\Documents\\iptv\\dividedByItem\\'
            # setting process
            processList = []
            loopCount += 1
            # loop untial usr latent factor converge to constant
            for i in range(4):
                process = multiprocessing.Process(
                    name=('loopCountForItem : ' + str(loopCount) + ' thread : ' + str(i)), target=multiprocessForItem,
                    args=(itemQueue, itemFileLock, itemTaskList, itemCsvPlace, lamda
                          , userName, itemName, targetName, userGroupName, itemGroupName
                          ,'l2'

                          )
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
            latentFactorOfItemInClass = latentFactorOfItemInClass.set_index(numOfK).reindex(list(range(numOfItem))).fillna(0)
            itemList = latentFactorOfItemInClass.index.values
            calculatedItemLatentFactor = latentFactorOfItemInClass.iloc[:, :numOfK].values
            originalItemLatentFactor = itemLatentFactor.values[itemList, :]

            cretiriaItem = ((calculatedItemLatentFactor - originalItemLatentFactor) ** 2).sum() / (numOfItem * numOfK)

            latentFactorOfItemClass = pd.read_csv(".\\updatedPQST\\lententFactor_of_itemClass.csv", header=None)
            itemClassList = latentFactorOfItemClass.iloc[:, numOfK].values
            calculatedItemClassLatentFactor = latentFactorOfItemClass.iloc[:, :numOfK].values
            originalItemClassLatentFactor = itemClassLatentFactor.values[itemClassList, :]
            cretiriaItem += ((calculatedItemClassLatentFactor - originalItemClassLatentFactor) ** 2).sum() / (
            numOfItemClass * numOfK)

            print("cretiria  is now at : ", cretiriaItem)

            if latentFactorOfItemInClass.shape[0] == numOfItem:
                latentFactorOfItemInClass. \
                    to_csv(".\\oldPQST\\itemLatentFactor.txt", sep='\t', index=False)
            else:
                assert Exception

            if latentFactorOfItemClass.shape[0] == numOfItemClass:

                latentFactorOfItemClass. \
                    sort_values(by=numOfK).iloc[:, :numOfK]. \
                    to_csv(".\\oldPQST\\itemClassLatentFactor.txt",
                           sep='\t', index=False)
            else:
                assert Exception


            ###################   rmse early stopping ##############################
            userLatentFactor = pd.read_csv(".\\oldPQST\\userLatentFactor.txt", sep='\t').values
            userClassLatentFactor = pd.read_csv(".\\oldPQST\\userClassLatentFactor.txt", sep='\t').values
            itemLatentFactor = pd.read_csv(".\\oldPQST\\itemLatentFactor.txt", sep='\t').values
            itemClassLatentFactor = pd.read_csv(".\\oldPQST\\itemClassLatentFactor.txt", sep='\t').values
            rmse = RMSE(test,
                        userLatentFactor, itemLatentFactor,
                        userClassLatentFactor, itemClassLatentFactor,
                        userName, itemName, userGroupName, itemGroupName
                        , userNum=-1)
            print("rmse at item loop : ",rmse)
            if cretiriaItem < 0.01 or loopCount >20 or rmse < 0.6:
                break



        userLatentFactor = pd.read_csv(".\\oldPQST\\userLatentFactor.txt", sep='\t').values
        userClassLatentFactor = pd.read_csv(".\\oldPQST\\userClassLatentFactor.txt", sep='\t').values
        itemLatentFactor = pd.read_csv(".\\oldPQST\\itemLatentFactor.txt", sep='\t').values
        itemClassLatentFactor = pd.read_csv(".\\oldPQST\\itemClassLatentFactor.txt", sep='\t').values

        rmse = RMSE(test,
             userLatentFactor, itemLatentFactor,
             userClassLatentFactor, itemClassLatentFactor,
             userName, itemName, userGroupName, itemGroupName
             , userNum=-1)
        RMSE_HIS.append(rmse)
        print("RMSE ON TEST IS ",RMSE_HIS)
        if epoch >20 or rmse < 0.7:
            break





