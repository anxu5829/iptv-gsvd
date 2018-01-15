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
from sklearn import linear_model
import gc



def multiprocessForUser(queue,fileLock,taskList,csvPlace,lamda):

    # queue = userQueue
    # filelock = userFileLock
    # taskList = userTaskList
    # csvPlace = userCsvPlace
    # fileLock = userFileLock


    name  = multiprocessing.current_process().name

    while(True):
        if not queue.empty():
            # remove all the files in the updataPQST
            # get the indexOffile
            print("name :" + str(name) + "  is running !")
            # indexOfFile equals to indexOfLatent
            indexOfFile =queue.get()

            print("name : " + str(name) + "  is doing " + str(indexOfFile)+ "  userClass")
            # 如果拿到的indexOfFile是第一个，那么就清空output.txt文件
            if indexOfFile == 0 :
                rootdir  = "C:\\Users\\22560\\Documents\\iptv\\updatedPQST\\"
                clearDir(rootdir)

            #loading data

            data = pd.read_csv(csvPlace+taskList[indexOfFile])

            # loading latent info
            itemClassLatentFactor = pd.read_table(".\\oldPQST\\itemClassLatentFactor.txt").values
            userClassLatentFactor = pd.read_table(".\\oldPQST\\userClassLatentFactor.txt").values
            userLatentFactor = pd.read_table(".\\oldPQST\\userLatentFactor.txt").values
            itemLatentFactor = pd.read_table(".\\oldPQST\\itemLatentFactor.txt").values

            # extract useful infomation
            # getting user list , item list , userClass , itemClass
            userList  = data['newUserID'].unique()
            itemList  = data['name'].unique()
            userClass = data['userGroup'].unique()
            itemClass = data['tag'].unique()


            # now , our target is updating user info:

            ##### extract info for user :
            userLentFacorForUserList = []
            counter = 0
            for userNowDealing in userList:
                counter +=1
                if counter%3000 == 0:
                    print('name ',name,' :  we have finished  :',counter  / len(userList), 'percent of ', indexOfFile,' user class ')
                dataUsed    = data[data['newUserID'] == userNowDealing]
                targetUsed     = dataUsed['MEDIACOUNT']
                itemLatentFactorUsed      = itemLatentFactor[ dataUsed['name'],:]
                itemClassLatentFactorUsed = itemClassLatentFactor[ dataUsed['tag'],:  ]

                # attention : cause of user is in one group, so we can just extract one row form useClsLntfctr
                userClassLatentFactorUsed = userClassLatentFactor[ indexOfFile,:  ]

                # record p+s & y-(p+s)*t
                ps = itemClassLatentFactorUsed + itemLatentFactorUsed
                yforReg  = targetUsed - ps.dot(userClassLatentFactorUsed)
                reg = linear_model.Ridge(alpha=lamda)
                reg.fit(ps,yforReg)
                userLentFacorForUserList.append(reg.coef_)

            userLentFacorForUserList = pd.DataFrame(userLentFacorForUserList)
            userLentFacorForUserList['userList'] = userList
            with fileLock:
                print(" is writting ", "class " , indexOfFile," data now")
                print(userLentFacorForUserList.shape)
                with open('.\\updatedPQST\\lententFactor_of_user_in_class.csv','a') as f:
                    userLentFacorForUserList.to_csv(f,index=False,header=False)


            del userLentFacorForUserList,reg,dataUsed,ps,yforReg,targetUsed,itemLatentFactorUsed,itemClassLatentFactorUsed
            gc.collect()
            # output the result and release the data

            ##### extract info for userClass :
            print("user in class : ",indexOfFile," latent factor is prepared! ")

            targetUsed = data['MEDIACOUNT']
            itemLatentFactorUsed = itemLatentFactor[data['name'], :]
            itemClassLatentFactorUsed = itemClassLatentFactor[data['tag'], :]
            userLatentFactorUsed = userLatentFactor[ data['newUserID'] ,:]

            ps = (itemLatentFactorUsed + itemClassLatentFactorUsed)
            yforReg = targetUsed - (ps*(userLatentFactorUsed)).sum(1)

            reg = linear_model.Ridge(alpha=lamda)
            reg.fit(ps, yforReg)

            NewUserClassLatentFactor = pd.DataFrame(reg.coef_).T
            NewUserClassLatentFactor['userClass'] = indexOfFile

            with fileLock:
                with open('.\\updatedPQST\\lententFactor_of_userClass.csv','a') as f:
                        NewUserClassLatentFactor.to_csv(f,index=False,header=False)

            print("user class : ", indexOfFile, "'s latent factor is prepared! ")


        else:
            print("name :" + str(name) + "is finished !")
            break



if __name__ == "__main__":
    # data prepare
    os.chdir("C:\\Users\\22560\\Documents\\iptv")
    data = []

    # divided data by user class  and by item class
    trainSet = pd.read_csv("trainSet.csv",usecols = ['newUserID', 'name', 'MEDIACOUNT','userGroup','tag'])
    trainSet = trainSet.dropna(axis = 0)
    # encode user , item


    behavior,mediaid   = changeNameToID(trainSet,'name',plan='B')
    behavior,userid    = changeNameToID(behavior, 'newUserID', plan='B')
    behavior,userGroup = changeNameToID(behavior,'userGroup',plan='B')
    behavior,tag       = changeNameToID(behavior,'tag',plan='B')

    # saving original name

    save_pickle(mediaid, "mediaid.data")
    save_pickle(userid, "userid.data")
    save_pickle(userGroup,"userGroup.data")
    save_pickle(tag, "tag.data")



    print("encode is ready! starting to split data")

    for _,data in behavior.groupby('userGroup'):
        data.to_csv( ".\\dividedByUser\\"+str(data.userGroup.iloc[0])+'.csv',index=False)



    for _,data in behavior.groupby('tag'):
        data.to_csv( ".\\dividedByItem\\"+str(data.tag.iloc[0])+'.csv',index=False)


    print("data has splited ! ,starting to initailizing paras matrix ! ")

    # 初始化P,Q,S,T　对应的参数
    numOfUser      = load_pickle("userid.data").keys().__len__()
    numOfItem      = load_pickle("mediaid.data").keys().__len__()
    numOfUserClass = len(os.listdir(".\\dividedByUser"))
    numOfItemClass = len(os.listdir(".\\dividedByItem"))
    numOfK         = 7

    # generate data # 本步骤有待于后续开发
    userLatentFactor = randomInitalizer(numOfUser,numOfK)
    itemLatentFactor = randomInitalizer(numOfItem,numOfK)
    userClassLatentFactor = randomInitalizer(numOfUserClass,numOfK)
    itemClassLatentFactor = randomInitalizer(numOfItemClass,numOfK)


    userLatentFactor.to_csv(".\\oldPQST\\userLatentFactor.txt", sep='\t',index = False)
    itemLatentFactor.to_csv(".\\oldPQST\\itemLatentFactor.txt",sep = '\t',index = False)
    userClassLatentFactor.to_csv(".\\oldPQST\\userClassLatentFactor.txt",sep = '\t',index = False)
    itemClassLatentFactor.to_csv(".\\oldPQST\\itemClassLatentFactor.txt",sep = '\t',index = False)



    # 构建 user 部分 的 任务
    userTaskList = os.listdir(".\\dividedByUser")
    userTaskNum  = len(userTaskList)

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

    loopCount = -1
    while(True):
        processList = []
        loopCount += 1
        # loop untial usr latent factor converge to constant
        for i in range(4*loopCount,4*loopCount+4):
            process = multiprocessing.Process(
                        name=str(i),target=multiprocessForUser,
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

        if cretiria < 1e-5 :

            break


    while(True):



