import multiprocessing
from sklearn import linear_model
import shutil
from usefulTool import clearDir,changeNameToID
import pandas as pd
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



def multiprocessForItem(queue,fileLock,taskList,csvPlace,lamda):

    # queue = itemQueue
    # filelock = itemFileLock
    # taskList = itemTaskList
    # csvPlace = itemCsvPlace
    # fileLock = itemFileLock


    name  = multiprocessing.current_process().name

    while(True):
        if not queue.empty():
            # remove all the files in the updataPQST
            # get the indexOffile
            print("name :" + str(name) + "  is running !")
            # indexOfFile equals to indexOfLatent
            indexOfFile =queue.get()

            print("name : " + str(name) + "  is doing " + str(indexOfFile)+ "  itemClass")
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
            itemLentFacorForItemList = []
            counter = 0
            for itemNowDealing in itemList:
                counter +=1
                if counter%3000 == 0:
                    print('name ',name,' :  we have finished  :',counter  / len(itemList), 'percent of ', indexOfFile,' item class ')
                dataUsed    = data[data['name'] == itemNowDealing]
                targetUsed     = dataUsed['MEDIACOUNT']
                userLatentFactorUsed      = userLatentFactor[ dataUsed['newUserID'],:]
                userClassLatentFactorUsed = userClassLatentFactor[ dataUsed['userGroup'],:  ]

                # attention : cause of user is in one group, so we can just extract one row form useClsLntfctr
                itemClassLatentFactorUsed = itemClassLatentFactor[ indexOfFile,:  ]

                # record p+s & y-(p+s)*t
                ps = userClassLatentFactorUsed + userLatentFactorUsed
                yforReg  = targetUsed - ps.dot(itemClassLatentFactorUsed)
                reg = linear_model.Ridge(alpha=lamda)
                reg.fit(ps,yforReg)
                itemLentFacorForItemList.append(reg.coef_)

            itemLentFacorForItemList = pd.DataFrame(itemLentFacorForItemList)
            itemLentFacorForItemList['itemList'] = itemList
            with fileLock:
                print(" is writting ", "class " , indexOfFile," data now")
                print(itemLentFacorForItemList.shape)
                with open('.\\updatedPQST\\lententFactor_of_item_in_class.csv','a') as f:
                    itemLentFacorForItemList.to_csv(f,index=False,header=False)


            del itemLentFacorForItemList
            gc.collect()
            # output the result and release the data

            ##### extract info for userClass :
            print("user in class : ",indexOfFile," latent factor is prepared! ")

            targetUsed = data['MEDIACOUNT']
            userLatentFactorUsed = userLatentFactor[data['newUserID'], :]
            userClassLatentFactorUsed = userClassLatentFactor[data['userGroup'], :]
            itemLatentFactorUsed = itemLatentFactor[ data['name'] ,:]

            ps = (userLatentFactorUsed + userClassLatentFactorUsed)
            yforReg = targetUsed - (ps*(itemLatentFactorUsed)).sum(1)

            reg = linear_model.Ridge(alpha=lamda)
            reg.fit(ps, yforReg)

            NewItemClassLatentFactor = pd.DataFrame(reg.coef_).T
            NewItemClassLatentFactor['itemClass'] = indexOfFile

            with fileLock:
                with open('.\\updatedPQST\\lententFactor_of_itemClass.csv','a') as f:
                        NewItemClassLatentFactor.to_csv(f,index=False,header=False)

            print("item class : ", indexOfFile, "'s latent factor is prepared! ")

        else:
            print("name :" + str(name) + "is finished !")
            break
