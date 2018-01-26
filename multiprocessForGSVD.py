import multiprocessing
from sklearn import linear_model
import shutil
from usefulTool import clearDir,changeNameToID
import pandas as pd
import gc
import scipy.sparse as sparse

def multiprocessForUser(queue,fileLock,taskList,csvPlace,lamda,
                        userName, itemName, targetName, userGroupName, itemGroupName
                        ,penalty
                        ):
    #
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

            # indexOfFile equals to indexOfLatent
            indexOfFile =queue.get()

            print("name : " + str(name) + "  is doing " + str(indexOfFile)+ "  userClass")
            # 如果拿到的indexOfFile是第一个，那么就清空output.txt文件
            if indexOfFile == 0 :
                rootdir  = "C:\\Users\\22560\\Documents\\iptv\\updatedPQST\\"
                clearDir(rootdir)



            # loading latent info
            itemClassLatentFactor = pd.read_table(".\\oldPQST\\itemClassLatentFactor.txt").values
            userClassLatentFactor = pd.read_table(".\\oldPQST\\userClassLatentFactor.txt").values
            userLatentFactor = pd.read_table(".\\oldPQST\\userLatentFactor.txt").values
            itemLatentFactor = pd.read_table(".\\oldPQST\\itemLatentFactor.txt").values

            ##### extract info for user :

            # genertate data need
            data = pd.read_csv(csvPlace + taskList[indexOfFile])
            data  = data.sort_values(by = userName).reset_index(drop = True).reset_index()
            userClassLatentFactorUsed = userClassLatentFactor[indexOfFile, :]
            ps      = (itemLatentFactor[data[itemName],:] +\
                                itemClassLatentFactor[data[itemGroupName],:])
            yForReg = (data[targetName]- ps.dot(userClassLatentFactorUsed)).values

            def lassoRegres(x):
                reg = linear_model.Lasso(alpha=1, fit_intercept=False,warm_start=True,)
                reg.fit(ps[x['index'], :], yForReg[x['index']])
                return reg.coef_

            def ridgeRegres(x):
                reg = linear_model.Ridge(alpha=lamda,fit_intercept=False)
                reg.fit(ps[x['index'] ,:],yForReg[x['index']])
                return reg.coef_

            if penalty == 'l2':
                userLentFacorForUserList  = data.groupby('newUserID').apply(ridgeRegres)
            elif penalty == 'l1':
                userLentFacorForUserList = data.groupby('newUserID').apply(lassoRegres)
            else :
                userLentFacorForUserList = None
                assert  Exception
            print("latent facotr for user in group  ",indexOfFile," is prepared !")

            userLentFacorForUserList = pd.DataFrame(userLentFacorForUserList.values.tolist(),index=userLentFacorForUserList.index)

            userLentFacorForUserList['userList'] = userLentFacorForUserList.index

            with fileLock:
                print(" is writting ", "class " , indexOfFile," data now")
                print(userLentFacorForUserList.shape)
                with open('.\\updatedPQST\\lententFactor_of_user_in_class.csv','a') as f:
                    userLentFacorForUserList.to_csv(f,index=False,header=False)

            gc.collect()
            # output the result and release the data

            ##### extract info for userClass :
            print("user in class : ",indexOfFile," latent factor is prepared! ")

            targetUsed = data[targetName]
            itemLatentFactorUsed = itemLatentFactor[data[itemName], :]
            itemClassLatentFactorUsed = itemClassLatentFactor[data[itemGroupName], :]
            userLatentFactorUsed = userLatentFactor[ data[userName] ,:]

            ps = (itemLatentFactorUsed + itemClassLatentFactorUsed)
            yforReg = targetUsed - (ps*(userLatentFactorUsed)).sum(1)

            if penalty == 'l2':
                reg = linear_model.Ridge(alpha=lamda,fit_intercept=False)
            elif penalty == 'l1':
                reg = linear_model.Lasso(alpha=1, fit_intercept=False, warm_start=True, )

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



def multiprocessForItem(queue,fileLock,taskList,csvPlace,lamda,
                        userName, itemName, targetName, userGroupName, itemGroupName
                        ,penalty
                        ):

    # queue = itemQueue
    # filelock = itemFileLock
    # taskList = itemTaskList
    # csvPlace = itemCsvPlace
    # fileLock = itemFileLock

    name = multiprocessing.current_process().name

    while (True):
        if not queue.empty():
            # remove all the files in the updataPQST
            # get the indexOffile

            # indexOfFile equals to indexOfLatent
            indexOfFile = queue.get()

            print("name : " + str(name) + "  is doing " + str(indexOfFile) + " 　itemClass")
            # 如果拿到的indexOfFile是第一个，那么就清空output.txt文件
            if indexOfFile == 0:
                rootdir = "C:\\Users\\22560\\Documents\\iptv\\updatedPQST\\"
                clearDir(rootdir)

            # loading latent info
            itemClassLatentFactor = pd.read_table(".\\oldPQST\\itemClassLatentFactor.txt").values
            userClassLatentFactor = pd.read_table(".\\oldPQST\\userClassLatentFactor.txt").values
            userLatentFactor = pd.read_table(".\\oldPQST\\userLatentFactor.txt").values
            itemLatentFactor = pd.read_table(".\\oldPQST\\itemLatentFactor.txt").values

            ##### extract info for user :

            # genertate data need
            data = pd.read_csv(csvPlace + taskList[indexOfFile])
            data = data.sort_values(by=itemName).reset_index(drop=True).reset_index()

            itemClassLatentFactorUsed = itemClassLatentFactor[indexOfFile, :]
            ps = (userLatentFactor[data[userName], :] +
                  userClassLatentFactor[data[userGroupName], :])
            yForReg = (data[targetName] - ps.dot(itemClassLatentFactorUsed)).values

            def lassoRegres(x):
                reg = linear_model.Lasso(alpha=1, fit_intercept=False, warm_start=True, )
                reg.fit(ps[x['index'], :], yForReg[x['index']])
                return reg.coef_

            def ridgeRegres(x):
                reg = linear_model.Ridge(alpha=lamda, fit_intercept=False)
                reg.fit(ps[x['index'], :], yForReg[x['index']])
                return reg.coef_

            if penalty == 'l2':
                itemLentFacorForUserList = data.groupby(itemName).apply(ridgeRegres)
            elif penalty == 'l1':
                itemLentFacorForUserList = data.groupby(itemName).apply(lassoRegres)
            else:
                itemLentFacorForUserList = None
                assert Exception
            print("latent facotr for item in group  ", indexOfFile, " is prepared !")

            itemLentFacorForUserList = pd.DataFrame(itemLentFacorForUserList.values.tolist(),
                                                    index=itemLentFacorForUserList.index)

            itemLentFacorForUserList['itemList'] = itemLentFacorForUserList.index

            with fileLock:
                print(itemLentFacorForUserList.shape)
                with open('.\\updatedPQST\\lententFactor_of_item_in_class.csv', 'a') as f:
                    itemLentFacorForUserList.to_csv(f, index=False, header=False)

            gc.collect()
            # output the result and release the data

            ##### extract info for userClass :
            print("item in class : ", indexOfFile, " latent factor is prepared! ")

            targetUsed = data[targetName]
            userLatentFactorUsed = userLatentFactor[data[userName], :]
            userClassLatentFactorUsed = userClassLatentFactor[data[userGroupName], :]
            itemLatentFactorUsed = itemLatentFactor[data[itemName], :]

            ps = (userLatentFactorUsed + userClassLatentFactorUsed)
            yforReg = targetUsed - (ps * (itemLatentFactorUsed)).sum(1)

            if penalty == 'l2':
                reg = linear_model.Ridge(alpha=lamda,fit_intercept=False)
            elif penalty == 'l1':
                reg = linear_model.Lasso(alpha=1, fit_intercept=False, warm_start=True, )

            reg.fit(ps, yforReg)

            NewItemClassLatentFactor = pd.DataFrame(reg.coef_).T
            NewItemClassLatentFactor['itemClass'] = indexOfFile

            with fileLock:
                with open('.\\updatedPQST\\lententFactor_of_itemClass.csv', 'a') as f:
                    NewItemClassLatentFactor.to_csv(f, index=False, header=False)

            print("item    class : ", indexOfFile, "'s latent factor is prepared! ")


        else:
            print("name :" + str(name) + "is finished !")
            break
