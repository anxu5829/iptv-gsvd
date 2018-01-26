import os
import  pandas as pd
from sltools import loadDictInversed,load_pickle
from evaluate import\
    userGroupToitemGroupPreference,userToitemGroupPreference,\
    userToitemPreference,topNRec,predictTarget,RMSE,topNbyClass,evaluation,topNbyClassOfPay
from usefulTool import invertDict,userRecord
import numpy as np
import seaborn as sns
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from sklearn.cluster import  KMeans
from scipy.sparse import  lil_matrix,csr_matrix
if __name__ == "__main__":

    os.chdir("C:\\Users\\22560\\Documents\\iptv")
    userLatentFactor = pd.read_csv(".\\oldPQST\\userLatentFactor.txt", sep='\t').values
    userClassLatentFactor = pd.read_csv(".\\oldPQST\\userClassLatentFactor.txt", sep='\t').values
    itemLatentFactor = pd.read_csv(".\\oldPQST\\itemLatentFactor.txt", sep='\t').values
    itemClassLatentFactor = pd.read_csv(".\\oldPQST\\itemClassLatentFactor.txt", sep='\t').values

    userGroupName = "userGroup"
    itemGroupName = "tag"
    userName = "newUserID"
    itemName = "TV_NAME"
    targetName = "MEDIACOUNT"


    numOfUser      = load_pickle("./temp/userid.data").keys().__len__()
    numOfItem      = load_pickle("./temp/mediaid.data").keys().__len__()
    numOfUserClass = len(os.listdir(".\\dividedByUser"))
    numOfItemClass = len(os.listdir(".\\dividedByItem"))
    numOfK         = 5


    userdict = load_pickle("./temp/userdict.data")
    itemdict = load_pickle("./temp/itemdict.data")
    itemTag  = loadDictInversed("./temp/tag.data")
    userTag  = loadDictInversed("./temp/userGroup.data")
    userid   = loadDictInversed("./temp/userid.data")
    itemid   = loadDictInversed("./temp/mediaid.data")

    behavior = pd.read_csv("./originalData/behavior.csv")
    test = pd.read_csv("./originalData/test.csv")

    userBelong  = pd.read_csv("./temp/userBelong.csv")
    itemBelong  = pd.read_csv("./temp/itemBelong.csv")

    #
    #
    # # 用户对于各个类别的喜好情况
    userGroupMode , itemGroupMode = userGroupToitemGroupPreference(
        userClassLatentFactor, itemClassLatentFactor, itemTag, userTag
    )
    #
    # # 具体用户对于各个类别喜好
    # userLike = userToitemGroupPreference(
    #     userLatentFactor, userClassLatentFactor, itemClassLatentFactor, itemTag, userBelong
    # )
    #
    #
    # userNum = 4
    # userToitemPreference(
    #     userLatentFactor, userClassLatentFactor,
    #     itemLatentFactor, itemClassLatentFactor,
    #     itemid, userTag, userNum, userBelong, itemBelong,itemTag
    #
    # )
    #
    # userNum =4
    # userRecord(behavior,itemid,itemTag,userNum )
    #
    #
    # rec = topNbyClass(behavior, userNum, userLatentFactor, userClassLatentFactor,
    #             itemLatentFactor, itemClassLatentFactor, userBelong, itemBelong, N=100)
    #
    # rec = [ itemid[i] for i in rec]
    # rec
    #
    #
    # test['predict'] = predictTarget(test,
    #                         userLatentFactor, itemLatentFactor,
    #                         userClassLatentFactor, itemClassLatentFactor,
    #                         userName, itemName, userGroupName, itemGroupName
    #                         , userNum=-1
    #                         )
    #
    #
    RMSE(behavior,
         userLatentFactor, itemLatentFactor,
         userClassLatentFactor, itemClassLatentFactor,
         userName, itemName, userGroupName, itemGroupName
         , userNum=-1)



    RMSE(test,
         userLatentFactor, itemLatentFactor,
         userClassLatentFactor, itemClassLatentFactor,
         userName, itemName, userGroupName, itemGroupName
         , userNum=-1)





    test['pre'] = predictTarget(test,userLatentFactor,itemLatentFactor,userClassLatentFactor,itemClassLatentFactor,userName,itemName
                  ,userGroupName,itemGroupName
                  )



    test.head()
    test_in_behavior = test[test.newUserID.isin(behavior.newUserID.unique())]
    np.sum((np.exp(test_in_behavior['MEDIACOUNT']) - np.exp(test_in_behavior['pre']))**2)/test_in_behavior.shape[0]

    test = test_in_behavior

    test['pre'] = predictTarget(test,userLatentFactor,itemLatentFactor,userClassLatentFactor,itemClassLatentFactor,userName,itemName
                  ,userGroupName,itemGroupName
                  )

    test['realTime'] = np.exp(test['MEDIACOUNT'])
    test['predictTime'] = np.exp(test['pre'])

    test = pd.merge(test,test.groupby('TV_NAME')[["realTime"]].max().rename(columns ={"realTime":"timeOfMv"})
             ,right_index=True,left_on='TV_NAME')

    test.loc[test['predictTime'] > test['timeOfMv'], 'predictTime'] = test['timeOfMv'][
        test['predictTime'] > test['timeOfMv']]

    ((test['realTime'] - test['predictTime']) ** 2).mean()

    realLike = test['realTime']/test['timeOfMv']
    preLike  = test['predictTime']/test['timeOfMv']
    preLike[preLike>1] = 1
    np.exp(-((realLike - preLike)**2)).mean()

    #

    #
    #
    #
    #
    #
    #
    #
    # #
    # #
    #
    # anima = itemLatentFactor[itemBelong.loc[itemBelong['tag'] == 4,'TV_NAME'],:] + itemClassLatentFactor[4,:]
    # pca = PCA(n_components=2)
    # pca.fit(anima)
    # itemFactorPCA = pca.transform(anima)
    # sns.regplot(x=itemFactorPCA[:,0], y=itemFactorPCA[:,1], fit_reg=False)

    #
    #
    #
    #
    userList = range(numOfUser)
    userList = range(10000)
    everyOneLike = behavior.groupby('TV_NAME').agg({"newUserID": "count"})
    everyOneLike = everyOneLike.sort_values(by='newUserID', ascending=False).index.tolist()

    for p in [0.01]:
        x,y,z = evaluation(userList,numOfUser,numOfItem,behavior,test,
                userLatentFactor,userClassLatentFactor,
               itemLatentFactor,itemClassLatentFactor,
                userBelong,itemBelong,N=10,everyOneLike = everyOneLike,rateList = (p,p,1-4*p,2*p)
               )
        print(x,y,z)


    # # 拿到原始信息
    userHasPayHistory = load_pickle("./temp/userHasPayHistory.data")
    itemNeedPay = load_pickle("./temp/itemNeedPay.data")
    mediaid = load_pickle("./temp/mediaid.data")
    userid  = load_pickle("./temp/userid.data")

    itemNeedPayList = [ j for i,j in mediaid.items() if i in itemNeedPay.tolist()]
    userid = pd.DataFrame({"id":[ i for  i , j in userid.items()],
                  "name":[ j for  i, j in userid.items()]})
    userid['original'] = userid['id'].str.replace(r'Day|Aftn|Nght|Mdnt',"")
    userHasPayHistory = userid.loc[userid['original'].isin(userHasPayHistory),'name'].tolist()



    # 已经获得userHasPayHistory，itemNeedPayList

    # 为那些曾经付过费的用户在test中有观测的进行推荐

    userList = test.loc[test.newUserID.isin(userHasPayHistory),'newUserID'].unique()
    everyOneLike = behavior.groupby('TV_NAME').agg({"newUserID": "count"})
    everyOneLike = everyOneLike.sort_values(by='newUserID', ascending=False).index.tolist()
    everyOneLikePayed = [i for i in everyOneLike if i in itemNeedPayList]

    recMatrix = lil_matrix((numOfUser, numOfItem))
    counter = 0
    for user in userList :

        N = test.loc[test['newUserID'] == user,'TV_NAME'].tolist().__len__()

        rateList = (0.2, 0.2, 0.3, 0.3)
        rec = topNbyClassOfPay(behavior,user,userLatentFactor,userClassLatentFactor,
            itemLatentFactor,itemClassLatentFactor,userBelong,itemBelong,N,everyOneLikePayed,
                     itemNeedPayList,  rateList = rateList)
        recMatrix[user, rec] = 1
        counter += 1
        if counter % 10 == 0:
            print("counter is ",counter)


    recMatrix = recMatrix[userList,:]
    #recMatrix = recMatrix[:,itemNeedPayList]


    testMatrix = csr_matrix((np.ones(test.shape[0]),
                             (test['newUserID'],test['TV_NAME'])
                             ),shape=(numOfUser,numOfItem))

    testMatrix = testMatrix[userList,:]



    tstPay = testMatrix[:,itemNeedPayList]
    recPay = recMatrix[:,itemNeedPayList]



    #recMatrix[:,]



    userfactor = userLatentFactor[userList,:]+\
        userClassLatentFactor[userBelong.reindex(userList).userGroup,:]

    itemfactor = itemLatentFactor[itemNeedPayList,:]+ \
        itemClassLatentFactor[itemBelong.reindex(itemNeedPayList).tag, :]

    userToPayItem = userfactor.dot(itemfactor.transpose())










    userToPayItem = (userToPayItem-userToPayItem.min(1).reshape((-1,1)))/(userToPayItem.max(1) - userToPayItem.min(1)).reshape((-1,1))
    # 二值化
    userToPayItemMedian = np.median(userToPayItem,axis=1).reshape((-1,1))
    userToPayItem = userToPayItem > 0.5



    testMatrix = csr_matrix((np.ones(test.shape[0]),
                             (test['newUserID'],test['TV_NAME'])
                             ),shape=(numOfUser,numOfItem))

    testMatrix = testMatrix[userList,:]
    testMatrix = testMatrix[:,itemNeedPayList]
    testMatrix = testMatrix.toarray()

    (testMatrix *userToPayItem).sum()



    (recMatrix.toarray() * testMatrix).sum()





    userpayFactor = userLatentFactor[userHasPayHistory,:]+userClassLatentFactor[userBelong.values[userHasPayHistory,1],:]
    itempayFactor = itemLatentFactor[itemNeedPayList, :] + itemClassLatentFactor[
                                                             itemBelong.values[itemNeedPayList, 1], :]
    user_itemE = userpayFactor.dot(itempayFactor.transpose())
    user_itemE = (user_itemE-user_itemE.min(1).reshape((-1,1)))/(user_itemE.max(1).reshape((-1,1))-user_itemE.min(1).reshape((-1,1)))
    data = csr_matrix((np.exp(test[targetName]), (test[userName], test[itemName])),shape=(numOfUser,numOfItem))
    data = data[userHasPayHistory,:][:,itemNeedPayList]
    data = data.toarray()
    data[data>0] = 1
    (data * user_itemE).sum()/data.sum()



    data = csr_matrix((np.exp(behavior[targetName]), (behavior[userName], behavior[itemName])), shape=(numOfUser, numOfItem))
    data = data[userHasPayHistory, :][:, itemNeedPayList]
    data = data.toarray()
    data[data > 0] = 1
    (data * user_itemE).sum() / data.sum()
























