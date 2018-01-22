import numpy as np
import pandas as pd
from sklearn.metrics import pairwise_distances

def userGroupToitemGroupPreference(
        userClassLatentFactor,itemClassLatentFactor,itemTag,userTag

):

    groupPreference = userClassLatentFactor.dot(itemClassLatentFactor.transpose())
    userMode = pd.DataFrame(np.argsort(-groupPreference,axis = 1))
    userMode = userMode.applymap(lambda  x : itemTag[x])
    userMode.rename(index = userTag,inplace = True )
    userMode = userMode.add_prefix("itemType")

    itemMode = pd.DataFrame(np.argsort(-groupPreference, axis=0))
    itemMode =  itemMode.applymap(lambda  x: userTag[x])
    itemMode.rename(columns=itemTag,inplace=True)
    return userMode,itemMode






def userToitemGroupPreference(
        userLatentFactor,userClassLatentFactor,itemClassLatentFactor,itemTag,userBelong

):

    groupPreference =( userLatentFactor + userClassLatentFactor[userBelong['userGroup'],:]).dot(itemClassLatentFactor.transpose())
    userMode = pd.DataFrame(np.argsort(-groupPreference,axis = 1))
    userMode = userMode.applymap(lambda  x : itemTag[x])
    userMode = userMode.add_prefix("itemType")
    userDoesnotLikeTv = UserDoesnotLikeTV(userLatentFactor)
    userMode.loc[userDoesnotLikeTv, :] = userMode.loc[userDoesnotLikeTv,:].applymap(lambda x:"notLikeTV")
    return userMode





def userToitemPreference(
        userLatentFactor,userClassLatentFactor,
        itemLatentFactor,itemClassLatentFactor,
        itemid,userTag,userNum,userBelong,itemBelong,itemTag

):

    groupPreference = (userLatentFactor[userNum,:]+ userClassLatentFactor[userBelong.loc[9,'userGroup'],:] ).dot(
        (itemLatentFactor+itemClassLatentFactor[itemBelong['tag'],:]).transpose())
    userMode = pd.Series(np.argsort(-groupPreference))
    userCata = itemBelong.loc[userMode]['tag']
    userCata = userCata.map(lambda x: itemTag[x]).reset_index(drop=True)
    userMode = userMode.map(lambda  x : itemid[x]).reset_index(drop=True)
    # userMode.rename(index = userTag,inplace = True )
    # userMode = userMode.add_prefix("itemName")
    return pd.DataFrame({"usrMode":userMode,"userCata":userCata})




def UserDoesnotLikeTV(userLatentFactor):
    userList = np.array(list(range(userLatentFactor.shape[0])))
    userDoesnotLikeTv = userList[userLatentFactor.sum(1)==0]
    return userDoesnotLikeTv


############ fixed bugs ############
def topNRec(n,userLatentFactor,userClassLatentFactor,
            itemLatentFactor,itemClassLatentFactor,userBelong,itemBelong
            ,blockNum
            ):
    datas = np.split(
        (userLatentFactor + userClassLatentFactor[userBelong['userGroup'],:] ),
             [int(userLatentFactor.shape[0] * i / blockNum)
                for i in
                    list(
                        range(1, blockNum, 2)
                    )
              ]

             )
    userEvaluateList = []
    for data in datas:
        userEvaluate = data.dot(
            (itemLatentFactor + itemClassLatentFactor[itemBelong['tag'],:] ).transpose())
        userEvaluate = np.argsort(-userEvaluate)[:,:n]
        userEvaluateList.append(userEvaluate)
        del userEvaluate
    return np.vstack(userEvaluateList)



def topNbyClass(trainSet,userNum,userLatentFactor,userClassLatentFactor,
            itemLatentFactor,itemClassLatentFactor,userBelong,itemBelong,N):

    # 获得指定用户对于6万个商品的评分
    itemBelong = itemBelong.copy()
    userFactor = userLatentFactor[userNum,:] + userClassLatentFactor[userBelong.loc[userNum,'userGroup'],:]
    itemFactor = itemLatentFactor +  itemClassLatentFactor[ itemBelong['tag'],:]

    itemBelong['userEvaluate'] = userFactor.dot(itemFactor.transpose())
    # 从trainSet中抽取user的已经看过的：
    userHasViewed = trainSet[trainSet['newUserID'] == userNum]['TV_NAME'].values

    if len(userHasViewed) != 0:
        # 如果用户有观测记录
        #itemBelong.loc[userHasViewed,'userEvaluate'] = -100
        userHasViewedClass = trainSet[trainSet['newUserID'] == userNum]['tag'].values
        recommandByViewed = itemBelong[itemBelong['tag'].isin(userHasViewedClass)].groupby('tag').apply(
            lambda x: x.nlargest(N, 'userEvaluate').loc[:, ['userEvaluate']]).reset_index().rename(
            columns={"level_1": "TV_NAME"})
        recommandByViewed.sort_values(by='userEvaluate',inplace=True,ascending=False)
        recommandByViewed = recommandByViewed['TV_NAME'].values[:int(N / 5)]
        #itemBelong.loc[itemBelong['TV_NAME'].isin(recommandByViewed),'userEvaluate'] = -100

        recommandBySys = itemBelong.nlargest(N,'userEvaluate')
        recommandBySys = recommandBySys['TV_NAME'].values[:int(N / 5)]

        # 追加一部分和看过的相似的内容
        viewedItemFactor = itemLatentFactor[userHasViewed,:]+itemClassLatentFactor[itemBelong.loc[userHasViewed,'tag'],:]
        dis = pairwise_distances(viewedItemFactor,itemLatentFactor+itemClassLatentFactor[itemBelong['tag'],:])
        recommandByItem = np.argsort(dis, axis=1).ravel('F')[:(N-int(2*N/5))]

        return np.concatenate((recommandByViewed,recommandBySys,recommandByItem))
    else:
        recommandBySys = itemBelong.nlargest(N, 'userEvaluate')
        return np.array(recommandBySys['TV_NAME'].tolist())


def recallRate(testSet,userList,userRec):
    # 我预测的他看的比重
    pass




def precision():
    # 他看的占我预测的比重
    pass





def predictTarget(testSet,
                  userLatentFactor,itemLatentFactor,
                  userClassLatentFactor,itemClassLatentFactor,
                  userName,itemName,userGroupName,itemGroupName
                  ,userNum = -1
                  ):
    if userNum == -1:
        #对全体数据做预测
        #testSet = behavior
        predict =(
            (
                userLatentFactor[testSet[userName],:] +\
                userClassLatentFactor[testSet[userGroupName],:]
             )*
            (
                itemLatentFactor[testSet[itemName], :] +\
                itemClassLatentFactor[testSet[itemGroupName], :]
            )).sum(1)
        return predict


def RMSE(behavior,
         userLatentFactor, itemLatentFactor,
         userClassLatentFactor, itemClassLatentFactor,
         userName, itemName, userGroupName, itemGroupName
         , userNum=-1):
    predict = predictTarget(behavior,
                            userLatentFactor, itemLatentFactor,
                            userClassLatentFactor, itemClassLatentFactor,
                            userName, itemName, userGroupName, itemGroupName
                            , userNum=-1
                            )

    return ((np.exp(predict) - np.exp(behavior['MEDIACOUNT'])) ** 2).sum() / behavior.shape[0]



