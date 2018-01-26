import numpy as np
import pandas as pd
from sklearn.metrics import pairwise_distances
from scipy.sparse import  lil_matrix,csr_matrix


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
            itemLatentFactor,itemClassLatentFactor,userBelong,itemBelong,N,everyOneLike,
                rateList = (0.2,0.2,0.3,0.3)
                ):

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
        userHasViewedClass = np.unique(trainSet[trainSet['newUserID'] == userNum]['tag'].values)
        recommandByViewed = itemBelong[itemBelong['tag'].isin(userHasViewedClass)].groupby('tag').apply(
            lambda x: x.nlargest(N, 'userEvaluate').loc[:, ['userEvaluate']]).reset_index().rename(
            columns={"level_1": "TV_NAME"})
        recommandByViewed.sort_values(by='userEvaluate',inplace=True,ascending=False)
        recommandByViewed = recommandByViewed['TV_NAME'].values[:int(N *rateList[0])]
        #itemBelong.loc[itemBelong['TV_NAME'].isin(recommandByViewed),'userEvaluate'] = -100

        recommandBySys = itemBelong.nlargest(N,'userEvaluate')
        recommandBySys = recommandBySys['TV_NAME'].values[:int(N *rateList[1])]


        # 追加一部分大家都在看的内容（类似于排行榜的感觉）
        recommandByOther = everyOneLike[:int(N*rateList[2])]

        # 追加一部分和看过的相似的内容
        viewedItemFactor = itemLatentFactor[userHasViewed,:]+itemClassLatentFactor[itemBelong.loc[userHasViewed,'tag'],:]
        dis = pairwise_distances(viewedItemFactor,itemLatentFactor+itemClassLatentFactor[itemBelong['tag'],:])
        dis[range(len(userHasViewed)), userHasViewed] = 100
        rateOther =  rateList[0] + rateList[1] + rateList[2]
        recommandByItem = np.argsort(dis, axis=1).ravel('F')[:(N -int(N* rateOther))]

        recommandUnion = np.concatenate((recommandByViewed, recommandBySys, recommandByItem, recommandByOther))
        recommandUnion = np.unique(recommandUnion)
        if len(recommandUnion) != N:
            return np.concatenate((recommandUnion,everyOneLike[len(recommandUnion):N]))
        else:
            return recommandUnion

    else:
        recommandBySys = itemBelong.nlargest(N, 'userEvaluate')
        recommandBySys = np.array(recommandBySys['TV_NAME'].tolist())
        # 追加一部分大家都在看的内容（类似于排行榜的感觉）
        recommandByOther = everyOneLike[:int(N * rateList[2])]
        return np.concatenate((recommandBySys[:(N - int(N * rateList[2]))],recommandByOther))




def topNbyClassOfPay(trainSet,userNum,userLatentFactor,userClassLatentFactor,
            itemLatentFactor,itemClassLatentFactor,userBelong,itemBelong,N,everyOneLikePayed,
                     itemNeedPayList,  rateList = (0.2,0.2,0.3,0.3)
                ):

    # 获得指定用户对于6万个商品的评分
    itemBelong = itemBelong.copy()
    userFactor = userLatentFactor[userNum,:] + userClassLatentFactor[userBelong.loc[userNum,'userGroup'],:]
    itemFactor = itemLatentFactor +  itemClassLatentFactor[ itemBelong['tag'],:]

    itemBelong['userEvaluate'] = userFactor.dot(itemFactor.transpose())

    # 将itemBelong的结果进一步处理，只保留用户对付费内容的评分的预测
    # 这样能保证推荐的内容都是付费内容
    itemBelong.loc[-itemBelong['TV_NAME'].isin(itemNeedPayList),'userEvaluate'] = -1000

    # 从trainSet中抽取user的已经看过的：
    userHasViewed = trainSet[trainSet['newUserID'] == userNum]['TV_NAME'].values

    if len(userHasViewed) != 0:
        # 如果用户有观测记录
        #itemBelong.loc[userHasViewed,'userEvaluate'] = -100
        userHasViewedClass = np.unique(trainSet[trainSet['newUserID'] == userNum]['tag'].values)
        recommandByViewed = itemBelong[itemBelong['tag'].isin(userHasViewedClass)].groupby('tag').apply(
            lambda x: x.nlargest(N, 'userEvaluate').loc[:, ['userEvaluate']]).reset_index().rename(
            columns={"level_1": "TV_NAME"})
        recommandByViewed.sort_values(by='userEvaluate',inplace=True,ascending=False)
        recommandByViewed = recommandByViewed['TV_NAME'].values[:int(N *rateList[0])]
        #itemBelong.loc[itemBelong['TV_NAME'].isin(recommandByViewed),'userEvaluate'] = -100

        recommandBySys = itemBelong.nlargest(N,'userEvaluate')
        recommandBySys = recommandBySys['TV_NAME'].values[:int(N *rateList[1])]


        # 追加一部分大家都在看的内容（类似于排行榜的感觉）

        recommandByOther = everyOneLikePayed[:int(N*rateList[2])]

        # 追加一部分和看过的相似的内容
        viewedItemFactor = itemLatentFactor[userHasViewed,:]+itemClassLatentFactor[itemBelong.loc[userHasViewed,'tag'],:]
        dis = pairwise_distances(viewedItemFactor,itemLatentFactor+itemClassLatentFactor[itemBelong['tag'],:])
        dis[range(len(userHasViewed)), userHasViewed] = 100

        #将非付费节目的距离调高，使得算法不会选择这些非付费节目
        idx = np.ones((dis.shape[1],),bool)
        idx[itemNeedPayList] = 0
        dis[:,idx] = 100

        rateOther =  rateList[0] + rateList[1] + rateList[2]
        recommandByItem = np.argsort(dis, axis=1).ravel('F')[:int(N * (1-(rateOther)))]

        recommandUnion = np.concatenate((recommandByViewed, recommandBySys, recommandByItem, recommandByOther))
        recommandUnion = np.unique(recommandUnion)
        if len(recommandUnion) != N:
            return np.concatenate((recommandUnion,everyOneLikePayed[len(recommandUnion):N]))
        else:
            return recommandUnion
    else:
        recommandBySys = itemBelong.nlargest(N, 'userEvaluate')
        recommandBySys = np.array(recommandBySys['TV_NAME'].tolist())
        # 追加一部分大家都在看的内容（类似于排行榜的感觉）

        recommandByOther = everyOneLikePayed[:int(N * rateList[2])]

        return np.concatenate((recommandBySys[:(N - int(N * rateList[2]))], recommandByOther))






def evaluation(userList,numOfUser,numOfItem,trainSet,test,
                userLatentFactor,userClassLatentFactor,
               itemLatentFactor,itemClassLatentFactor,
                userBelong,itemBelong,N,everyOneLike,rateList = (0.2,0.2,0.4)
               ):

    # 考虑覆盖率和召回率
    # 对每个在测试集合出现的user，做推荐


    recMatrix =  lil_matrix((numOfUser,numOfItem))
    for user in userList:
        rec = topNbyClass(trainSet,user,userLatentFactor,userClassLatentFactor,
            itemLatentFactor,itemClassLatentFactor,userBelong,itemBelong,N,everyOneLike,
                rateList)
        recMatrix[user,rec] = 1
        if user % 10 == 0:
            print("rec for user : ",user," is done!")

    recMatrix = recMatrix[userList,:]

    # 构建test 的稀疏矩阵

    testMatrix = csr_matrix((np.ones(test.shape[0]),
                             (test['newUserID'],test['TV_NAME'])
                             ),shape=(numOfUser,numOfItem))

    testMatrix = testMatrix[userList,:]

    recall = recMatrix.multiply( testMatrix).sum()/testMatrix.sum()
    precision  = recMatrix.multiply(testMatrix).sum() /recMatrix.sum()
    cover  = (recMatrix.sum(0) > 0 ).sum()/numOfItem


    return (recall,precision,cover)









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



