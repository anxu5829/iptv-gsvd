import numpy as np
import pandas as pd
from scipy.sparse.linalg import svds
from scipy.sparse import csr_matrix


def randomInitalizer(rows,k):
    return(pd.DataFrame(np.random.random((rows,k))))






def svdInitalizerForObj(trainSet,userName,itemName,targetName
                        ,numOfUser,numOfItem,numOfK):
    train = csr_matrix((trainSet[targetName],
                (trainSet[userName],trainSet[itemName])),shape = (numOfUser,numOfItem))

    userLatentFactor, itemLatentFactor =   decomposed_rating_matrix(train,k = numOfK)
    return (pd.DataFrame(userLatentFactor) , pd.DataFrame(itemLatentFactor))


def initailizerBySVD(behavior, targetName, numOfUser, numOfItem, numOfK,
                          userBelong,itemBelong,
                          userName,userGroupName,
                          itemName,itemGroupName
                          ):
    userLatentFactor, itemLatentFactor = svdInitalizerForObj(
        behavior, userName, itemName, targetName, numOfUser, numOfItem, numOfK
    )


    userClassLatentFactor = userBelong.groupby(userGroupName).apply(
        lambda x: userLatentFactor.values[x[userName].values,:].mean(0)
    )
    userClassLatentFactor = pd.DataFrame(userClassLatentFactor.values.tolist())

    itemClassLatentFactor = itemBelong.groupby(itemGroupName).apply(
        lambda x: itemLatentFactor.values[x[itemName].values, :].mean(0)
    )
    itemClassLatentFactor = pd.DataFrame(itemClassLatentFactor.values.tolist())


    userLatentFactor = pd.DataFrame(userLatentFactor.values -\
                userClassLatentFactor.values[userBelong[userGroupName],:])

    itemLatentFactor = pd.DataFrame(itemLatentFactor.values -\
                itemClassLatentFactor.values[itemBelong[itemGroupName],:])




    return userLatentFactor,itemLatentFactor,userClassLatentFactor,itemClassLatentFactor





def svdInitalizerForGroup(trainSet,userName,itemName,targetName,userGroupName,itemGroupName
                        ,numOfItemClass,numOfUserClass,numOfK):


    train = trainSet.groupby([userGroupName,itemGroupName],as_index = False).agg({targetName:lambda x: np.log(np.sum(np.exp(x)))})

    train = csr_matrix((train[targetName],
                        (train[userGroupName], train[itemGroupName])), shape=(numOfUserClass, numOfItemClass))

    userClassLatentFactor, itemClassLatentFactor =   decomposed_rating_matrix(train,k = numOfK)

    return (pd.DataFrame(userClassLatentFactor) , pd.DataFrame(itemClassLatentFactor))


def decomposed_rating_matrix(sparse_matrix, k):
    u, s, vt = svds(sparse_matrix, k)
    return u.dot(np.diag(s)), vt.transpose()





