import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from sklearn.cluster import  KMeans
import pandas as pd
import numpy as np
import os

def userGroupToitemGroupHist(trainSet,userGroupName, itemGroupName,targetName
                             ):

    userGroupToitemGroupView = trainSet.groupby([itemGroupName],as_index=False).agg({targetName:
                                                         lambda x: np.sum(np.exp(x))
                                                         }).rename(columns={targetName:"viewSum"})

    sns.set_style("darkgrid")
    NumOfUser = len(trainSet[userGroupName].unique())
    plt.figure()
    for  i in range(1,9):
        plt.subplot(2, 4, i)
        #flag = userGroupToitemGroupView[userGroupName] == i-1
        sns.barplot(x = 'tag',y='viewSum',data=userGroupToitemGroupView)


def itemClustering(itemLatentFactor,
                   itemClassLatentFactor,itemBelong,itemGroupName,itemid
                   ):
    itemFactor =  itemClassLatentFactor[itemBelong[itemGroupName],:] +itemLatentFactor
    pca = PCA(n_components=2)
    pca.fit(itemFactor)
    itemFactorPCA = pca.transform(itemFactor)
    #sns.regplot(x=itemFactorForPlot[:,0], y=itemFactorForPlot[:,1], fit_reg=False)
    kms = KMeans(n_clusters=10, random_state=0).fit(itemFactor)
    kms.labels_
    itemBelong[kms.labels_ == 7]['name'].map(itemid).tolist()


def itemClusteringInGroup(
    itemLatentFactor,itemClassLatentFactor,itemBelong,groupNum,itemGroupName
):
    itemIngroup = itemLatentFactor[itemBelong[itemGroupName]==groupNum,:] + itemClassLatentFactor[groupNum,:]
    tsne = TSNE()
    tsne.fit_transform(itemIngroup)
    #sns.regplot(x=tsne.embedding_[:,0], y=tsne.embedding_[:, 1], fit_reg=False)
    kms = KMeans(n_clusters=30, random_state=0).fit(tsne.embedding_)
    kms.labels_
    itemBelong[itemBelong['tag'] == groupNum][kms.labels_ == 24]['name'].map(itemid).tolist()




if __name__ == "__main__":
    os.chdir("C:\\Users\\22560\\Documents\\iptv")
    behavior = pd.read_csv("./originalData/behavior.csv")
    userGroupName = "userGroup"
    itemGroupName = "tag"
    userName = "newUserID"
    itemName = "name"
    targetName = "MEDIACOUNT"

    userLatentFactor = pd.read_csv(".\\oldPQST\\userLatentFactor.txt", sep='\t').values
    userClassLatentFactor = pd.read_csv(".\\oldPQST\\userClassLatentFactor.txt", sep='\t').values
    itemLatentFactor = pd.read_csv(".\\oldPQST\\itemLatentFactor.txt", sep='\t').values
    itemClassLatentFactor = pd.read_csv(".\\oldPQST\\itemClassLatentFactor.txt", sep='\t').values


    userBelong  = pd.read_csv("./temp/userBelong.csv")
    itemBelong  = pd.read_csv("./temp/itemBelong.csv")



