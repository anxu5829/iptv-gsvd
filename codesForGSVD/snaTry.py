# -*- coding:utf-8 -*-
from igraph import *
import os
import pandas as pd

from scipy.sparse import  csr_matrix
import igraph

def changeNameToID(tableName,id , plan = 'A'):
    """

    :param tableName: the table name
    :param id: the primary id
    :param plan:plan A uses category.cat.codes to change the id , plan B use sklearn's encoder to encode id
    :return: return a tuple ,of which first is the table using encoded id , second is the original id
    """
    if plan == 'A':

        originalName = tableName[id]

        originalName_index = originalName.index

        originalNameUnique = originalName.unique()

        originalNameCategory = originalName.astype("category", categories=originalNameUnique).cat.codes

        mapdict = dict(zip(originalNameCategory,originalName))

        tableName[id] = originalNameCategory

        del originalName_index,originalNameUnique,originalNameCategory



        return(tableName,mapdict )

    elif plan == 'B':

        le = LabelEncoder()

        le.fit(tableName[id])

        originalName = tableName[id]

        originalNameCategory = le.transform(tableName[id])

        mapdict = dict(zip(originalName, originalNameCategory))

        tableName[id] = originalNameCategory

        return (tableName,mapdict)


if __name__ =="__main__":
    os.chdir("C:\\Users\\22560\\Documents\\iptv")
    behavior = pd.read_csv("behavior.csv")

    # 找出被观看次数前200的“东西”

    behavior = behavior[ behavior['MEDIAID'].isin( behavior.groupby('MEDIAID')['USERID'].count().nlargest(200).reset_index()['MEDIAID'])]

    behavior,mediaid = changeNameToID(behavior,'MEDIAID',plan='A')
    behavior,userid  = changeNameToID(behavior, 'newUserID', plan='A')

    sna = behavior[['MEDIAID','newUserID']]
    del behavior
    sna['weight']  = 1
    media_user  = csr_matrix((sna.weight,(sna.MEDIAID,sna.newUserID)))
    media_media = media_user.dot(media_user.transpose())
    media_media = media_media.tocoo()
    network = pd.DataFrame({'row' : media_media.row,
    'col' : media_media.col})

    # build graphics
    g = Graph()
    g.add_vertices(media_media.shape[0])
    g.add_edges(network.apply(lambda x : (x['col'],x['row']),axis = 1).tolist())

    layout = g.layout("rt")
    plot(g, layout=layout).show()







