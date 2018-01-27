# -*- coding:utf-8 -*-
import gc

from sklearn.preprocessing import LabelEncoder
import os
import shutil
import pandas as pd

"""
data preparaton

"""

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


def clearDir(rootdir):
    filelist = []
    filelist = os.listdir(rootdir)
    for f in filelist:
        filepath = os.path.join(rootdir, f)
        if os.path.isfile(filepath):
            os.remove(filepath)
            print(filepath + " removed!")
        elif os.path.isdir(filepath):
            shutil.rmtree(filepath, True)
            print("dir " + filepath + " removed!")

def objGroupMapGenerator(trainSet,groupName,obj):
    return trainSet.groupby(groupName)[obj].apply(lambda x :x.unique().tolist())

def invertDict(my_map):
    inv_map = {v: k for k, v in my_map.items()}
    return  inv_map


def userRecord(trainSet,itemid,itemTag,userNum):
   return(
       pd.DataFrame(
           {"name": trainSet[trainSet['newUserID'] == userNum]['TV_NAME'].map(itemid),
    "tag":trainSet[trainSet['newUserID'] == userNum]['tag'].map(itemTag)
   })
   )