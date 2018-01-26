from surprise import SVDpp
from surprise import Dataset
from surprise.model_selection import cross_validate
from surprise.model_selection import train_test_split
from surprise import accuracy
from surprise import Reader
import pandas as pd
import numpy as np
import os

if __name__ == "__main__":
    os.chdir("C:\\Users\\22560\\Documents\\iptv")
    behavior = pd.read_csv("./originalData/behavior.csv")
    test     = pd.read_csv("./originalData/test.csv")
    test     = pd.read_csv("./originalData/test.csv")
    data = pd.concat([behavior,test])
    algo = SVDpp()
    reader = Reader(rating_scale=(data.MEDIACOUNT.min(),
                                  data.MEDIACOUNT.max()))
    data = Dataset.load_from_df(data[['newUserID', 'TV_NAME', 'MEDIACOUNT']], reader)
    trainset, testset = train_test_split(data, test_size=.25)
    algo.fit(trainset)
    predictions = algo.test(testset)
    accuracy.rmse(predictions)

