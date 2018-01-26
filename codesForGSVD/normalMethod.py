from surprise import SVD
from surprise import Dataset
from surprise.model_selection import cross_validate
from surprise.model_selection import train_test_split
from surprise import accuracy
from surprise.reader import Reader
import os
import pandas as pd
from sklearn.decomposition import  PCA

if __name__ == "__main__":
    os.chdir("C:\\Users\\22560\\Documents\\iptv")
    behavior = pd.read_csv("C:\\Users\\22560\\Documents\\iptv\\originalData\\behavior.csv")
    reader = Reader(rating_scale=(behavior['MEDIACOUNT'].min(),behavior['MEDIACOUNT'].max()))

    data = Dataset.load_from_df(behavior[['newUserID', 'TV_NAME', 'MEDIACOUNT']], reader)
    trainset, testset = train_test_split(data, test_size=.25)
    algo = SVD()
    algo.fit(trainset)

    predictions = algo.test(testset)
    accuracy.rmse(predictions)

    userattr = algo.pu
    itemattr = algo.qi

    pca = PCA(n_components=2)
    behavior.head()





















