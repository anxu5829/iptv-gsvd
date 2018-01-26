import pandas as pd
import numpy as np
import os
from matplotlib import  pyplot as plt
import scipy.sparse as sparse
from sltools import  load_pickle

import numpy as np
from matplotlib import pyplot as plt

from sklearn.datasets import make_biclusters
from sklearn.datasets import samples_generator as sg
from sklearn.cluster.bicluster import SpectralCoclustering
from sklearn.cluster.bicluster import SpectralBiclustering
from sklearn.metrics import consensus_score


if __name__ == "__main__":
    os.chdir("C:\\Users\\22560\\Documents\\iptv")
    behavior = pd.read_csv("./originalData/behavior.csv")
    test = pd.read_csv("./originalData/test.csv")
    train = pd.concat([behavior,test])
    train_sparse = sparse.csr_matrix((np.exp(train['MEDIACOUNT']),(train['newUserID'],train['TV_NAME'])))
    #plt.matshow(train_sparse.toarray())

    userHasPayHistory = load_pickle("./temp/userHasPayHistory.data")
    itemNeedPay = load_pickle("./temp/itemNeedPay.data")
    mediaid = load_pickle("./temp/mediaid.data")
    userid = load_pickle("./temp/userid.data")

    itemNeedPayList = [j for i, j in mediaid.items() if i in itemNeedPay.tolist()]
    userid = pd.DataFrame({"id": [i for i, j in userid.items()],
                           "name": [j for i, j in userid.items()]})
    userid['original'] = userid['id'].str.replace(r'Day|Aftn|Nght|Mdnt', "")
    userHasPayHistory = userid.loc[userid['original'].isin(userHasPayHistory), 'name'].tolist()

    train_sparse = train_sparse[userHasPayHistory,:][:,itemNeedPayList]
    #model = SpectralCoclustering(n_clusters=5, random_state=0)
    model = SpectralBiclustering(n_clusters=5, method='log',
                                 random_state=0)
    model.fit(train_sparse.toarray())
    data = train_sparse.toarray()
    fit_data = data[np.argsort(model.row_labels_)]
    fit_data = fit_data[:, np.argsort(model.column_labels_)]

    plt.matshow(fit_data, cmap=plt.cm.Blues)