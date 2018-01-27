import networkx as nx
import matplotlib.pyplot as plt
import os
import pandas as pd
from scipy.sparse import  csr_matrix
if __name__ == "__main__":
    G = nx.petersen_graph()
    plt.subplot(121)
    nx.draw(G, with_labels=True, font_weight='bold')
    plt.subplot(122)
    nx.draw_shell(G, nlist=[range(5, 10), range(5)], with_labels=True, font_weight='bold')

    os.chdir("C:\\Users\\22560\\Documents\\iptv")
    behavior = pd.read_csv("./originalData/behavior.csv")

    # 找出被观看次数前200的“东西”

    #behavior = behavior[ behavior['TV_NAME'].isin( behavior.groupby('TV_NAME')['newUserID'].count().nlargest(200).reset_index()['TV_NAME'])]


    sna = behavior[['TV_NAME','newUserID']].copy()
    del behavior
    sna.loc[:,'weight']  = 1
    media_user  = csr_matrix((sna.weight,(sna.TV_NAME,sna.newUserID)))
    media_media = media_user.dot(media_user.transpose())
    numofitem = media_media.shape[0]
    media_media[range(numofitem),range(numofitem)] = 0

    G = nx.from_scipy_sparse_matrix(media_media)
    nx.write_gexf(G, 'tvrellation.gexf')

