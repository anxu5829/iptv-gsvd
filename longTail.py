import os
import pandas as pd
import seaborn as sns
import numpy as np

if __name__ == "__main__":
    os.chdir("C:\\Users\\22560\\Documents\\iptv")
    behavior = pd.read_csv("./originalData/behavior.csv")
    longTail = behavior.groupby('name').newUserID.count().reset_index().rename(columns = {"newUserID":"viewCount"})
    longTail =  longTail.groupby('viewCount',as_index= False).agg({"name":"count"})
    sns.regplot(x='viewCount',y='name',data=longTail,fit_reg=False)

    longTail['percent'] = longTail['name'].cumsum() / longTail['name'].sum()
