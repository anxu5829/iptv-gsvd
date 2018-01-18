import os
os.chdir("C:\\Users\\22560\\Desktop\\")

import  pandas as pd


new_data =  pd.read_csv("C:\\Users\\22560\\Desktop\\behavior.csv",encoding = 'gbk')

data = pd.read_csv("C:\\Users\\22560\\Documents\\iptv\\behavior.csv")

data_contentname = set(data.CONTENTNAME.unique())
new_data_contentname = set(new_data.CONTENTNAME.unique())

len(data_contentname & new_data_contentname)
len(new_data_contentname)
len(data_contentname)

dataNoID = pd.Series(list(new_data_contentname - data_contentname))
dataNoID = dataNoID.dropna()
dataNoID[dataNoID.str.contains(r'\][0-9]*＿')]



















data_user = set(data['USER_ID'])


time = data.STATIS_TIME.astype(str)
time.str.slice(0,8).unique()

dianbo = pd.read_csv("C:\\Users\\22560\\Documents\\iptv\\dianbo.csv",encoding='gbk')

dianbo_user = set(dianbo['下单用户'])



dinggou = pd.read_csv("C:\\Users\\22560\\Documents\\iptv\\dinggou.csv",encoding='gbk')
dinggou_user = set(dinggou['订购账号'])

len(data_user)
len(data_user & dinggou_user)
len(data_user & dianbo_user)











