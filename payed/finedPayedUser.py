import pandas as pd
import os
from sltools import save_pickle

if __name__=="__main__":

    os.chdir("C:\\Users\\22560\\Documents\\iptv")

    behavior_dateparse = lambda x: pd.datetime.strptime(x, '%Y%m%d%H')
    behavior = pd.read_csv("./behavior/behaviorHasName.csv")
    behavior['STATIS_TIME'] = pd.to_datetime(behavior['STATIS_TIME'], infer_datetime_format=True)


    dianbo_dateparse = lambda x: pd.datetime.strptime(x, '%Y/%m/%d %H:%M')
    dianbo  = pd.read_csv("./behavior/dianbo.csv",encoding = 'gbk',
                          date_parser=dianbo_dateparse,parse_dates=['下单时间'])
    dianbo['下单时间'] = dianbo['下单时间'].map(lambda x: x.replace(minute=0, second=0))




    dinggou = pd.read_csv("./behavior/dinggou.csv",encoding='gbk')

    userHasPayHistory = set(dinggou['订购账号'])| set(dianbo['下单用户'])
    save_pickle(userHasPayHistory,"./temp/userHasPayHistory.data")



    # find pay item

    itemNeedPay = dianbo['业务产品名称'].str.replace(r'[\(（].*[\)）]','').drop_duplicates()

    save_pickle(itemNeedPay,"./temp/itemNeedPay.data")














