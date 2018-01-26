import os
import numpy as np
import pandas as pd
from collections import  Counter
from usefulTool import changeNameToID
import gc
from sltools import save_pickle
import re

def timeinfoGenerator():
    # reading the behavior table, which containing the info of user-item info
    behavior_dateparse = lambda x: pd.datetime.strptime(x, '%Y%m%d%H')
    behavior = pd.read_csv("./behavior/behavior.csv", dtype={'CONTENTNAME': str, 'USERID': str},
                           parse_dates=['STATIS_TIME'],
                           date_parser=behavior_dateparse,encoding = 'gbk'
                           #,iterator=True
                           )
    #behavior = behavior.get_chunk(100)
    #behavior['date'] = behavior.STATIS_TIME.map(lambda x: ''.join([str(x.year), str(x.month), str(x.day), str(x.hour)]))
    behavior = behavior.dropna(axis=0, how='any')
    behavior['date']   = behavior.STATIS_TIME.dt.strftime('%Y%m%d%H')

    behavior_time_info = pd.DataFrame(
        {'hour' : behavior.STATIS_TIME.dt.strftime('%H'),
         'Week' : behavior.STATIS_TIME.dt.weekday.map(lambda x: 1 if x == 7 else (x +1) )
         }
    )

    behavior_time_info['ifWeekend']= behavior_time_info['Week'].map(lambda x: x in [6, 7])


    # wkHrMID = behavior.groupby(['ifWeekend','hour']).apply(lambda x: x.groupby('CONTENTNAME')['USERID'].count().nlargest(10).index.tolist())
    # wkHrMID = wkHrMID.to_frame()
    # wkHrMID.reset_index(inplace = True)
    # wkHrMID.to_csv("userHabbit.csv")



    behavior_time_info['hour_split_weekend']  = pd.cut(
                        pd.to_numeric(behavior_time_info.hour),
                        bins=[-1, 5, 12, 19, 24],
                        labels=['wedMdnt', 'wedMrng', 'wedAft', 'wedNt']
                        )

    behavior_time_info['hour_split_week']  = pd.cut(
                            pd.to_numeric(behavior_time_info.hour),
                            bins=[-1, 5, 18, 21, 24],
                            labels=['wkMdnt', 'wkDay', 'wkEve', 'wkNght']
                        )


    behavior['userGroup'] = behavior_time_info['ifWeekend'] * behavior_time_info['hour_split_weekend'].astype(str)+ \
                                      (- behavior_time_info['ifWeekend']) * behavior_time_info['hour_split_week'].astype(str)


    behavior['newUserID'] = behavior['USERID'] + behavior['userGroup']


    behavior['hourInfo']  = behavior.STATIS_TIME.dt.hour

    del behavior_time_info
    gc.collect()

    # SORT BY STATIS_TIME , for the purpose of
    # calculating how many bangumi he watched each hour
    behavior = behavior.sort_values(by = ['newUserID','STATIS_TIME'])


    behavior.to_csv("behavior.csv",index = False)


if __name__ == "__main__":

    # user info extracter :
    gc.collect()
    # reading data and encode item & user name
    os.chdir("C:\\Users\\22560\\Documents\\iptv")


    # generate time info for later analysis
    timeinfoGenerator()

    behavior = pd.read_csv("behavior.csv",
                           usecols=['MEDIAID','CONTENTNAME','userGroup','newUserID','STATIS_TIME'])

    # dealing with type and name temperory

    第几集 = re.compile(r'第\d{1,2}集')
    括号集 = re.compile(r'\(\d{1,2}\)(?=[\u4e00-\u9fa5]*)')
    数字集 = re.compile(u'\d{1,2}(?=[\u4e00-\u9fa5]*)')
    集在前 = re.compile(r'\[HD\]\d{1,2}')
    第几季 = re.compile(r'(第[一二三四五六七八九十]季)|第\d{1,2}季')
    第几期 = re.compile(r'第[一二三四五六七八九十]期|第\d{1,2}期')
    之 = re.compile(r'之')
    全集 = re.compile(r'全集')


    def ruleSelect(x):
        if 集在前.search(x) != None:
            return (re.split(集在前, x)[0])
        elif 第几季.search(x) != None:
            return (re.split(第几季, x)[0])
        elif 第几集.search(x) != None:
            return (re.split(第几集, x)[0])
        elif 括号集.search(x) != None:
            return (re.split(括号集, x)[0])
        elif 第几期.search(x) != None:
            return (re.split(第几期, x)[0])
        elif 数字集.search(x) != None:
            return (re.split(数字集, x)[1])
        else:
            return (x)


    # 去除括号内
    去括号 = re.compile(r'\（[\u4e00-\u9fa5]*\）')
    去中括号 = re.compile(r'\[*\]')
    去国语 = re.compile(r'-[\u4e00-\u9fa5]*')

    behavior['name'] = behavior.CONTENTNAME.map(
        ruleSelect
    ).map(lambda x: re.split(去括号, x)[0]). \
        map(lambda x: re.split(去中括号, x)[0]). \
        map(lambda x: re.split(去国语, x)[0])



    # calculating how many bangumi he watched each hour
    behavior = behavior.set_index(['newUserID','STATIS_TIME'])
    mediacount =1/ behavior.groupby(['newUserID','STATIS_TIME'])[['MEDIAID']].count().rename(columns = {"MEDIAID":"MEDIACOUNT"})



    behavior_with_count = pd.merge(behavior,mediacount,left_index=True,right_index=True)

    # get  user item evaluate
    user_item_evaluate  =  behavior_with_count.groupby(['newUserID','name']).aggregate({'MEDIACOUNT':'sum'})

    # get user group
    user_group =     behavior.groupby(level='newUserID')[['userGroup']].first()

    # get item group
    artist_type = pd.read_csv("name_type.csv",usecols=['name','type'])
    # type = '|'.join(artist_type.groupby('type').count().sort_values(by='name', ascending=False)[:20].index.tolist())
    type = '类型：电影作品|类型：电视剧作品|类型：动画作品|类型：单曲|类型：游戏作品|类型：电视节目|类型：漫画作品|类型：小说作品|类型：专辑|类型：轻小说作品'
    artist_type['tag'] = artist_type.type.str.findall(type).map(lambda x: x[0] if len(x)>0 else "no type")


    artist_no_type = pd.read_csv("no_type_typeList.csv", usecols=['name', 'type'])

    type = '原创|儿童|娱乐|游|片花|CC|母婴|搞笑|时尚|搜狐视频|生活|动漫|教|体育|资讯|少儿|音乐|综艺|电影|纪录片|电视剧|在线观看|搞笑视频|健'
    artist_no_type['tag'] = artist_no_type.type.str.findall(type).map(lambda x: x[0] if len(x)>0 else "no type")
    artist_no_type.groupby('tag').count().sum()



    artists_tag = pd.merge(artist_type[['name','tag']],artist_no_type[['name','tag']],left_on='name',right_on='name',how='outer')

    artists_tag['tag'] = artists_tag.apply(lambda x: x['tag_y'] if x['tag_x'] == 'no type' else x['tag_x'],axis = 1)

    artists_tag.groupby('tag')['name'].count()

    user_item_evaluate = user_item_evaluate.reset_index()
    user_item_evaluate =pd.merge(user_item_evaluate,user_group,left_on='newUserID',right_index=True)

    trainSet = pd.merge(user_item_evaluate,artists_tag[['name','tag']],right_on='name',left_on='name')

    trainSet.to_csv("trainSet.csv",index=False)

    #
    # #userHabbitCount = behavior.groupby(['ifWeekend','hour']).count()
    #
    #
    # # generating a dict containing the data of media-id - media-name
    # MEDIA_MEDIANAME =  behavior.groupby(['MEDIAID','CONTENTNAME']).aggregate({"USERID":'count'}).rename({"USERID":"COUNT"})
    # MEDIA_MEDIANAME =  MEDIA_MEDIANAME.reset_index()
    # media_name      = dict(zip(MEDIA_MEDIANAME.MEDIAID , MEDIA_MEDIANAME.CONTENTNAME ))
    #
    #
    # # merge_tags
    #
    #
    # del MEDIA_MEDIANAME
    # gc.collect()
    #
    #
    #
    # behavior,mediaid = changeNameToID(behavior,'MEDIAID',plan='A')
    # behavior,userid  = changeNameToID(behavior, 'newUserID', plan='A')
    #
    #
    # save_pickle(mediaid, "mediaid.data")
    # save_pickle(userid, "userid.data")
    # save_pickle(media_name, "media_name.data")
    #
    # for _,data in behavior.groupby('userGroup'):
    #     data.to_csv( data.userGroup.iloc[0]+'.csv')
    #


    # freq stats info calculating



    # userFreq  = behavior.groupby('USERID')['MEDIAID'].count().rename({"MEDIAID":"UserActivity"}).reset_index()
    # mediaFreq = behavior.groupby('MEDIAID')['USERID'].count().rename({"USERID":"WatchingNumber"}).reset_index()
    #
    # userFreq['userclass']       =  pd.cut(userFreq.MEDIAID,4,labels=['VeryHateTv','HateTv','LikeTv','VeryLikeTV'])
    # userFreq['originalUserID'] =  userFreq.MEDIAID.map(userid)
    # mediaFreq['mediaclass'] = pd.cut(mediaFreq.USERID,4,labels=['VeryUnpopular','Unpopular','Popular','VeryPopular'])
    # mediaFreq['originalMedia'] = mediaFreq.MEDIAID.map(mediaid)
    # mediaFreq['contentName']   = mediaFreq.originalMedia.map(media_name)
    # mediaFreq.sort_values(by='WatchingNumber',inplace=True,ascending=False)
    #
    #
    #
    #





