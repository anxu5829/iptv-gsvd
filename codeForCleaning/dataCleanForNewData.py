import os
import pandas as pd
import numpy as np
import multiprocessing
import re
import requests
from bs4 import BeautifulSoup
import time
import  numpy as np
import gc
from sltools import save_pickle,load_pickle
import multiprocessing
import time

def scraping(data):
    typeList = dict()
    processName = multiprocessing.current_process().name
    counter = 0
    for name in data:
        counter += 1
        try:
            r = requests.get("http://www.baidu.com/s?wd="+name)
            if r.status_code == 200:
                beautiful = BeautifulSoup(r.content.decode('utf-8'), "lxml")

                typeOfit = beautiful.find(attrs={'mu':re.compile(r'baike')})
                try:
                    if type(typeOfit.p.text) == str :
                        typeList[name] = (typeOfit.p.text)
                        print('process name is ',processName,' name :',name, ' type: ', typeOfit.p.text)
                    else:
                        typeList[name] = "no type"
                        print('process name is ',processName,name, '  ', "no type")
                    #time.sleep(np.random.random()*4)

                except:
                    typeList[name] = "no type"
                    print('process name is ', processName, name, '  ', "no type")
                    save_pickle(typeList, str(processName) + 'typeList.data')
                    save_pickle(counter, str(processName) + 'counterRecord.data')

            else:
                typeList.append('no name')
                print(name, '  ', "no type")
            if counter%2000 == 0:
                print('process',str(processName),'is now having a ',"counter of",counter)
                save_pickle(typeList,  str(processName) +'typeList.data')
                save_pickle(counter, str(processName) + 'counterRecord.data')

        except:
            save_pickle(typeList, str(processName) + 'typeList.data')
            save_pickle(counter ,  str(processName) + 'counterRecord.data')

    print(typeList)
    save_pickle(typeList,str(processName)+'typeList.data')


def scrapingNotype(data):
    typeList = dict()
    processName = multiprocessing.current_process().name
    counter = 0
    np.random.shuffle(data)
    errorcount = 0
    for name in data:
        counter += 1
        try:
            #time.sleep(2)
            r = requests.get("http://www.baidu.com/s?wd="+name)
            if r.status_code == 200:
                beautiful = BeautifulSoup(r.content.decode('utf-8'), "lxml")

                typeOfit = beautiful.find(attrs={'tpl':re.compile(r'se_st_single_video_zhanzhang')})
                typeOfit = typeOfit.h3.a.text
                try:
                    if type(typeOfit) == str :
                        typeList[name] = (typeOfit)
                        print('process name is ',processName,' name :',name, ' type: ',typeOfit)
                    else:
                        typeList[name] = "no type"
                        print('process name is ',processName,name, '  ', "no type")
                    #time.sleep(np.random.random()*4)

                except:
                    typeList[name] = "no type"
                    print('process name is ', processName, name, '  ', "no type")
                    print("save it !")
                    save_pickle(typeList, str(processName) + 'no_type_typeList.data')
                    save_pickle(counter, str(processName) + 'counterRecord.data')

            else:
                typeList.append('no name')
                print(name, '  ', "no type")
            if counter%2000 == 0:
                print('process',str(processName),'is now having a ',"counter of",counter)
                save_pickle(typeList,  str(processName) +'no_type_typeList.data')
                save_pickle(counter, str(processName) + 'counterRecord.data')

        except:
            typeList[name] = "no type"
            save_pickle(typeList, str(processName) + 'no_type_typeList.data')
            save_pickle(counter ,  str(processName) + 'counterRecord.data')

    print(typeList)
    save_pickle(typeList,str(processName)+'no_type_typeList.data')




if __name__=="__main__":
    os.chdir("C:\\Users\\22560\\Documents\\iptv")


    # part1 提取原始数据中的名称信息
    behavior_dateparse = lambda x: pd.datetime.strptime(x, '%Y%m%d%H')
    behavior = pd.read_csv("./behavior/behavior.csv", dtype={'CONTENTNAME': str, 'USERID': str},
                           parse_dates=['STATIS_TIME'],
                           date_parser=behavior_dateparse, encoding='gbk'
                           # ,iterator=True
                           )


    # 进行数据清洗，选择出所有的电视剧，使得节目的数量能够被进一步压缩

    # 1、去除CONTENTNAME中的NA行
    behavior = behavior[pd.notnull(behavior['CONTENTNAME'])]
    behavior['ORIGINALNAME'] = behavior['CONTENTNAME']
    # 2 去除contentName中的特殊字符
    behavior['CONTENTNAME'] = behavior['CONTENTNAME'].str.replace(r'^\s+|(\[)?\w*\]|＿|-.*$', '')
    # 3、有的CONTENTNAME都为数字，将这些去掉
    behavior = behavior[behavior['CONTENTNAME'].str.contains(r'^\d+$') == False]

    #############################
    # 下面对CONTENTNAME进行处理，以提取电视剧名称
    # 开头或者结尾是数字;以第d集或d集结尾；
    temp = behavior.CONTENTNAME.str.replace(r'^第?\d+集', '')
    temp = temp.str.replace(r'^\d+|\d+$|第?\d+集(.*$)?|第.+集|\(.+\)|（.+）|全集|南京骨干区域', '')
    behavior['TV_NAME'] = temp
    behavior.to_csv('behaviorHasName.csv',index=False)





    # part2


    #
    # # 进行数据清洗，选择出所有的电视剧，使得节目的数量能够被进一步压缩
    #
    # # 1、去除CONTENTNAME中的NA行
    # behavior = behavior[pd.notnull(behavior['CONTENTNAME'])]
    # behavior['ORIGINALNAME'] = behavior['CONTENTNAME']
    # # 2 去除contentName中的特殊字符
    # behavior['CONTENTNAME'] = behavior['CONTENTNAME'].str.replace(r'^\s+|(\[)?\w*\]|＿|-.*$', '')
    # # 3、有的CONTENTNAME都为数字，将这些去掉
    # behavior = behavior[behavior['CONTENTNAME'].str.contains(r'^\d+$') == False]
    #
    # #############################
    # # 下面对CONTENTNAME进行处理，以提取电视剧名称
    # # 开头或者结尾是数字;以第d集或d集结尾；
    # temp = behavior.CONTENTNAME.str.replace(r'^第?\d+集', '')
    # temp = temp.str.replace(r'^\d+|\d+$|第?\d+集(.*$)?|第.+集|\(.+\)|（.+）|全集|南京骨干区域', '')
    # behavior['TV_NAME'] = temp
    # behavior.to_csv('behaviorHasName.csv',index=False)
    #
    #
    # #爬取新数据,获取TV_NAME的信息
    #
    #
    #
    #
    #
    #
    # # 对人进行分类，统计人的兴趣爱好，从而对人群划分
    # behavior['watchingTime'] =  pd.cut(
    #     pd.to_numeric(behavior.STATIS_TIME.dt.hour),
    #     bins=[-1, 5, 13, 17, 25],
    #     labels=['Mdnt', 'Day', 'Aftn', 'Nght']
    # )
    # behavior['newUserID'] = behavior['USER_ID'] + behavior['watchingTime'].astype(str)
    #
    #
    # #统计每个用户在每个时间区间看了多少的电视节目
    # behavior   = behavior.set_index(['newUserID', 'STATIS_TIME'])
    # mediacount = 1 / behavior.groupby(['newUserID','STATIS_TIME'])[['CONTENTNAME']].count().rename(columns = {"CONTENTNAME":"MEDIACOUNT"})
    # behavior_with_count = pd.merge(behavior, mediacount, left_index=True, right_index=True).reset_index()
    #
    # user_item_evaluate  =  behavior_with_count.groupby(['newUserID','TV_NAME']).aggregate({'MEDIACOUNT':'sum'}).reset_index()
    #
    #
    #
    # #统计每个用户在过去的几天都看了多少的东西
    # behavior = behavior.reset_index()
    # user_item_evaluate.groupby('USER_ID').CONTENTNAME.count().mean()
    #
    #
    # #洗数据
    # #user_item_evaluate.to_csv("./behavior/user_item_evaluate.csv",index=False)
    # user_item_evaluate = pd.read_csv("./behavior/user_item_evaluate.csv")
    # TV_SET = (user_item_evaluate['TV_NAME'].unique())
    #
    # os.chdir(".\\temp")
    #
    # # 获取电视剧的类型
    # data = np.split(TV_SET, [int(.2 * len(TV_SET)),
    #                            int(.4 * len(TV_SET)),
    #                            int(.6 * len(TV_SET)),
    #                            int(.8 * len(TV_SET))
    #                            ])
    #
    #
    # processList = []
    # for i in range(5):
    #     process = multiprocessing.Process(target=scraping, name=str(i), args=(data[i],))
    #     processList.append(process)
    #
    # for i in range(5):
    #     processList[i].start()
    #
    # for i in range(5):
    #     processList[i].join()
    #
    # nameList = []
    # typeList = []
    #
    # for i in range(5):
    #     type = load_pickle(str(i) + 'typeList.data')
    #     nameList.extend(list(type.keys()))
    #     typeList.extend(list(type.values()))
    #
    # name_type = pd.DataFrame({'name': nameList, 'type': typeList})
    # #name_type.shape
    #
    # #name_type.groupby('type').count().sort_values(by='name', ascending=False)[:10]
    #
    # #name_type[name_type.type == 'no type']
    # name_type.to_csv('name_type.csv')
    # name_type[name_type['type'] == "no type"].to_csv("no_type.csv")
    #
    # #读取no_type 的信息


    os.chdir("C:\\Users\\22560\\Documents\\iptv\\temp\\")
    name = pd.read_csv("no_type.csv")

    print("name is loaded!!")
    gc.collect()
    nameList = name.iloc[:, 1].unique()
    np.random.shuffle(nameList)
    data = np.split(nameList, [int(.2 * len(nameList)),
                               int(.4 * len(nameList)),
                               int(.6 * len(nameList)),
                               int(.8 * len(nameList))
                               ])

    print(len(nameList))
    del name
    processList = []
    for i in range(5):
        process = multiprocessing.Process(target=scrapingNotype, name='no type'+str(i), args=(data[i],))
        processList.append(process)

    for i in range(5):
        processList[i].start()

    for i in range(5):
        processList[i].join()

    nameList = []
    typeList = []

    for i in range(5):
        type = load_pickle('no type'+str(i) + 'no_type_typeList.data')
        nameList.extend(list(type.keys()))
        typeList.extend(list(type.values()))

    name_type = pd.DataFrame({'name': nameList, 'type': typeList})
    name_type.to_csv('no_type_typeList.csv', index=False)
    #
    # #
    name_type1 = pd.read_csv('name_type.csv',usecols =['name','type'] )
    name_type2 = pd.read_csv('no_type_typeList.csv',usecols = ['name','type'])

    type = '类型：电影作品|类型：电视剧作品|类型：动画作品|类型：单曲|类型：游戏作品|类型：电视节目|类型：漫画作品|类型：小说作品|类型：专辑|类型：轻小说作品'
    name_type1['tag'] = name_type1.type.str.findall(type).map(lambda x: x[0] if len(x) > 0 else "no type")

    type = '原创|儿童|娱乐|游|片花|CC|母婴|搞笑|时尚|生活|动漫|教|体育|资讯|少儿|音乐|综艺|电影|纪录片|电视剧|在线观看|搞笑视频|健'
    name_type2['tag'] = name_type2.type.str.findall(type).map(lambda x: x[0] if len(x) > 0 else "no type")



    artists_tag = pd.merge(name_type1[['name','tag']],name_type2[['name','tag']],left_on='name',right_on='name',how='left')

    artists_tag['tag'] = artists_tag.apply(lambda x: x['tag_y'] if x['tag_x'] == 'no type' else x['tag_x'], axis=1)
    artists_tag['tag'] = artists_tag['tag'].fillna("no type")


    tagdict={'类型：电影作品':"电影", '时尚':"其他", '类型：电视剧作品':"电视剧",
             '原创':"其他", 'no type':"no type", '类型：单曲':"音乐", '在线观看':"其他",
       '类型：电视节目':"节目", 'CC':"节目", '少儿':"动漫", '综艺':"节目", '儿童':"动漫",
             '搞笑':"节目", '类型：动画作品':"动画", '娱乐':"节目",
       '类型：游戏作品':"游戏", '母婴':"教育", '类型：漫画作品':"动漫", '动漫':'动漫', '游':'游戏',
             '电影':"电影", '教':'教育', '类型：轻小说作品':'动漫', '体育':"体育",
       '音乐':'音乐', '健':"体育", '生活':"其他", '纪录片':"纪录片", '电视剧':"",
             '片花':"节目", '类型：小说作品':"其他", '资讯':"节目", '类型：专辑':"音乐"}


    artists_tag['tag'] = artists_tag['tag'].map(tagdict)

    #

    #
    # user_group = behavior.groupby(level='newUserID')[['watchingTime']].first()
    # user_group.reset_index(inplace = True)
    # user_item_evaluate = pd.merge(user_item_evaluate, user_group, on='newUserID')
    # user_item_evaluate.rename(columns = {"watchingTime":"userGroup"},inplace =True)
    #
    # user_item_evaluate = pd.merge(user_item_evaluate, artists_tag[["name", "tag"]], left_on="TV_NAME", right_on="name",
    #                               how="left")
    #
    # user_item_evaluate.head()
    #
    # user_item_evaluate.to_csv("trainSet.csv",index=False)
    #



    #
    #
    # # 考虑订购的信息
    # dianbo = pd.read_csv("dianbo.csv",encoding = 'gbk')
    #
    # behavior[behavior.CONTENTNAME.isin(dianbo['业务产品名称'].unique())].CONTENTNAME.unique()
    #
    #
    # dinggou = pd.read_csv("dinggou.csv",encoding='gbk')
    # dianbo  = pd.read_csv("dianbo.csv",encoding = 'gbk')
    #
    # userSpecial = set(dianbo['下单用户'].unique()) | set(dinggou['订购账号'].unique())
    # userSpecial.__len__()
    #
    # (set(behavior['USER_ID']) & userSpecial).__len__()
    #
    # set(behavior['USER_ID']).__len__()
    #
    # specialView = set(behavior[behavior.USER_ID.isin(userSpecial)].CONTENTNAME.unique())-\
    #               set(behavior[-behavior.USER_ID.isin(userSpecial)].CONTENTNAME.unique())
    # specialView.__len__()
    #
    # behavior['isSpecial']  = behavior['USER_ID'].isin(userSpecial)
    #
    #
    # behavior.head()
    #
    #
    #
    #
    #
    #
    #
    #
