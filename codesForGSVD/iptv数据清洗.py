#读取数据
import pandas as pd
from pandas import  Series
import os
import numpy as np
os.chdir("C:\\Users\\22560\\Documents\\iptv")
result1=pd.read_csv("result1.csv")

result1.groupby(result1.MEDIAID.str.slice(0,2))['MEDIAID'].count()
# '''
# 结果
# 00        192
# hn      20030
# jh          4
# jr      14230
# m0    4794373
# m1      73970
# m2     709005
# m3          3
# m5          5
# m_    4198842
# mf         23
# mo       2698
# ym    1422158
# '''




#清洗数据集00
#读取数据
#(192, 5)只有192个样本

temp=result1[ result1['CONTENTNAME'].str.slice(0,2) == "00" ]['CONTENTNAME']

#按关键词提取
temp_result=temp.str.contains('电竞|新闻|解说|吃鸡|绝地求生')
#分类求和

temp.groupby(temp_result).count()


# CONTENTNAME
# False      8.0
# True     184.0
#192个样本中有184个为游戏类节目，可以判定此数据集主要为游戏节目


#清洗数据集hn
result1 = result1.dropna(axis = 0,how='any')
#将此数据集删去


#清洗数据集jh
# (4, 5)只有4个样本，忽略不记
result1 = result1[result1.MEDIAID.str.slice(0,2) != 'jh' ]


# jr
动画    = result1[result1.MEDIAID.str.slice(0,2) == 'jr' ].CONTENTNAME.str.split(r'第?\d{1,2}集').\
                map( lambda x: x[0]
            )



动画集数 = result1[result1.MEDIAID.str.slice(0,2) == 'jr' ].CONTENTNAME.str.findall(r'第?\d{1,2}集').map(lambda x: x[0])
# ['功夫鸡', '滚滚玩具颜色屋', '欧布奥特曼', '铁蛋大冒险', '海底小纵队', '酷垒积木大作战', '闲画部落',
# '原生之初', '汪汪队玩具秀', '奥飞救救帮', '巴拉拉小魔仙', '超级飞侠欢乐黏土', '艾克斯奥特曼', '臭屁虫第三季',
# '斗龙战士第五季', '樱桃小丸子', '超级飞侠第二季01集', '臭屁虫第二季', '魔幻陀螺', '神奇鸡仔英文',
# '银河奥特曼s', '艾克斯奥特曼中文', '宝宝巴士奇妙汉字第一季', '银河奥特曼大怪兽', '乐高玩具秀', '小汽车欧力',
# '奇积乐园', '熊孩子绘本故事', '猪猪侠安全教育', '钢甲小龙侠', '逗比僵尸', '花园宝宝', '精灵梦叶罗丽第四季',
# '兽王争锋', '图图的数学故事', '小伴龙儿歌第一季', '小猪佩奇玩具秀', '小伶玩具第212集', '巨神救救帮第三季',
# '宝宝巴士儿歌', '宝宝巴士好习惯', '涂鸦宝宝', '昆塔因为所以', '口水三国第001集', '星兽猎人',
# '滚滚玩具拆蛋队', '西游记的故事', '铠甲勇士', '爆裂兄弟第二季', '魔幻车神第二季01集', '魔幻车神第二季12集',
# '京剧猫', '魔幻车神第二季26集', '精灵梦叶罗丽第三季', '超级飞侠第三季', '精灵梦叶罗丽第五季', '火线英雄',
# '芭比公主玩具秀', '爱精灵乐吉儿', '星学院', '神奇鸡仔儿歌', '布鲁精灵01集', '超级飞侠简笔画',
# '神奇鸡仔中文', '钢甲卡卡龙', '激战奇轮第三季', '魔幻车神第二季13集', '乐比悠悠第二季', '海底小纵队特别',
# '蓝猫淘气3000问恐龙传奇']
#可以看出此类电视剧全为动画片


#清洗数据集m_

result1[result1.MEDIAID.str.slice(0,2) == 'm_']
namem_=pd.read_csv(os.path.join(d,'m_.csv'),encoding='gbk')
namem_=namem_[pd.notnull(namem_.CONTENTNAME)]#除去数据集中的空集
len(namem_)#4177618
#默认CONTENTNAME中带有“集”或者连续两个数字以上的为电视剧
TVSeries=namem_[namem_.CONTENTNAME.str.contains(r'\d+集|\d\d\d?|\(\d*\)')]
len(TVSeries)#3565434，占比85.3%

#剩下的思路：
Others=namem_[namem_.CONTENTNAME.str.contains(r'\d+集|\d\d\d?|\(\d*\)')==False]

#在Others数据集中，看Others.CONTENTNAME 有哪些落在TVSeries.CONTENTNAME中，如果落在其中的话，则为电视剧
#将Others按'MEDIAID'和'CONTENTNAME1'进行排序，看电视剧的MEDIAID是否靠在一起
Others=Others.sort_values(by=['MEDIAID','CONTENTNAME1'])


##########################################################################
############################################################################
#读取数据m0
namem0=pd.read_csv(os.path.join(d,'m0.csv'),encoding='gbk')
len(namem0)#4794373
#默认含有“数字+集”的都为电视剧
TVSeries=namem0[namem0['CONTENTNAME'].str.contains(r'\d集')]
len(TVSeries)#3845747,占比80.2%

#其它思考
#探索在剩下的数据集中有多少电视剧在TVSeries中
temp=namem0[namem0['CONTENTNAME'].str.contains(r'三生三世十里桃花')]#示例

TVSeries['CONTENTNAME1']=TVSeries['CONTENTNAME'].str.replace(r'第\d*集','')
TVSeriesUnique=list(TVSeries.CONTENTNAME1.unique())#或得TVSeries的unique

Others=namem0[namem0['CONTENTNAME'].str.contains(r'\d集')==False]
sum(Others.CONTENTNAME.isin(TVSeriesUnique))#2287，太少，微不足道


##########################################################################
############################################################################

#读取数据m1
namem1=pd.read_csv(os.path.join(d,'m1.csv'),encoding='gbk')
len(namem1)#73970
#默认MEDIAID中含有字符“_”的即为电视剧
TVSeries=namem1[namem1['MEDIAID'].str.contains(r'_')]
len(TVSeries)#58322  占比78.8%


#读取数据m2，数据m2的处理与m1相似
namem2=pd.read_csv(os.path.join(d,'m2.csv'),encoding='gbk')
print(len(namem2))#709005
TVSeries=namem2[namem2['MEDIAID'].str.contains(r'_')]
print(len(TVSeries))#638132  占比90%

#数据m3、m5、mo太小，忽略
#数据ym
#数据ym的操作与m_和m0相似
nameym=pd.read_csv(os.path.join(d,'ym.csv'),encoding='gbk')
print(len(nameym))#1422158
TVSeries=nameym[nameym['CONTENTNAME'].str.contains(r'\d集')]
print(len(TVSeries))#1232777 占比86.7%


#如此，将数据集00——ym拼接起来就可以得到打上电视剧标签的result1




#最终目标数据

#原始数据信息：

#
# media_id name
# m1_01    网球王子01
# m1_02    网球王子02
# m2       前任
#
# # 目标数据
#
# media_id    name       集数   具体集标号
# m1        网球王子        2    [m1_01 , m1_02]
# m1        网球王子        2    [m1_01 , m1_02]
# m2        前任            1    [m2]
#


