import pandas  as pd
import os
import re
import requests
from bs4 import BeautifulSoup
import time
import  numpy as np
import gc
from sltools import save_pickle

if __name__ == "__main__":
    os.chdir("C:\\Users\\22560\\Documents\\iptv")
    behavior = pd.read_csv("behavior.csv"
                           #,iterator= True
                           )
    #behavior = behavior.get_chunk(10000)

    print("data loaded !!")
    第几集 = re.compile(r'第\d{1,2}集')
    括号集 = re.compile(r'\(\d{1,2}\)(?=[\u4e00-\u9fa5]*)')
    数字集 = re.compile(u'\d{1,2}(?=[\u4e00-\u9fa5]*)')
    集在前 = re.compile(r'\[HD\]\d{1,2}')
    第几季 = re.compile(r'(第[一二三四五六七八九十]季)|第\d{1,2}季')
    第几期 = re.compile(r'第[一二三四五六七八九十]期|第\d{1,2}期')
    之     = re.compile(r'之')
    def ruleSelect(x):
        if  集在前.search(x) != None:
            return(re.split(集在前,x)[0])
        elif 第几季.search(x) != None:
            return(re.split(第几季, x)[0])
        elif 第几集.search(x) != None:
            return(re.split(第几集,x)[0])
        elif 括号集.search(x) != None:
            return(re.split(括号集,x)[0])
        elif 第几期.search(x) != None:
            return(re.split(第几期,x)[0])
        elif 数字集.search(x) != None:
            return(re.split(数字集,x)[1])
        else:
            return(x)


    # 去除括号内
    去括号 = re.compile(r'\（[\u4e00-\u9fa5]*\）')
    去中括号 = re.compile(r'\[*\]')
    去国语 = re.compile(r'-[\u4e00-\u9fa5]*')

    name = behavior.CONTENTNAME.map(
        ruleSelect
    ).map(lambda x: re.split(去括号,x)[0]).\
        map(lambda x:re.split(去中括号,x)[0]).\
            map(lambda x:re.split(去国语,x)[0])

    name.to_csv("name.csv",index=False)

    print("name is loaded!!")
    gc.collect()
    nameList = name.unique()
    del name
    typeList = dict()
    counter = 0
    for name in nameList:
        counter += 1
        try:

            r = requests.get("http://www.baidu.com/s?wd="+name)
            if r.status_code == 200:
                beautiful = BeautifulSoup(r.content.decode('utf-8'), "lxml")

                typeOfit = beautiful.find(attrs={'mu':re.compile(r'baike')})
                try:
                    if type(typeOfit.p.text) == str :
                        typeList[name] = (typeOfit.p.text)
                        print(name, '  ', typeOfit.p.text)
                    else:
                        typeList[name] = "no type"
                        print(name, '  ', "no type")
                    #time.sleep(np.random.random()*4)

                except:
                    typeList[name] = "no type"
                    print(name, '  ',"no type")
                    save_pickle(typeList, 'typeList.data')

            else:
                typeList.append('no name')
                print(name, '  ', "no type")
            if counter%10 == 0:
                print("counter",counter)
        except:
            save_pickle(typeList, 'typeList.data')
            save_pickle(counter,'counterRecord.data')

    print(typeList)
    save_pickle(typeList,'typeList.data')





