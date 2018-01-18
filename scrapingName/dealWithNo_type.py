import pandas  as pd
import os
import re
import requests
from bs4 import BeautifulSoup
import time
import  numpy as np
import gc
from sltools import save_pickle,load_pickle
import multiprocessing


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
            save_pickle(typeList, str(processName) + 'no_type_typeList.data')
            save_pickle(counter ,  str(processName) + 'counterRecord.data')

    print(typeList)
    save_pickle(typeList,str(processName)+'no_type_typeList.data')





if __name__ == "__main__":
    os.chdir("C:\\Users\\22560\\Documents\\iptv")
    name = pd.read_csv("no_type.csv")
    print("name is loaded!!")
    gc.collect()
    nameList = name.iloc[:,1].unique()
    data     = np.split(nameList,[int(.2*len(nameList)),
                                  int(.4*len(nameList)),
                                  int(.6*len(nameList)),
                                  int(.8*len(nameList))
                                  ])
    print(len(nameList))
    del name
    processList = []
    for i in range(5):
        process = multiprocessing.Process(target=scraping,name = str(i),args=(data[i],) )
        processList.append(process)

    for i in range(5):
        processList[i].start()

    for  i in range(5):
        processList[i].join()

    nameList = []
    typeList = []

    for i in range(5):
        type = load_pickle(str(i)+'no_type_typeList.data')
        nameList.extend(list(type.keys()))
        typeList.extend(list(type.values()))

    name_type = pd.DataFrame({'name':nameList,'type':typeList})
    name_type.to_csv('no_type_typeList.csv',index=False)