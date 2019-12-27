#coding=utf-8
import os
import json
import codecs
import pandas as pd
from sqllib import mysqlite
import warnings
warnings.filterwarnings("ignore")

def InitDB(dbname = 'user.db'):
    conn = mysqlite(dbname)
    ## 初始化统计表
    if "data" not in conn.list_table():
        conn.creat_table("data",columns = {'id':'INTEGER PRIMARY KEY AUTOINCREMENT',"filename":"text","size":"int",\
          "rawdir":"text","tag":"text","name":"text","dist":"float",\
          "defect":"text","repair":"text"})
    print ("{} init successful".format(dbname))
    return conn


def GetFiles(dirname):
    filesname = []
    for root, dirs, files in os.walk(dirname, topdown=False):
        for name in files:
            path = os.path.join(root, name)
            size = os.path.getsize(path)
            filesname.append([name,os.path.realpath(root),size,path])
    filesname = pd.DataFrame(filesname,columns = ['filename','rawdir','size','path'])
    return filesname


def SaveFile(content,filename):
    with codecs.open(filename,'wb',encoding='gbk') as f:
        json.dump(content,fp=f,indent='\t')

def GetData(path = "data.json"):
    try:
        with codecs.open(path,'r',encoding='utf-8') as f:
            msg = json.loads(f.read())
        return msg
    except Exception as e:
        print ("{} 文件解析失败 : ".format(path,e))
        return {}

def GetTagName(path = "data.json"):
    with codecs.open(path,'r',encoding='utf-8') as f:
        msg =  ''
        for i in f.readlines():
            msg+=("\n"+i)
            if "name" in i:
                break
    msg = json.loads(msg+'}}]}')
    tag = msg['features'][0]['properties']['type']
    name = msg['features'][0]['properties']['name']
    return tag,name

def GetStatistics(df,tagfile = ''):
    res = df['tag'].value_counts().reset_index()
    res.columns = ['tag','count']
    res['pre'] = (res['count']/len(df)*100).round(1).astype(str)+"%"
    print ("\n\n****************标签统计结果 **************** \n{}".format(res))
    if len(tagfile):
        res.to_csv(tagfile,index=False,encoding ='gbk')
    print("\n")

def RepairFile(coordinates,distance_,dist):
    ## 文件修复
    ## 查找 N2-N 的距离是否小于2*dist, 因为删除点位迭代过程, 固使用for完成
    flag = True ## 修复标识位
    for index in distance_.index:
        ## 由于每次删除 异常坐标,所以index会发生变化，此处应该用iloc查询 N和N2点 ，而不能使用 loc
        iloc_index =coordinates.index.get_indexer([index])[0]
        N = coordinates.iloc[iloc_index-1,:2]
        N2 = coordinates.iloc[iloc_index+1,:2]
        d_ = pow((N2-N).pow(2).sum(),0.5) ## 计算N2-N的距离
        if d_>= 2*dist: ## 只要出现 N2-N 超过2*dist 即可认为该文件不可修复
            flag = False
            break
        else:
            coordinates = coordinates.drop(index = index)## 可修复, 去除 N1点
    return coordinates,flag
    
def JudgeFile(args):
    path,opt_args,index= args
    name = os.path.split(path)[-1]
    dist  = opt_args['dist'] ## 用户设定距离值
    geojson = GetData(path)
    coordinates = geojson['features'][0]['geometry']['coordinates'][0]
    coordinates = pd.DataFrame(coordinates) ## 获取 数据坐标 
    ## 此处计算相邻坐标 方法为 欧式距离
    distance = coordinates.iloc[:,:2].diff().pow(2).sum(axis=1).pow(0.5)
    distance_ = distance[distance>=dist] ## 检测 文件有无缺陷, 若有任何一个距离值超过 dist, 则认为该文件有缺陷
    defect = 'yes' if len(distance_) else "no"
    if defect=='yes':
        SaveFile(geojson,os.path.join(opt_args['defect'],name))
        ## 若文件为 有缺陷文件, 则进行文件修复
        coordinates,flag = RepairFile(coordinates,distance_,dist)
        if flag:
            ## 文件被修复
            geojson['features'][0]['geometry']['coordinates'][0] = coordinates.values.tolist()
            repair = 'yes'
            SaveFile(geojson,os.path.join(opt_args['repair'],name))
            print ("文件 : {} 修复成功 ".format(path))
        else:
            print ("文件 : {} 存在缺陷且不可修复 ".format(path))
            repair = 'error'
    else:
        SaveFile(geojson,os.path.join(opt_args['no_defect'],name))
        repair = 'no'
            
    return defect,repair,index