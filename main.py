import os
import json
import codecs
import pandas as pd
from sqllib import siplitlist,mysqlite
import sys
import getopt
from multiprocessing import Pool,cpu_count
import warnings
warnings.filterwarnings("ignore")

def MakeDir(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)

def OptsTransfrom(opts):
    opts = dict(opts)
    if "-h" in opts:
        print ('''
    short输入量: 
    -i : 原始未处理文件所在目录 , **必填值**
    -d : 用户指定dist, 用以识别文件是否存在缺陷, 若-a 未开启 则为**必填值**
    -t : 标签筛选,支持多标签筛选,多个标签用 , 连接 ,输入实例: -t tag1,tag2 代表仅筛选tag1和tag2的所有文件, 默认全选
    -o : 处理后的文件输出到目录 , 默认为 output
    -c : 文件处理采用多核并行计算, 指定使用CPU个数, 默认为全部CPU
    
    long输入量: 
    tagfile : 标签统计结果存文件名称, 默认为空, 则不存储, 若输入值, 则以名称(带文件后缀)此存储文件
    defect : 开启 -s后 有缺陷文件存储目录, 默认为 defect
    no_defect :  开启 -s后 无缺陷文件存储目录 , 默认为 no_defect
    repair : 有缺陷的文件 经过修复后存储的目录, 默认为 repair
    
    开关量: 
    -h : 查看参数帮助
    -v : 开启模糊搜索, 开启模糊搜索, 则按照模糊匹配要筛选的标签，反之为精确匹配
    -a : 开启标签统计模式，只统计各个 tag 的分布数量, 不做处理文件
    -s : 开启区分存储模式，有无缺陷的文件是否区分存储 
    -f : 开启强制文件处理模式，若开启该模式, 则强制处理全部符合要求的所有文件, 否则, 若dist相同, 之前处理过的文件则不予处理
        
    ''')
        sys.exit()
    if "-i" not in opts:
        print ("程序错误 :  -i 请输入原始未处理文件所在目录")
        sys.exit()
    if "-a" not in opts and "-d" not in opts:
        print ("程序错误 : 文件处理模式下 需要指定dist值")
        sys.exit()
    opt_args = {}
    opt_args['input'] = opts["-i"]
    opt_args['output'] = 'output' if "-o" not in opts else opts["-o"]
    if '-s' in opts:
        for k in ['defect','no_defect']:
            opt_args[k] = os.path.join(opt_args['output'],\
                k if "--{}".format(k) not in opts else opts["--{}".format(k)])
    else:
        opt_args['defect'] = opt_args['no_defect'] = opt_args['output']
    opt_args['repair'] = os.path.join(opt_args['output'],'repair' if "--repair" not in opts else opts["--repair"])
    opt_args['dist'] = None if "-d" not in opts else float(opts["-d"])
    opt_args['cpu'] = 1 if "-c" not in opts else int(opts["-c"]) ## cpu_count() 为全部CPU, 这里可以根据自习惯 更改cpu默认配置
    opt_args['tag'] = [] if "-t" not in opts else opts["-t"].split(",") ## 这里可以根据自习惯 更换 分隔符
    opt_args['vague'] = '-v' in opts
    opt_args['sta'] = '-a' in opts
    opt_args['force'] = '-f' in opts
    opt_args['tagfile'] = '' if ("--tagfile" not in opts) or (not opt_args['sta']) else opts["--tagfile"]
    
    if not opt_args['sta']:
        ## 文件处理模式下, 生成文件夹
        for k in ['defect','no_defect','repair']:
            MakeDir(opt_args[k])
    
    return opt_args

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
            filesname.append([name,root,size,path])
    filesname = pd.DataFrame(filesname,columns = ['filename','rawdir','size','path'])
    return filesname

def SaveFile(content,filename):
    with codecs.open(filename,'wb',encoding='gbk') as f:
        json.dump(content,fp=f,indent=4)

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
    print ("\n\n****************标签统计结果 **************** \n{}".format(res))
    if len(tagfile):
        res.to_csv(tagfile,index=False)

def RepairFile(coordinates,distance_,dist):
    ## 文件修复
    ## 查找 N2-N 的距离是否小于2*dist, 因为删除点位迭代过程, 固使用for完成
    flag = True ## 修复标识位
    for index in distance_.index:
        ## 由于每次删除 异常坐标,所以index会发生变化，此处应该用iloc 而不能使用 loc
        iloc_index =coordinates.index.get_indexer([index])[0]
        N = coordinates.iloc[iloc_index-1,:2]
        N2 = coordinates.iloc[iloc_index+1,:2]
        d_ = pow((N2-N).pow(2).sum(),0.5) ## 计算N2-N的距离
        if d_>= 2*dist: ## 只要出现 N2-N 不超过2*dist即可认为该文件不可修复
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

class Geo():
    def __init__ (self,dbname = 'user.db',opt_args ={},pool = None):
        self.opt_args = opt_args
        self.pool = pool
        self.dbname = dbname ## 用于存储已处理过的文件信息, 可根据用户习惯更改名称
        self.conn = InitDB(dbname)
        self.perkey = ['filename','rawdir','size'] ## 该主键作为 识别增量文件的唯一标识
        self.batch = 1000 ## 单批运行最大 文件数量
        
    def RecognitionIncrement(self):
        ## 第一步: 按照指定 rawdir 对比数据库 增量解析文件入库
        rawdir = self.opt_args['input']
        files = GetFiles(rawdir) ## 递归获取指定目录下的所有文件信息
        self.total = self.conn.show_df("data")  ## 提取已处理过的所有文件信息
        Increment  = pd.merge(files,self.total,on = self.perkey,how = 'left')
        Increment = Increment[Increment.tag.isnull()] ## 识别增量文件信息
        Increment.index = range(len(Increment))
        print ("识别增量文件 : {} 个".format(len(Increment)))
        return Increment
    
    def AnalysisIncrement(self,Increment):
        ## 第二步: 解析增量文件
        result = self.pool.map(GetTagName,Increment['path'])
        result = pd.DataFrame(result,columns = ['tag','name'])
        if self.opt_args['dist']:
            result['dist'] = self.opt_args['dist']
        Increment_ = pd.concat([Increment[self.perkey],result],axis=1)
        self.conn.insert_df('data',Increment_)
        id_ = self.conn.execute("select max(id) from data")[0][0]
        Increment_['id'] = range(id_+1-len(Increment_),id_+1) ## 补充 id 至 Increment_
        print ("解析增量文件 : {} 个 ".format(len(Increment_)))
        return Increment_
        
    def FilterIncrement(self,df):
        ## 第三步: 根据tag筛选文件
        tags = self.opt_args['tag']
        vague = self.opt_args['vague']
        if len(tags):
            if vague:
                df = df[df['tag'].str.contains("|".join(tags))]
            else:
                df = df[df['tag'].isin(tags)]
        return df

    def AnalysisData(self,data):
        ## 第四步: 缺陷识别文件识别 ： 支持 增量模式 和 全量模式
        attribute_cols = ['name','tag']  ## 文件的静态属性信息字段名称
        attribute = data[self.perkey+attribute_cols].drop_duplicates(self.perkey) ## 文件的静态属性信息
        perkeys = data[self.perkey].drop_duplicates() ## 未处理文件
        perkeys['dist'] = self.opt_args['dist']
        data = data.dropna(subset = ['dist'])
        print (data)
        print (perkeys)
        untreated = pd.merge(perkeys,data,on = perkeys.columns.tolist() , how = 'left')
        untreated.loc[untreated['id'].isnull(),'db'] = 'insert'
        untreated['db'] = untreated['db'].fillna('update')
        untreated.index = range(len(untreated))
        if not self.opt_args['force'] :
            ## 全量模式 是处理全部 untreated
            untreated = untreated[untreated['defect'].isnull()] ## 增量模式 只处理 defect 为空的那部分
        print ("\n\n文件处理模式, 本次任务处理文件总数 : {} 个".format(len(untreated)))
        judge_args = list(map(lambda x,y,i:[os.path.join(x,y),self.opt_args,i],\
                untreated['rawdir'],untreated['filename'],untreated.index))
        
        task_args = siplitlist(judge_args,self.batch,axis=1)
        for progress,task in enumerate(task_args): ## 分批解析文件
            result = self.pool.map(JudgeFile,task)
            result = pd.DataFrame(result,columns = ['defect','repair','index'])
            result = result.set_index('index')
            untreated.loc[result.index,['defect','repair']] = result[['defect','repair']] ## 修正untreated 中结果
            untreated_ = untreated.loc[result.index,:] ## 筛选本批次结果中的 untreated
            ## insert
            insert_df = untreated_[untreated_['db']=='insert']
            if len(insert_df):
                insert_df = insert_df.drop(['id','db']+attribute_cols,axis=1)
                insert_df = pd.merge(insert_df,attribute,on = self.perkey) ## 给结果添加静态属性
                self.conn.insert_df("data",insert_df)
            ## update
            update_df = untreated_[untreated_['db']=='update']
            if len(update_df):
                for id_,defect,repair in update_df[['id','defect','repair']].values:
                    self.conn.execute("update data set defect = '{}' , repair = '{}'  where id = {} ".\
                                          format(defect,repair,id_))
            print ('更新数据库: 插入记录数 {} , 更新记录数: {}'.format(len(insert_df),len(update_df)))
            ## 显示进度条
            print ("当前处理进度 : {:.1f}%".format(float(progress+1)/len(task_args)*100))
        print ("全部文件处理完成\n\n")
        return untreated

    def NonDefect(self,data,untreated):
        ## 第五步: 针对此次符合要求的所有数据进行 无缺陷文件 统计
        result = data.append(untreated)
        result = result[result['dist']==self.opt_args['dist']]\
            .drop_duplicates(self.perkey,keep = 'last')
        result_ = result[result['defect']=='no']
        print ('****************无缺陷文件统计****************')
        print ("无缺陷文件数量 : {} 个, 占比 : {:.1f}%".format(len(result_),float(len(result_))/len(result)*100))

    def close(self):
        self.conn.close()
        self.pool.close()
        sys.exit()
    
    def run(self):
        Increment = self.RecognitionIncrement()
        Increment_ = self.AnalysisIncrement(Increment)
        data = self.total.append(Increment_).drop_duplicates()
        data = self.FilterIncrement(data) ## 符合此次需要处理要求 的所有的文件 
        GetStatistics(data,self.opt_args['tagfile'])
        if self.opt_args['sta']: ## 如果是标签统计模式, 则处理止于此
            self.close()
        untreated =  self.AnalysisData(data) ## 缺陷识别
        self.NonDefect(data,untreated) ## 无缺陷文件比例
        self.close()


if __name__ == "__main__":   
    opts, args = getopt.getopt(sys.argv[1:], "i:t:o:d:c:vashf", \
                               ["tagfile=","defect=", "no_defect=", "repair="])
    opt_args = OptsTransfrom(opts)
    pool= Pool(opt_args['cpu'])
    geo = Geo(dbname = 'user.db',opt_args =opt_args,pool = pool)
    geo.run()
    
#    ## 第四步: 处理数据
#    comman = 'Statistics'
#    tagfile = ''
#    args['tag_sta'] = tagfile
#    funcs = {'tag_sta':GetStatistics}
#    funcs[comman](**args)
#    
    
    
    
    
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    