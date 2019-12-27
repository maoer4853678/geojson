#coding=utf-8
import os
import pandas as pd
from sqllib import siplitlist
import warnings
import sys
warnings.filterwarnings("ignore")
from common import *

class Geo():
    def __init__ (self,dbname = 'user.db',opt_args ={},batch = 1000,pool = None):
        self.opt_args = opt_args
        self.pool = pool
        self.dbname = dbname ## 用于存储已处理过的文件信息, 可根据用户习惯更改名称
        self.conn = InitDB(dbname)
        self.perkey = ['filename','rawdir','size'] ## 该主键作为 识别增量文件的唯一标识
        
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
        untreated = pd.merge(perkeys,data,on = perkeys.columns.tolist() , how = 'left')
        untreated.loc[untreated['id'].isnull(),'db'] = 'insert'
        untreated['db'] = untreated['db'].fillna('update')
        untreated.index = range(len(untreated))
        if not self.opt_args['force'] :
            ## 全量模式 是处理全部 untreated
            untreated = untreated[untreated['defect'].isnull()] ## 增量模式 只处理 defect 为空的那部分

        judge_args = list(map(lambda x,y,i:[os.path.join(x,y),self.opt_args,i],\
                untreated['rawdir'],untreated['filename'],untreated.index))
        
        task_args = siplitlist(judge_args,self.opt_args['batch'],axis=1)
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
            print ("Batch No.{} 当前处理进度 : {:.1f}%".format(progress+1,float(progress+1)/len(task_args)*100))
        ##  "全部文件处理完成\n\n"
        return untreated

    def NonDefect(self,data,untreated):
        ## 第五步: 针对此次符合要求的所有数据进行 无缺陷文件 统计
        result = data.append(untreated)
        result = result[result['dist']==self.opt_args['dist']]\
            .drop_duplicates(self.perkey,keep = 'last')    
        result_ = result[result['defect']=='no']      
        print ('\n****************无缺陷文件统计****************')
        print ("无缺陷文件数量 : {} 个, 文件总数 : {} 个, 占比 : {:.1f}%".format(len(result_),len(result),float(len(result_))/len(result)*100))

    def close(self):
        self.conn.close()
        self.pool.close()
        sys.exit()
    
    def run(self):
        Increment = self.RecognitionIncrement()
        Increment_ = self.AnalysisIncrement(Increment)
        data = self.total.append(Increment_).drop_duplicates()
        data = self.FilterIncrement(data) ## 符合此次需要处理要求 的所有的文件 
        if len(data)==0:
            print ("***警告: 未发现符合要求的处理文件, 请检查输入参数")
            return 
        data_ = data.drop_duplicates(self.perkey)
        GetStatistics(data_,self.opt_args['tagfile'])
        if self.opt_args['sta']: ## 如果是标签统计模式, 则处理止于此
            return
        untreated =  self.AnalysisData(data) ## 缺陷识别
        self.NonDefect(data,untreated) ## 无缺陷文件比例
        return 