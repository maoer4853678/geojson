import sqlite3
import pandas as pd

def siplitlist(listx,n,axis = 0):
    '''
    listx: 待分组序列，类型可以是list，np.array,pd.Series...
    n: 若axis=0 n为组别个数,计算生成元素个数,若axis=1 n为每组元素个数,计算生成组别个数
    axis: 若axis=0 按照固定组别个数分组, 若axis=1 按照固定元素个数分组
    '''
    if axis ==0:
        a1,a2 = divmod(len(listx),n)
        N = [a1]*(n-a2)+[a1+1]*a2 if a1!=0 else [1]*len(listx)
        res = [data[sum(N[:index]):sum(N[:index+1])] for index in range(len(N))]
    else:
        N = int(len(listx)/n)+1
        res = [listx[i*n:(i+1)*n] for i in range(N) if len(listx[i*n:(i+1)*n])!=0]
    return res

class mysqlite():
    def __init__(self,dbname):
        self.conn = sqlite3.connect(dbname)
        self.cur = self.conn.cursor()

    def close(self):
        self.cur.close()
        self.conn.close()

    def execute(self,sql):
        if isinstance(sql,str):
            sql = [sql]
        result = [] 
        for s in sql:
            try:
                self.cur.execute(s)
                self.conn.commit()
                result.extend(self.cur.fetchall())
            except Exception as e:
                print ("*** Error : {}".format(e))
                self.conn.rollback()
        return result

    def list_table(self):
        sql = "SELECT name FROM sqlite_master WHERE type='table' order by name"
        tables = pd.read_sql_query(sql, con = self.conn)['name'].tolist()
        return tables
    
    def show_schema(self,tablename):
        sql = "PRAGMA table_info(%s)"%(tablename.lower())
        schema = pd.read_sql_query(sql, con = self.conn)
        return schema
    
    def alter_table(self,tablename,action,col):
        for i in col.items():
            sql ='alter table %s ' %tablename+"%s %s %s"%(self.actions[action],i[0],i[1])
        self.execute(sql)
    
    def creat_table(self,tablename,columns,perkey = None,default = {}):
        u'''
        创建数据表
        tablename : 数据表名称 [字符串]
        col : 创建表需要的字段配置 [字典] ，key为字段名称，value是 字段类型
        perkey : 创建表的主键
            [字符串] 可以是单一列
            [列表] 多列的列表
        foreign : 创建外键 [字典] ，key为字段名称，value是 外链表名(主键)
        auto : 设置主键为自增长 [布尔]
        default : 设置字段默认值 [字典] ，key为字段名称，value是 字段默认值
        '''
        if tablename in self.list_table():
            print (u'数据表创建失败: %s已存在'%(tablename))
        else:
            if perkey!= None:
                if type(perkey)==type([]):
                    perkey = ','.join(perkey)
                Psql = ',PRIMARY KEY (%s)' % (perkey)
            else:
                Psql =''
            defs = {'m':" default '%s'",'s':" default('%s')",'p':" default('%s')",'o':" default(%s)"} 
            for key in default.keys():
                columns[key] = columns[key]+ defs[self.enginetype]%default[key]
                   
            sql = ','.join(map(lambda i:"%s %s"%(i[0],i[1]),columns.items()))
            sql = '''CREATE TABLE %s (%s%s) ''' % (tablename,sql,Psql)
            self.execute(sql)


    def creat_table_from_df(self,tablename,df):
        dtypes = {"object":"text","int32":"integer","int64":"integer",\
                "float64":"REAL","datetime64[ns]":"timestamp"}
        cols = df.dtypes.astype(str).map(dtypes).to_dict()
        self.creat_table(tablename,cols)

    def colcom(self,g):
        sql = ' UNION ALL SELECT '+','.join(["'%s'" for i in range(len(g.index))]) % (tuple(g.tolist()))
        return sql
    
    def insert_df(self,tablename, rdf):
        columns = rdf.columns
        dfs = siplitlist(rdf,500,axis=1)
        sqls = []
        for df in dfs:
            col = ','.join(["%s" for i in range(len(columns))]) % (tuple(columns))
            col1 = ''.join(df.apply(self.colcom,axis = 1))
            col1 = col1.replace("UNION ALL SELECT",'SELECT',1)
            sql = "insert INTO %s (%s) %s;" % (tablename,col,col1)
            sqls.append(sql)
        self.execute(sqls)
    
    def show_df(self,tablename,columns = "*",condition = '',count=-1):
        u'''
        获取数据
        tablename : 数据表名称 [字符串]
        columns : 筛选字段名称
            默认是 * 全部字段
            [] 部分字段列表
            [字符串] 单个字段或者部分字段
    
        condition : 条件 [字符串] 默认是 '' 无条件 ，若有筛选条件，输入条件字符串
        tablename : 数据表名称 [字符串]
        df [一维/二维 向量/列表]:
            DataFrame: 要插入的数据DataFrame，df.columns需与数据表的字段配置保持一致
            Series: 要插入的数据Series，df.index需与数据表的字段配置保持一致
            [[]]: 要插入的数据[[]]，df的列数需与数据表的字段个数一致
            []: 要插入的数据[]，df的长度需与数据表的字段个数一致
        '''
        if type(columns) == type([]):
            columns = ','.join(columns)
        sql = 'select %s from %s '% (columns,tablename)
        if condition!="":
            if "where" not in condition.lower() :
                condition = "WHERE "+condition
            sql += " "+condition    
        if count>=0:
            sql += " LIMIT %d"%count

        df = pd.read_sql_query(sql,self.conn)
        return df