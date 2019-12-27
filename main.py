#coding=utf-8
import os
import time
import sys
import getopt
from multiprocessing import Pool,cpu_count
import warnings
warnings.filterwarnings("ignore")
from core import Geo

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
    opt_args['batch'] = 1000 if "-b" not in opts else int(opts["-b"])
    opt_args['cpu'] = 1 if "-c" not in opts else int(opts["-c"]) ## cpu_count() 为全部CPU, 这里可以根据自习惯 更改cpu默认配置
    opt_args['tag'] = [] if "-t" not in opts else opts["-t"].split(",") ## 这里可以根据自习惯 更换 分隔符
    opt_args['vague'] = '-v' in opts
    opt_args['sta'] = '-a' in opts
    opt_args['force'] = '-f' in opts
    opt_args['tagfile'] = '' if ("--tagfile" not in opts) or (not opt_args['sta']) else opts["--tagfile"]
    opt_args['dbname'] = 'user.db' if "--dbname" not in opts else os.path.splitext(opts["--dbname"])[0]+".db"
    
    if not opt_args['sta']:
        ## 文件处理模式下, 生成文件夹
        for k in ['defect','no_defect','repair']:
            MakeDir(opt_args[k])
    return opt_args

if __name__ == "__main__":   
    opts, args = getopt.getopt(sys.argv[1:], "i:t:o:d:b:c:vashf", \
                               ["tagfile=","defect=", "no_defect=", "repair=",'dbname='])
    opt_args = OptsTransfrom(opts)
    st = time.time()
    pool= Pool(opt_args['cpu'])
    geo = Geo(dbname = opt_args['dbname'],opt_args =opt_args,pool = pool)
    geo.run()
    et = time.time()
    print ("程序总计耗时 : {:.2f}s".format(et-st))
    geo.close()
    
    
    
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    