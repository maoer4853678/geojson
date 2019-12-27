# geojson
geojson格式文件 并行文件处理 和统计

# 脚本参数说明：

python main.py [OPTIONS]

OPTIONS (脚本参数):
    
##    short输入量:
    -i : 原始未处理文件所在目录 , **必填值**
    -d : 用户指定dist, 用以识别文件是否存在缺陷, 若-a 未开启 则为**必填值**
    -t : 标签筛选,支持多标签筛选,多个标签用 , 连接 ,输入实例: -t tag1,tag2 代表仅筛选tag1和tag2的所有文件, 默认全选
    -o : 处理后的文件输出到目录 , 默认为 output
    -c : 文件处理采用多核并行计算, 指定使用CPU个数, 默认为1

##   long输入量:
    tagfile : 标签统计结果存文件名称, 默认为空, 则不存储, 若输入值, 则以名称(带文件后缀)此存储文件
    defect : 开启 -s后 有缺陷文件存储目录, 默认为 defect
    no_defect :  开启 -s后 无缺陷文件存储目录 , 默认为 no_defect
    repair : 有缺陷的文件 经过修复后存储的目录, 默认为 repair

##    开关量:
    -h : 查看参数帮助
    -v : 开启模糊搜索, 开启模糊搜索, 则按照模糊匹配要筛选的标签，反之为精确匹配
    -a : 开启标签统计模式，只统计各个 tag 的分布数量, 不做处理文件
    -s : 开启区分存储模式，有无缺陷的文件是否区分存储
    -f : 开启强制文件处理模式，若开启该模式, 则强制处理全部符合要求的所有文件, 否则, 若dist相同, 之前处理过的文件则不予 处理

# 脚本使用案例：

##   1. 指定 输入目录为 data文件夹 , 标签统计模式，不做缺陷文件识别和修复
        1.1 查看全部标签  , 使用 -a 开关
            python main.py -i data -a
        1.2 查看 步行 标签 (默认为 精确匹配)
            python main.py -i data -t 步行 -a
        1.3 查看 步行 和 骑行 标签
            python main.py -i data -t 步行,骑行 -a
        1.4 查看 含有 步 关键字的标签 , 使用 -v 开关 启动模糊匹配
            python main.py -i data -t 步 -a -v
        1.4 查看 含有 步,骑, 关键字的标签 
            python main.py -i data -t 步,骑 -a -v 
        1.5 查看全部标签, 并将标签统计结果存储 为 tag.csv文件 (注意: 不开启标签统计模式, 同样可以存储)
            python main.py -i data --tagfile=tag.csv -a

##    2. 指定 输入目录为 data文件夹 , 指定输出目录为 result , 指定dist值为 0.004 
        2.1 不指定输出目录名称, 采用默认名称
            python main.py -i data -d 0.004 
        2.1 不区分区分有无缺陷文件的存储目录 (默认不区分)
            python main.py -i data -o result -d 0.004
        2.2 区分有无缺陷文件的存储目录 , 使用 -s 开关
            python main.py -i data -o result -d 0.004 -s
        2.3 区分有无缺陷文件的存储目录, 且指定 有缺陷文件存储目录为 dir1 , 无缺陷文件存储目录为 dir2 , 修复后文件存储目录为 dir3
            python main.py -i data -o result -d 0.004 -s --defect=dir1 --no_defect=dir2 --repair=dir3
            (注意: dir1 和 dir2 文件数量 等于 增量处理文件总和, dir3是 dir1的 文件子集)

##    3. 并行计算
       2.1  采用单进程处理 (目前默认就是单进程)
            python main.py -i data -d 0.004 
       2.2  采用4核处理
            python main.py -i data -d 0.004 -c 4
    
##    4. 增量处理文件
       对于同一文件(输入目录,文件名称和文件大小假设都一致, 则认为是同一文件), 默认情况下, 若同一dist值之前处理过, 则可不做处理, 只此次任务的处理增量文件
       4.1  即使同一文件, dist相同, 强制重新处理 , 使用 -f 开关
            python main.py -i data -d 0.004 -f
       注意 : 增量文件的识别 只和 输入目录 , 文件名称 , 文件大小 和 dist 有关, 若更换 输出目录名称存储, 同样需要启动 -f 开关