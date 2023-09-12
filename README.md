# mysql2yasdb

## **主要功能说明：**

1. **读取MySQL数据库内的对象生产YashanDB的元数据创建SQL。**包括表、约束、默认值、自增序列、主键、外键、普通索引、视图。（暂不包含存储过程、自定义函数、触发器）
2. **将MySQL数据库内的表数据迁移到YashanDB中。**支持以表模式、库模式迁移。支持模式对应、并行迁移、批量处理、指定排除表、指定表的过滤条件等配置参数。

## **工具使用说明：**

### 1、数据库用户权限：用于连接MySQL数据库的用户，需要授予如下MySQL系统表的查询权限

```mysql
 information_schema.tables
 information_schema.columns
 information_schema.key_column_usage
 information_schema.views
 information_schema.triggers
```

### 2、设置环境变量

```shell
export MYSQL2YASDB_HOME=/xx/yy/mysql2yasdb  ----工具包mysql2yasdb-x.x.x.zip解压后的根目录mysql2yasdb，根据部署环境提供真实路径
export PATH=$PATH:$MYSQL2YASDB_HOME
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$MYSQL2YASDB_HOME/lib ${MYSQL2YASDB_HOME}为工具包解压后的根目录
```

​工具依赖于如下lib包，如果工具运行过程中报lib包相关错误，可使用与目标库YashanDB版本匹配的lib文件进行替换。

```linux
libyascli.so
libyas_infra.so.0
libcrypto.so.1.1
libzstd.so.1
```

### 3、工具使用帮助：安装包解压后，执行命令./mysql2yasdb -h可获取使用帮助，使用帮助示例如下

```shell
全局选项:
-h, --help     显示帮助信息
-v, --version  显示程序版本号
-c, --config   指定DB配置信息文件
-d, --data     仅同步表数据,此参数开启时,不生成ddl文件
●用法示例1:     直接执行,使用当前目录下的db.ini配置文件获取程序运行时的配置信息,导出对象ddl
./mysql2yasdb 

●用法示例2:     使用自定义配置文件xxx.ini,导出对象ddl
./mysql2yasdb -c xxx.ini   或 ./mysql2yasdb --config=xxx.ini

●用法示例3:     使用当前目录下的db.ini配置文件,并进行表数据的同步,但不生成ddl文件
./mysql2yasdb -d
```

### 4、配置文件说明：mysql2yasdb解压目录下db.ini文件为工具参数配置文件，其中参数说明如下

```ini
[mysql]
host=192.168.3.180                      #mysql主机IP地址
port=3306                               #mysql访问端口
database=test                           #默认访问的database，当按tables导出时,导出此database下面的表
username=yashan                         #mysql访问用户名，需授予information_schema下相关系统表访问权限
password=yashan123                      #mysql访问用户密码

#tables=table1,table2                   #需迁移的mysql表名称，和参数schemas不能同时配置
schemas=db1,db2,db3                     #需迁移的databases的名称，和参数tables不能同时配置
#exclude_tables=table3,table4           #迁移过程中需排除的表名称，schemas配置多个时，多个schemas下面的此名称的表都不导出/数据同步
#parallel=1                             #并发度，值为N时表示同时并发迁移N个表，表较多时建议加大此参数可以提升速度,默认值1，取值范围[1-8]
#parallel_per_table=1                   #表内并行度，值为N时表示同一张表开启N个并行同步数据，表较大时建议加大此参数可以提升,默认值1，取值范围[1-8]
#batchSize=1000                         #批次大小，值为N时表示一次事务处理N行数据，默认值1000
#query=where create_date < '2022-01-11 00:00:00'  #设置查询条件,会对所有要同步的表都加上此条件

[yashandb]
host=192.168.3.180                      #YahsanDB主机IP地址
port=1688                               #YashanDB访问端口
username=yashan                         #YashanDB访问用户名，按表导入时，导入到此用户下
password=yashan123                      #YashanDB访问用户密码
remap_schemas=yashan,yashan,yashan      #迁移至YashanDB的目标用户名称，当和参数schemas一起配置时，它的值需要和参数schemas的值一一对应，schemas第N个值对应到remap_schemas第N个值。当和tables一起配置时，只取remap_schemas的第一个值。

```
