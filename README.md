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
export MYSQL2YASDB_HOME=/xx/yy/mysql2yasdb  ----工具包mysql2yasdb-xxxx-linux-x86_64.tar.gz解压后的根目录mysql2yasdb-xxxx，根据部署环境提供真实路径
export PATH=$PATH:$MYSQL2YASDB_HOME
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$MYSQL2YASDB_HOME/lib ${MYSQL2YASDB_HOME}为工具包解压后的根目录
```

工具依赖于如下lib包，如果工具运行过程中报lib包相关错误，可使用与目标库YashanDB版本匹配的lib文件进行替换。

```linux
libyascli.so
libyas_infra.so.0
libcrypto.so.1.1
libzstd.so.1
```

### 3、工具使用帮助：安装包解压后，执行命令./mysql2yasdb -h可获取使用帮助，使用帮助示例如下

```shell
Usage: mysql2yasdb <command> [flags]

mysql2yasdb is a tool for synchronizing data from MySQL to YashanDB.

Flags:
  -h, --help                          Show context-sensitive help.
  -v, --version                       Show version.
  -c, --config="./config/m2y.toml"    Configuration file.

Commands:
  sync      Sync data from MySQL to YashanDB.

  export    Export DDLs from MySQL.

Run "mysql2yasdb <command> --help" for more information on a command.
```

- `-h`参数显示工具帮助信息
- `-v`参数显示工具版本信息
- `-c`参数指定工具配置文件，默认配置文件为`{M2Y_HOME}/config/m2y.toml`



mysql2yasdb工具有两条子命令：

- `export`命令用于导出Mysql数据库的DDL到`{M2Y_HOME}/export`目录下
- `sync`命令用于直接将Mysql数据库的指定表的数据导入到YashanDB数据库中

`export`和`sync`子命令的数据库连接信息和表信息均由工具配置文件指定

### 4、配置文件说明：{M2Y_HOME}/config/m2y.toml文件为工具参数配置文件，其中参数说明如下

```ini
log_level = "debug"							#工具的日志级别
[mysql]
host="192.168.3.180"                        #mysql主机IP地址
port=3306                                   #mysql访问端口
database="test"                             #默认访问的database，当按tables导出时,导出此database下面的表
username="yashan"                           #mysql访问用户名，需授予information_schema下相关系统表访问权限
password="yashan123"                        #mysql访问用户密码

#tables=["table1","table2"]                 #需迁移的mysql表名称，和参数schemas不能同时配置
schemas=["db1","db2","db3"]                 #需迁移的databases的名称，和参数tables不能同时配置
#exclude_tables=table3,table4               #迁移过程中需排除的表名称，schemas配置多个时，多个schemas下面的此名称的表都不导出/数据同步
#parallel=1                                 #并发度，值为N时表示同时并发迁移N个表，表较多时建议加大此参数可以提升速度,默认值1，取值范围[1-8]
#parallel_per_table=1                       #表内并行度，值为N时表示同一张表开启N个并行同步数据，表较大时建议加大此参数可以提升,默认值1，取值范围[1-8]
#batchSize=1000                             #批次大小，值为N时表示一次事务处理N行数据，默认值1000
#query="where create_date < '2022-01-11 00:00:00'"  #设置查询条件,会对所有要同步的表都加上此条件

[yashandb]
host="192.168.3.180"                        #YahsanDB主机IP地址
port=1688                                   #YashanDB访问端口
username="yashan"                           #YashanDB访问用户名，按表导入时，导入到此用户下
password="yashan123"                        #YashanDB访问用户密码，建议密码串用双引号引起来，避免复杂密码识别有误
remap_schemas=["yashan","yashan","yashan"]  #迁移至YashanDB的目标用户名称，当和参数schemas一起配置时，它的值需要和参数schemas的值一一对应，schemas第N个值对应到remap_schemas第N个值。当和tables一起配置时，只取remap_schemas的第一个值
```

### 5、最佳实践

#### 前置准备：

- 一个需要导出数据的Mysql数据库
- 一个用于导入数据的YashanDB数据库

#### 导出Mysql数据库指定表的DDL：

1. 编辑mysql2yasdb工具配置文件，使用满足工具要求的用户连接数据库，并指定要导出DDL的schema或表格
2. 执行 `./mysql2yasdb export`命令导出DDL

#### YashanDB数据库建表：

1. 使用前置过程中导出的DDL在YashanDB数据库中创建表、索引、约束等

>直接使用导出的DDL在YashanDB数据库中执行可能会报错。
>
>使用`yasql ***/***  -f -e  table_ddl.sql > table_ddl.log`命令可以查看建表语句中具体报错内容，如有报错需要手动修改DDL后重新执行。

#### 同步数据到YashanDB数据库：

1. 需要修改配置文件，指定需要导出的YashanDB数据库的连接信息和导出的Schema名，如果前置过程中已经指定，无需重复指定
2. 执行 `./mysql2yasdb sync`命令同步数据到YashanDB数据库。

>同步过程中会在终端打印同步过程，如有报错信息，需要在同步完成后根据报错信息定位错误原因并重新同步失败的表数据