erlang 的 mongodb 驱动, 基于 OP_MSG 协议, 支持 bulk write, cursor，***不支持master-slave***。

## 功能

1. 基于 OP_MSG  mongo 版本 3.6+

2. 支持 

    batch 操作，单次提交数据总量大小不能超过16M

    bulk 操作，每条记录不超过16M，但数据总量大小没限制

3. 支持游标cursor
   
    每个游标会启动一个进程来管理

    超过时间进程会自动关闭，并释放游标

4. 支持连接池
   
    基于 pool boy 进程池
   
    每个链接对应一个进程

    超过30秒链接失败，会尝试重连.

### 添加依赖

rebar3 文件rebar.config添加依赖

    {deps, [
      {mgd4erl, {git, "https://github.com/yinye1024/mgd4erl.git", {tag, "<Latest tag>"}}}
       ]
    }

 __Latest tag__ 是最新版本.

### 测试用例

1. 对应修改测试用例的数据库链接地址和端口，
2. 跑用例 
   修改 mongo_api_test 和 mongo_client_test 的数据库连接信息

  > rebar3 eunit 
  > 
  > rebar3 eunit --module=mongo_api_test
  > 
  > rebar3 eunit --module=mongo_client_test

### 如何使用

参考测试用例

1. mongo_api_test
    测试 yymg_mongo_api 接口 

2. mongo_client_test
    测试 yymg_mongo_client_mgr 接口

### 主要模块

1. client 目录， mongo客户端，OP_MSG的实现，可以基于 client 做自己的连接池。
   
   模块接口 yymg_mongo_client_mgr
   OP_MSG编解码实现 yymg_mongo_client_proto
   
   

2. cursor 目录，游标管理，实现分批查询。
   
   模块接口 gs_yymg_mongo_cursor_mgr   
   

3. pool 目录，基于poolboy的连接池实现。
   
   模块接口 yymg_mongo_poolboy_mgr  
   

4. 使用接口
   
   yymg_mongo_api，使用方法参考测试用例 test/api/mongo_api_test
   使用规范 参考 tpl/tpl_mongo_dao
   
   
   
