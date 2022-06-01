# mgd4erl
=====

mongodb driver for erlang, base on OP_MSG,support bulk write, cursor.

mongo 驱动
-----

功能
        基于 OP_MSG
        mongo 版本 3.6+
        支持 bulk write
            bulk update
            bulk insert
        支持游标cursor
            每个游标会启动一个进程来管理
            超过时间进程会自动关闭，并释放游标
        支持连接池
            基于 pool boy 进程池
            每个链接对应一个进程
            超过30秒链接失败，会尝试重连


测试用例
        rebar3 eunit
        rebar3 eunit --module=mongo_api_test
        rebar3 eunit --module=mongo_client_test

如何使用
        查看测试用例
        mongo_api_test
            测试 yymg_mongo_api 接口
        mongo_client_test
            测试 yymg_mongo_client_mgr 接口


主要模块
        client
            mongo客户端，OP_MSG的实现
            可以基于 client 做自己的连接池
            对外接口
                yymg_mongo_client_mgr
            OP_MSG编解码
                yymg_mongo_client_proto
        cursor
            游标管理
            对外接口
                gs_yymg_mongo_cursor_mgr
        pool
            基于poolboy的连接池
            对外接口
                yymg_mongo_poolboy_mgr
        使用接口
            yymg_mongo_api
