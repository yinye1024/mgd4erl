%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. 六月 2021 19:07
%%%-------------------------------------------------------------------
-module(mongo_api_test).
-author("yinye").
-include_lib("yyutils/include/yyu_comm.hrl").
-include_lib("eunit/include/eunit.hrl").


%% API functions defined
-define(Collection, <<"test_api">>).
-define(PoolId, mg_test_pool_Id).


%% ===================================================================================
%% API functions implements
%% ===================================================================================
api_test_() ->
  yyu_logger:start(),
  [{setup,
    fun start_suite/0,
    fun stop_suite/1,
    fun (_SetupData) ->
      [
        {foreach,
          fun start_case/0,
          fun stop_case/1,
          [
            fun ensure_indexes/1,
            fun incr_and_get_autoId/1,
            fun insert_and_update/1,
            fun batch_insert/1,
            fun cursor_find_batch/1
          ]
        }
      ]
    end}].


start_suite() ->
  yymg_mongo_api:ensure_sup_started(),
  ?LOG_INFO({"api test start ==================="}),
  PbPid = priv_new_conn_pool(?PoolId),
  {?PoolId,PbPid}.
priv_new_conn_pool(PoolId)->
  McCfg = yymg_mongo_client_cfg:new_auth_cfg("192.168.43.29", 27017,<<"test_db">>,30000,{"mongo-admin", "mgadmin@123456"}),
  PoolSize = 1,
  PoolPid = yymg_mongo_api:new_pool(PoolId,PoolSize,McCfg),
  PoolPid.

stop_suite({_PoolId,PbPid}) ->
  yymg_mongo_api:stop_pool(PbPid),
  ?LOG_INFO({"api test end ======================"}),
  ?OK.


start_case()->
  ?LOG_INFO({"start case ==================="}),
  %% 清理数据
  priv_delete_all(?PoolId,?Collection),
  {?PoolId,?Collection}.

stop_case({PoolId,Collection})->
  %% 清理数据
  priv_delete_all(PoolId,Collection),
  ?LOG_INFO({"stop case ==================="}),
  ?OK.

ensure_indexes({PoolId,Collection})->
  IndexSpec = {<<"key">>,{name,1},<<"unique">>,false,<<"dropDups">>,false,name,<<"index_name">>},
  yymg_mongo_api:ensure_indexes(PoolId,{Collection, IndexSpec}),
  [].

incr_and_get_autoId({PoolId,_Collection})->
  AutoId_1 = yymg_mongo_api:incr_and_get_autoId(PoolId,autoId),
  AutoId_2 = yymg_mongo_api:incr_and_get_autoId(PoolId,autoId),
  [
    ?_assertMatch(1, AutoId_2 - AutoId_1)
  ].

insert_and_update({PoolId,Collection})->
  %% 插入新数据
  {Id,Name,Age} = {1,"yyname",18},
  NewPojo = mongo_api_test_pojo:new_one({Id,Name,Age}),
  yymg_mongo_api:insert(PoolId,{Collection,NewPojo}),

  %% 获取新插入的数据
  {QueryMap, Projector} = {#{'_id'=>Id},#{}},
  {?OK, FromDb_1} = yymg_mongo_api:find_one(PoolId,{Collection, QueryMap, Projector}),

  %% 更新并获取新数据
  UpdateAge = 19,
  UpdatePojo = mongo_api_test_pojo:set_age(UpdateAge,FromDb_1),
  IsUpsert = ?TRUE,
  yymg_mongo_api:update(PoolId,{Collection, UpdatePojo, IsUpsert}),
  {?OK, FromDb_2} = yymg_mongo_api:find_one(PoolId,{Collection, QueryMap, Projector}),

  [
    ?_assertMatch(Age,mongo_api_test_pojo:get_age(FromDb_1)),
    ?_assertMatch(UpdateAge,mongo_api_test_pojo:get_age(FromDb_2))
  ].

batch_insert({PoolId,Collection})->

  BatchSize = 10,
  {NamePrefix,Count} = {"yyname_",BatchSize},
  PojoList = mongo_api_test_pojo:new_list(NamePrefix,Count),
  yymg_mongo_api:batch_insert(PoolId,{Collection, PojoList}),
  AllList = priv_find_all(PoolId,Collection),
  [
    ?_assertMatch(BatchSize,erlang:length(AllList))
  ].

cursor_find_batch({PoolId,Collection})->
  TotalSize = 100,
  {NamePrefix,Count} = {"yyname_",TotalSize},
  PojoList = mongo_api_test_pojo:new_list(NamePrefix,Count),
  yymg_mongo_api:batch_insert(PoolId,{Collection, PojoList}),

  QueryMap = #{},
  TotalCount = yymg_mongo_api:get_count(PoolId,{Collection, QueryMap}),

  {Projector,Skip,BatchSize} = {#{},0,50},
  {CursorPid,FirstBatchList} = yymg_mongo_api:find_batch(PoolId,{Collection,{QueryMap,Projector,Skip,BatchSize}}),
  {?OK,NextBatchList_1} = yymg_mongo_api:next_batch(CursorPid),
  {?OK,NextBatchList_2} = yymg_mongo_api:next_batch(CursorPid),
  yymg_mongo_api:close(CursorPid),

  [
    ?_assertMatch(TotalSize,TotalCount),
    ?_assertMatch(BatchSize,erlang:length(FirstBatchList)),
    ?_assertMatch(BatchSize,erlang:length(NextBatchList_1)),
    ?_assertMatch(0,erlang:length(NextBatchList_2))
  ].


priv_find_all(PoolId,Collection)->
  QueryMap = #{},
  Projector = #{},
  {?OK, List} = yymg_mongo_api:find_list(PoolId,{Collection, QueryMap, Projector}),
  ?LOG_INFO({List}),
  List.

priv_delete_all(PoolId,Collection)->
  SelectMap = #{},
  yymg_mongo_api:delete(PoolId,{Collection, SelectMap}),
  ?OK.

