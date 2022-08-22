%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_mongo_poolboy_mgr).
-author("yinye").

-include_lib("yyutils/include/yyu_comm.hrl").


%% API functions defined
-export([ensure_sup_started/0, new_pool/3, stop_pool/1,ensure_indexes/2]).
-export([insert/2,batch_insert/2,
          update/2,bulk_update/2,update_selection/2,
          delete/2,batch_delete/2]).
-export([find_one/2,find_list/2,find_and_modify/2]).
-export([get_count/2,find_batch/2,next_batch/2,kill_cursor/2]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================

%% =======================  WorkMod 相关方法 ============================================================
%% yymg_mongo_poolboy_mgr:start_link().
ensure_sup_started()->
  yypb_poolboy_api:ensure_sup_started(),
  ?OK.

new_pool(PoolId,PoolSize,McCfg)->
  WorkerAgent = yypb_pb_worker_agent:new_pojo(yymg_mongo_pb_worker, yymg_mongo_pb_worker_data:new_pojo(McCfg)),
  WorkPid = yypb_poolboy_api:new_pool(PoolId,PoolSize,WorkerAgent),
  WorkPid.

stop_pool(WorkPid)->
  yypb_poolboy_api:stop_pool(WorkPid),
  ?OK.

ensure_indexes(PoolId,{_Collection,_IndexSpec}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:ensure_indexes/2,
  Result = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {?OK,Result}.

insert(PoolId,{_Collection,_InsertMap}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:insert/2,
  Result = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {?OK,Result}.

batch_insert(PoolId,{_Collection,_InsertMapList}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:batch_insert/2,
  Result = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {?OK,Result}.


update(PoolId,{_Collection,{_SelectMap,_UpdateMap},_IsUpsert}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:update/2,
  Result = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {?OK,Result}.


update_selection(PoolId,{_Collection,_SelectorMap,_UpdateMap}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:update_selection/2,
  Result = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {?OK,Result}.

bulk_update(PoolId,{_Collection,SelectedUpdateMapList,_IsUpsert}=Param) when is_list(SelectedUpdateMapList)->
  WorkFun = fun yymg_mongo_pb_worker:bulk_update/2,
  Result = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {?OK,Result}.


delete(PoolId,{_Collection, SelectorMap}=Param) when is_map(SelectorMap)->
  WorkFun = fun yymg_mongo_pb_worker:delete/2,
  Result = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {?OK,Result}.

batch_delete(PoolId,{_Collection, QueryMap_LimitList}=Param) when is_list(QueryMap_LimitList)->
  WorkFun = fun yymg_mongo_pb_worker:batch_delete/2,
  Result = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {?OK,Result}.


find_and_modify(PoolId,{_Collection,_QueryMap,_UpdateMap}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:find_and_modify/2,
  {?OK,Result}  = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {?OK,Result}.

%% return all fields of found record if Projector = #{}
find_one(PoolId,{_Collection,_QueryMap,_Projector}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:find_one/2,
  Result= yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  Result.
%% return all fields of found record if Projector = #{}
find_list(PoolId,{_Collection,_QueryMap,_Projector}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:find_list/2,
  {?OK, ItemList}= yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {?OK, ItemList}.


%% ==================== 批量查找 要管理好 cursorId (开始)==========================================

get_count(PoolId,{_Collection,_QueryMap}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:get_count/2,
  Count = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  Count.

%% return all fields of found record if Projector = #{}
find_batch(PoolId,{_Collection,{_QueryMap,_Projector,_Skip,_BatchSize}}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:find_batch/2,
  {CursorId,FirstBatch} = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {CursorId,FirstBatch}.

next_batch(PoolId,{_Collection,_CursorId,_BatchSize}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:next_batch/2,
  {Result,BatchList} = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {Result,BatchList}.

kill_cursor(PoolId,{_Collection,_CursorId}=Param)->
  WorkFun = fun yymg_mongo_pb_worker:kill_cursor/2,
  Result = yypb_poolboy_api:do_work(PoolId,{WorkFun,Param}),
  {?OK,Result}.

%% ==================== 批量查找 要管理好 cursorId (结束)==========================================




