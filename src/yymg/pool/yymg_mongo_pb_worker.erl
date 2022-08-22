%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_mongo_pb_worker).
-author("yinye").

-include_lib("yyutils/include/yyu_comm.hrl").


%% API functions defined
-export([init/1,do_fun/2,get_tick_time_span/0,loop_tick/1]).
-export([get_count/2, ensure_indexes/2,insert/2,batch_insert/2,
  update/2,bulk_update/2,update_selection/2,
  delete/2,batch_delete/2]).
-export([find_and_modify/2,find_one/2,find_list/2,find_batch/2,next_batch/2,kill_cursor/2]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================

%% =======================  WorkMod 相关方法 开始 ============================================================
init(WorkData)->
  McCfg = yymg_mongo_pb_worker_data:get_conn_cfg(WorkData),
  {Socket,NextReqId} = yymg_mongo_client_mgr:new_conn(McCfg),
  NewWorkData = yymg_mongo_pb_worker_data:init_socket(Socket,NextReqId,WorkData),
  NewWorkData.

get_tick_time_span()->
  %% 30 检查一次，超过5次失败认为是服务down了，发起重连
  30*1000.

%% return NewWorkData
loop_tick(WorkData)->
  NewWorkData =
  case yymg_mongo_pb_worker_data:is_down(WorkData) of
    ?TRUE ->
      ?LOG_ERROR({"mongo conn is down, try renew socket"}),
      WorkDataTmp = priv_renew_socket(WorkData),
      WorkDataTmp;
    ?FALSE ->
      WorkData
  end,
  NewWorkData_1 = priv_ping(NewWorkData),
  NewWorkData_1.

priv_renew_socket(WorkData)->
  McCfg = yymg_mongo_pb_worker_data:get_conn_cfg(WorkData),
  NewWorkData =
  case catch yymg_mongo_client_mgr:new_conn(McCfg) of
    {'EXIT',_}->
      ?LOG_ERROR({"renew socket error",McCfg}),
      WorkData;
    {NewSocket,NextReqId} ->
      OldSocket = yymg_mongo_pb_worker_data:get_socket(WorkData),
      catch yymg_mongo_client_mgr:close(OldSocket),
      WorkDataTmp = yymg_mongo_pb_worker_data:init_socket(NewSocket,NextReqId,WorkData),
      WorkDataTmp
  end,
  NewWorkData.

priv_ping(WorkData)->
  {ReqId, Socket, McCfg,NewWorkData_1} = priv_before_send(WorkData),
  NewWorkData_2 =
  case catch yymg_mongo_client_mgr:ping({Socket,McCfg},ReqId) of
    ?OK ->
      WorkDataTmp = yymg_mongo_pb_worker_data:reset_ping_count(NewWorkData_1),
      WorkDataTmp;
    _->
      WorkDataTmp = yymg_mongo_pb_worker_data:incr_ping_fail(NewWorkData_1),
      WorkDataTmp
  end,
  NewWorkData_2.

do_fun({Fun,Param},WorkData)->
  Result = erlang:apply(Fun,[Param,WorkData]),
  Result.
%% =======================  WorkMod 相关方法 结束 ============================================================

%% ================ call fun ===================================
priv_before_send(WorkData) ->
  {ReqId, NewWorkData} = yymg_mongo_pb_worker_data:incr_and_get_req_id(WorkData),
  Socket = yymg_mongo_pb_worker_data:get_socket(NewWorkData),
  McCfg = yymg_mongo_pb_worker_data:get_conn_cfg(WorkData),
  {ReqId, Socket, McCfg, NewWorkData}.

get_count({Collection,QueryMap},WorkData)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  Count = yymg_mongo_client_mgr:get_count({Socket,McCfg},Collection,ReqId,QueryMap),
  Count.

ensure_indexes({Collection,IndexSpec},WorkData)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  yymg_mongo_client_mgr:ensure_indexes({Socket,McCfg},Collection,ReqId,IndexSpec),
  ?OK.


insert({Collection,InsertMap},WorkData) when is_map(InsertMap)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  Result = yymg_mongo_client_mgr:insert({Socket,McCfg},Collection,ReqId,InsertMap),
  Result.

batch_insert({Collection,InsertMapList},WorkData) when is_list(InsertMapList)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  Result = yymg_mongo_client_mgr:batch_insert({Socket,McCfg},Collection,ReqId,InsertMapList),
  Result.


update({Collection,{SelectMap,UpdateMap},IsUpsert},WorkData)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  Result = yymg_mongo_client_mgr:update_one({Socket,McCfg},Collection,ReqId,{SelectMap,UpdateMap},IsUpsert) ,
  Result.


update_selection({Collection,SelectorMap,UpdateMap},WorkData)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  Result = yymg_mongo_client_mgr:update_selection({Socket,McCfg},Collection,ReqId,SelectorMap,UpdateMap),
  Result.

bulk_update({Collection,SelectedUpdateMapList,IsUpsert},WorkData) when is_list(SelectedUpdateMapList)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  Result = yymg_mongo_client_mgr:bulk_update({Socket,McCfg},Collection,ReqId,SelectedUpdateMapList,IsUpsert),
  Result.

delete({Collection, QueryMap},WorkData) when is_map(QueryMap)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  Result = yymg_mongo_client_mgr:delete({Socket,McCfg},Collection,ReqId, QueryMap),
  Result.

batch_delete({Collection, QueryMap_LimitList},WorkData) when is_list(QueryMap_LimitList)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  Result = yymg_mongo_client_mgr:batch_delete({Socket,McCfg},Collection,ReqId, QueryMap_LimitList),
  Result.


find_and_modify({Collection,QueryMap,UpdateMap},WorkData)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  {?OK,Result} = yymg_mongo_client_mgr:find_and_modify({Socket,McCfg},Collection,ReqId,QueryMap,UpdateMap),
  {?OK,Result} .

%% return all fields of found record if Projector = #{}
find_one({Collection,QueryMap,Projector},WorkData)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  Result = yymg_mongo_client_mgr:find_one({Socket,McCfg},Collection,ReqId,QueryMap,Projector),
  Result.

%% return all fields of found record if Projector = #{}
find_list({Collection,QueryMap,Projector},WorkData)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  {?OK, ItemList} = yymg_mongo_client_mgr:find_list({Socket,McCfg},Collection,ReqId,QueryMap,Projector),
  {?OK, ItemList}.


%% return all fields of found record if Projector = #{}
find_batch({Collection,{QueryMap,Projector,Skip,BatchSize}},WorkData)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  {CursorId,FirstBatch} = yymg_mongo_client_mgr:cursor_find_batch({Socket,McCfg},Collection,ReqId,{QueryMap,Projector,Skip,BatchSize}),
  {CursorId,FirstBatch}.

next_batch({Collection,CursorId,BatchSize},WorkData)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  {Result,BatchList} = yymg_mongo_client_mgr:cursor_next_batch({Socket,McCfg},Collection,ReqId,{CursorId,BatchSize}),
  {Result,BatchList}.

kill_cursor({Collection,CursorId},WorkData)->
  {ReqId, Socket, McCfg,_NewWorkData} = priv_before_send(WorkData),
  Result = yymg_mongo_client_mgr:kill_cursor({Socket,McCfg},Collection,ReqId,CursorId),
  Result.





