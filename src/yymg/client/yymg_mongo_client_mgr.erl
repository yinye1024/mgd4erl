%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_mongo_client_mgr).
-author("yinye").

-include_lib("yyutils/include/yyu_comm.hrl").


%% API functions defined
-export([new_conn/1,close/1]).
-export([ping/2,get_count/4,get_buildInfo/2]).
-export([ensure_indexes/4,
  insert/4,batch_insert/4,
  update_one/5, bulk_update/5,update_selection/5,
  delete/4,batch_delete/4]).
-export([find_and_modify/5,find_one/5, find_list/5]).
-export([cursor_find_batch/4, cursor_next_batch/4,kill_cursor/4]).
%% ===================================================================================
%% API functions implements
%% ===================================================================================
new_conn(McCfg)->
  Host = yymg_mongo_client_cfg:get_host(McCfg),
  Port = yymg_mongo_client_cfg:get_port(McCfg),
  TimeOut = yymg_mongo_client_cfg:get_time_out(McCfg),
  {?OK,Socket} = gen_tcp:connect(Host,Port,[binary,{active,false},{packet,raw}],TimeOut),
  NextReqId =
  case yymg_mongo_client_cfg:get_login_name(McCfg) of
    ?NOT_SET -> 1;
    LoginName ->
      Password = yymg_mongo_client_cfg:get_login_pwd(McCfg),
      AdminDb = erlang:list_to_binary("admin"),
      NextReqIdTmp = yymg_mongo_client_auth_logic:auth(Socket, AdminDb, LoginName, Password),
      NextReqIdTmp
  end,
  {Socket,NextReqId}.

close(Socket)->
  gen_tcp:close(Socket).

priv_send(Pack,Socket)->
  Result = gen_tcp:send(Socket,Pack),
  Result.

priv_recv_data({Socket,McCfg})->
  TimeOut = yymg_mongo_client_cfg:get_time_out(McCfg),
  {?OK,LengthPack} = gen_tcp:recv(Socket,4,TimeOut),
  <<MsgLength:32/signed-little>> = LengthPack,
  PayloadLength = MsgLength-4,
  {?OK,Payload} = gen_tcp:recv(Socket,PayloadLength,TimeOut),
  Payload.
priv_fetch_from_map(Key,RespMap)->
  case maps:find(Key,RespMap) of
    error -> {?NOT_FOUND};
    {?OK,Value}->{?OK,Value}
  end.

ping({Socket,McCfg},ReqId)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  ReqPack = yymg_mongo_client_proto:encode_ping(Db,ReqId),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  SuccessNo = 1.0,
  {?OK,SuccessNo} = priv_fetch_from_map(<<"ok">>,RespMap),
  ?OK.

get_buildInfo({Socket,McCfg},ReqId)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  ReqPack = yymg_mongo_client_proto:encode_buildInfo(Db,ReqId),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  RespMap.

get_count({Socket,McCfg},Collection,ReqId,QueryMap)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  ReqPack = yymg_mongo_client_proto:encode_count({Collection,Db},ReqId,QueryMap),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  SuccessNo = 1.0,
  {?OK,SuccessNo} = priv_fetch_from_map(<<"ok">>,RespMap),
  {?OK,Count} = priv_fetch_from_map(<<"n">>,RespMap),
  Count.

ensure_indexes({Socket,McCfg},Collection,ReqId,IndexSpec)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  ReqPack = yymg_mongo_client_proto:encode_createIndexes({Collection,Db},ReqId,IndexSpec),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  SuccessNo = 1.0,
  {?OK,SuccessNo} = priv_fetch_from_map(<<"ok">>,RespMap),
  ?OK.

insert({Socket,McCfg},Collection,ReqId,InsertMap) when is_map(InsertMap)->
  batch_insert({Socket,McCfg},Collection,ReqId,[InsertMap]).

batch_insert({Socket,McCfg},Collection,ReqId,InsertMapList) when is_list(InsertMapList)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  ReqPack = yymg_mongo_client_proto:encode_insert({Collection,Db},ReqId,InsertMapList),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  ?LOG_INFO({respmap,RespMap}),
  {?OK,SuccessCount} =  priv_fetch_from_map(<<"n">>,RespMap),
  {?OK,SuccessCount}.


update_one({Socket,McCfg},Collection,ReqId, {SelectMap,UpdateMap},IsUpsert)->
  priv_batch_update({Socket,McCfg},Collection,ReqId,[{SelectMap,UpdateMap}],IsUpsert).

priv_batch_update({Socket,McCfg},Collection,ReqId,SelectedUpdateMapList,IsUpsert) when is_list(SelectedUpdateMapList)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  UpdateMapList = priv_build_update_list(SelectedUpdateMapList,IsUpsert,[]),
  ReqPack = yymg_mongo_client_proto:encode_update({Collection,Db},ReqId, UpdateMapList),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  ?LOG_INFO({respMap,RespMap}),
  {?OK,SuccessCount} =  priv_fetch_from_map(<<"n">>,RespMap),
  {?OK,SuccessCount}.

update_selection({Socket,McCfg},Collection,ReqId,SelectorMap,UpdateMap)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  UpdateItem = #{q=>SelectorMap,u=>UpdateMap,multi=>true},
  ReqPack = yymg_mongo_client_proto:encode_update({Collection,Db},ReqId, [UpdateItem]),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  {?OK,SuccessCount} =  priv_fetch_from_map(<<"n">>,RespMap),
  {?OK,SuccessCount}.

bulk_update({Socket,McCfg},Collection,ReqId, SelectUpdateMapList,IsUpsert) when is_list(SelectUpdateMapList)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  UpdateMapList = priv_build_update_list(SelectUpdateMapList,IsUpsert,[]),
  ReqPack = yymg_mongo_client_proto:encode_bulk_update({Collection,Db},ReqId,UpdateMapList),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  {?OK,SuccessCount} =  priv_fetch_from_map(<<"n">>,RespMap),
  {?OK,SuccessCount}.

priv_build_update_list([{SelectorMap,UpdateMap}|Less],IsUpsert,AccList)->
  UpdateItem = #{q=>SelectorMap,u=>UpdateMap,upsert=>IsUpsert},
  NewAccList = [UpdateItem|AccList],
  priv_build_update_list(Less,IsUpsert, NewAccList);
priv_build_update_list([],_IsUpsert,AccList)->
  lists:reverse(AccList).


delete({Socket,McCfg},Collection,ReqId,QueryMap) when is_map(QueryMap)->
  DeleteAll = 0,
  batch_delete({Socket,McCfg},Collection,ReqId,[{QueryMap,DeleteAll}]).

batch_delete({Socket,McCfg},Collection,ReqId, QueryMap_LimitList) when is_list(QueryMap_LimitList)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  DeleteMapList = priv_build_delete_list(QueryMap_LimitList,[]),
  ReqPack = yymg_mongo_client_proto:encode_delete({Collection,Db},ReqId, DeleteMapList),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),

  SuccessNo = 1.0,
  {?OK,SuccessNo} = priv_fetch_from_map(<<"ok">>,RespMap),
  ?OK.

priv_build_delete_list([{QueryMap, Limit}|Less],AccList)->
  DeleteItem = #{q=>QueryMap,limit =>Limit},
  NewAccList = [DeleteItem |AccList],
  priv_build_delete_list(Less,NewAccList);
priv_build_delete_list([],AccList)->
  lists:reverse(AccList).

find_and_modify({Socket,McCfg},Collection,ReqId,QueryMap,UpdateMap)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  ReqPack = yymg_mongo_client_proto:encode_findAndModify({Collection,Db},ReqId, QueryMap,UpdateMap),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  SuccessNo = 1.0,
  ?LOG_INFO({resp,RespMap}),
  {?OK,SuccessNo} = priv_fetch_from_map(<<"ok">>,RespMap),
  {?OK, NewItem} =  priv_fetch_from_map(<<"value">>,RespMap),

  {?OK, NewItem}.

%% return all fields of found record if Projector = #{}
find_one({Socket,McCfg},Collection,ReqId,QueryMap,Projector)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  ReqPack = yymg_mongo_client_proto:encode_find_one({Collection,Db},ReqId, QueryMap,Projector),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  SuccessNo = 1.0,
  {?OK,SuccessNo} = priv_fetch_from_map(<<"ok">>,RespMap),

  {?OK, CursorMap} =  priv_fetch_from_map(<<"cursor">>,RespMap),
  {?OK, FirstBatch} =  priv_fetch_from_map(<<"firstBatch">>,CursorMap),
  Result = case FirstBatch of
             [Item]->{?OK,Item};
             []->{?NOT_FOUND}
           end,
  Result.
%% return all fields of found record if Projector = #{}
find_list({Socket,McCfg},Collection,ReqId,QueryMap,Projector)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  ReqPack = yymg_mongo_client_proto:encode_find_list({Collection,Db},ReqId, QueryMap,Projector),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  SuccessNo = 1.0,
  {?OK,SuccessNo} = priv_fetch_from_map(<<"ok">>,RespMap),

  {?OK, CursorMap} =  priv_fetch_from_map(<<"cursor">>,RespMap),
  {?OK, ItemList} =  priv_fetch_from_map(<<"firstBatch">>,CursorMap),
  {?OK, ItemList}.


%% return all fields of found record if Projector = #{}
cursor_find_batch({Socket,McCfg},Collection,ReqId,{QueryMap,Projector,Skip,BatchSize})->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  ReqPack = yymg_mongo_client_proto:encode_find_batch({Collection,Db},ReqId, QueryMap,Projector,Skip,BatchSize),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  SuccessNo = 1.0,
  {?OK,SuccessNo} = priv_fetch_from_map(<<"ok">>,RespMap),

  {?OK, CursorMap} =  priv_fetch_from_map(<<"cursor">>,RespMap),
  {?OK, FirstBatch} =  priv_fetch_from_map(<<"firstBatch">>,CursorMap),
  {?OK, CursorId} =  priv_fetch_from_map(<<"id">>,CursorMap),
  {CursorId,FirstBatch}.

cursor_next_batch({Socket,McCfg},Collection,ReqId,{CursorId,BatchSize})->
  ?LOG_ERROR({nextbatch,Collection,ReqId,{CursorId,BatchSize}}),
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  ReqPack = yymg_mongo_client_proto:encode_getMore({Collection,Db},ReqId, CursorId,BatchSize),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
  SuccessNo = 1.0,
  FailNo = 0.0,

  {Result,BatchList} =
  case priv_fetch_from_map(<<"ok">>,RespMap) of
    {?OK,SuccessNo}->
      {?OK, CursorMap} =  priv_fetch_from_map(<<"cursor">>,RespMap),
      {?OK, NextBatch} =  priv_fetch_from_map(<<"nextBatch">>,CursorMap),
      {?OK, NextBatch};
    {?OK,FailNo}->
      CursorNotFoundCode = 43,
      CursorIdIsZero = 14,
      case priv_fetch_from_map(<<"code">>,RespMap) of
        {?OK, CursorNotFoundCode} ->
          ?LOG_ERROR({"CursorNotFoundCode"}),
          ?OK;
        {?OK, CursorIdIsZero}->
          ?LOG_ERROR({"CursorIdIsZero"}),
          ?OK;
        _Other ->
          yyu_error:throw_error(db_unknown_error,RespMap)
      end,
      {?OK, []}
  end,
  {Result,BatchList}.


kill_cursor({Socket,McCfg},Collection,ReqId,CursorId)->
  Db = yymg_mongo_client_cfg:get_db(McCfg),
  ReqPack = yymg_mongo_client_proto:encode_killCursors({Collection,Db},ReqId, CursorId),
  ?OK = priv_send(ReqPack,Socket),
  DataPack = priv_recv_data({Socket,McCfg}),
  %% ResponseTo should eq to ReqId
  {ReqId,_RespMap} = yymg_mongo_client_proto:decode_resp(DataPack),
%%  SuccessNo = 1.0,
%%  {?OK,SuccessNo} = priv_fetch_from_map(<<"ok">>,RespMap),
  ?OK.
