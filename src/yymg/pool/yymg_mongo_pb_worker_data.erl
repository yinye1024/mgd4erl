%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_mongo_pb_worker_data).
-author("yinye").

-include_lib("yyutils/include/yyu_comm.hrl").
-define(MAX_REQ_ID,65535).
-define(MAX_FAIL_COUNT,5).

%% API functions defined
-export([new_pojo/1]).
-export([get_socket/1, init_socket/3, get_conn_cfg/1]).
-export([incr_ping_fail/1,reset_ping_count/1,get_ping_fail_count/1, is_down/1]).
-export([incr_and_get_req_id/1,get_req_id/1]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================

new_pojo(McCfg)->
  #{
    req_id => 1,
    socket => ?NOT_SET,
    conn_cfg =>McCfg,
    ping_fail_count => 0
  }.

incr_and_get_req_id(ItemMap)->
  CurReqId = get_req_id(ItemMap),
  NewReqId = ?IF(CurReqId > ?MAX_REQ_ID,1,CurReqId+1),
  NewItemMap = priv_set_req_id(NewReqId, ItemMap),
  {NewReqId,NewItemMap}.

get_req_id(ItemMap) ->
  yyu_map:get_value(req_id, ItemMap).

priv_set_req_id(Value, ItemMap) ->
  yyu_map:put_value(req_id, Value, ItemMap).


get_socket(ItemMap) ->
  yyu_map:get_value(socket, ItemMap).

init_socket(Socket, NextReqId,ItemMap) ->
  ItemMap_1 = yyu_map:put_value(socket, Socket, ItemMap),
  priv_set_req_id(NextReqId,ItemMap_1).

get_conn_cfg(ItemMap) ->
  yyu_map:get_value(conn_cfg, ItemMap).


reset_ping_count(ItemMap) ->
  priv_set_ping_fail_count(0, ItemMap).

incr_ping_fail(ItemMap)->
  NewCount = get_ping_fail_count(ItemMap) +1,
  priv_set_ping_fail_count(NewCount, ItemMap).

get_ping_fail_count(ItemMap) ->
  yyu_map:get_value(ping_fail_count, ItemMap).

priv_set_ping_fail_count(Value, ItemMap) ->
  yyu_map:put_value(ping_fail_count, Value, ItemMap).

is_down(ItemMap) ->
  get_ping_fail_count(ItemMap) > ?MAX_FAIL_COUNT.






