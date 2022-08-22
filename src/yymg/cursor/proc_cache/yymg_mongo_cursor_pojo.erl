%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_mongo_cursor_pojo).
-author("yinye").

-include_lib("yyutils/include/yyu_comm.hrl").

%% API functions defined
-export([new_pojo/2,get_id/1]).
-export([get_poolId/1,get_collection/1,get_batchSize/1,get_cursorId/1,set_batch_info/2]).
-export([get_last_req/1,set_last_req/2]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================
new_pojo(DataId, PoolId)->
  #{
    id => DataId,
    poolId => PoolId,
    collection => ?NOT_SET,
    cursorId => ?NOT_SET,
    batchSize => ?NOT_SET,
    last_req => yyu_time:now_seconds()    %% 上一次请求时间
  }.

get_id(ItemMap) ->
  yyu_map:get_value(id, ItemMap).


set_batch_info({Collection,CursorId,BatchSize},ItemMap)->
  ItemMap#{
    collection => Collection,
    cursorId => CursorId,
    batchSize => BatchSize
  }.

get_poolId(ItemMap) ->
  yyu_map:get_value(poolId, ItemMap).

get_collection(ItemMap) ->
  yyu_map:get_value(collection, ItemMap).

get_batchSize(ItemMap) ->
  yyu_map:get_value(batchSize, ItemMap).

get_cursorId(ItemMap) ->
  yyu_map:get_value(cursorId, ItemMap).

get_last_req(ItemMap) ->
  yyu_map:get_value(last_req, ItemMap).

set_last_req(Value, ItemMap) ->
  yyu_map:put_value(last_req, Value, ItemMap).

