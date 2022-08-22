%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_mongo_cursor_mgr).
-author("yinye").

-include_lib("yyutils/include/yyu_comm.hrl").


%% API functions defined
-export([init/1, get_batch_cfg/0]).
-export([set_batch_info/1]).
-export([get_last_req/0,update_last_req/0]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================
init(PoolId)->
  yymg_mongo_cursor_dao:init(PoolId),
  ?OK.

get_batch_cfg()->
  Data = yymg_mongo_cursor_dao:get_data(),
  PoolId = yymg_mongo_cursor_pojo:get_poolId(Data),
  Collection = yymg_mongo_cursor_pojo:get_collection(Data),
  CursorId = yymg_mongo_cursor_pojo:get_cursorId(Data),
  BatchSize = yymg_mongo_cursor_pojo:get_batchSize(Data),
  {PoolId,Collection,CursorId,BatchSize}.

set_batch_info({Collection,CursorId,BatchSize})->
  Data = yymg_mongo_cursor_dao:get_data(),
  NewData = yymg_mongo_cursor_pojo:set_batch_info({Collection,CursorId,BatchSize},Data),
  yymg_mongo_cursor_dao:put_data(NewData),
  ?OK.

get_last_req()->
  Data = yymg_mongo_cursor_dao:get_data(),
  WorkData = yymg_mongo_cursor_pojo:get_last_req(Data),
  WorkData.

%% 更新最后一次请求时间，超过时间没有请求，自动退出进程
update_last_req()->
  Data = yymg_mongo_cursor_dao:get_data(),
  NewData = yymg_mongo_cursor_pojo:set_last_req(yyu_time:now_seconds(),Data),
  yymg_mongo_cursor_dao:put_data(NewData),
  ?OK.



