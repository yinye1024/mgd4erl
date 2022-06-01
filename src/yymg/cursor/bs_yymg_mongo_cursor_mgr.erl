%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(bs_yymg_mongo_cursor_mgr).
-author("yinye").

-include("yymg_comm.hrl").


%% API functions defined
-export([init/1, loop_tick/0,terminate/0]).
-export([find_batch/2,next_batch/0]).
-define(MAX_IDLE_TIME, 30).   %% 最大闲置时间30秒，超过就退出进程，回收cursor。

%% ===================================================================================
%% API functions implements
%% ===================================================================================
init(PoolId)->
  yymg_mongo_cursor_mgr:init(PoolId),
  ?OK.

loop_tick()->
  ShouldStop = yymg_mongo_cursor_mgr:get_last_req()+?MAX_IDLE_TIME < yymg_time:now_seconds(),
  case ShouldStop of
    ?TRUE ->
      erlang:send_after(0,self(),{stop}),
      ?OK;
    ?FALSE ->
      ?OK
  end,
  ?OK.


terminate()->
  {PoolId,Collection,CursorId,_BatchSize}= yymg_mongo_cursor_mgr:get_batch_cfg(),
  %% 回收cursorId 防止泄露
  yymg_mongo_poolboy_mgr:kill_cursor(PoolId,{Collection,CursorId}),
  ?OK.

find_batch(Collection,{QueryMap,Projector,Skip,BatchSize})->
  yymg_mongo_cursor_mgr:update_last_req(),
  {PoolId,?NOT_SET,?NOT_SET,?NOT_SET} = yymg_mongo_cursor_mgr:get_batch_cfg(),
  {CursorId,FirstBatch} = yymg_mongo_poolboy_mgr:find_batch(PoolId,{Collection,{QueryMap,Projector,Skip,BatchSize}}),
  yymg_mongo_cursor_mgr:set_batch_info({Collection,CursorId,BatchSize}),

  FirstBatch.

next_batch()->
  yymg_mongo_cursor_mgr:update_last_req(),
  {PoolId,Collection,CursorId,BatchSize}= yymg_mongo_cursor_mgr:get_batch_cfg(),
  {Result, BatchList} = yymg_mongo_poolboy_mgr:next_batch(PoolId,{Collection,CursorId,BatchSize}),
  {Result, BatchList}.


