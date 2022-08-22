%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(gs_yymg_mongo_cursor_mgr).
-author("yinye").

-include_lib("yyutils/include/yyu_comm.hrl").


%% API functions defined
-export([ensure_sup_started/0]).
-export([find_batch/3,next_batch/1,close/1]).

-define(MAX_IDLE_TIME, 30).   %% 最大闲置时间30秒，超过就退出进程，回收cursor。

%% ===================================================================================
%% API functions implements
%% ===================================================================================
ensure_sup_started()->
  yymg_mongo_cursor_sup:ensure_sup_started(),
  ?OK.

priv_new_child(CursorGenId)->
  {?OK,Pid} = yymg_mongo_cursor_sup:new_child(CursorGenId),
  Pid.

find_batch(PoolId,Collection,{QueryMap,Projector,Skip,BatchSize})->
  CursorPid = priv_new_child(PoolId),
  Param = [Collection,{QueryMap,Projector,Skip,BatchSize}],
  WorkFun = fun bs_yymg_mongo_cursor_mgr:find_batch/2,
  FirstBatch = priv_call_fun(CursorPid,{WorkFun,Param}),
  {CursorPid,FirstBatch}.

next_batch(CursorPid)->
  Param=[],
  WorkFun = fun bs_yymg_mongo_cursor_mgr:next_batch/0,
  {Result,BatchList} = priv_call_fun(CursorPid,{WorkFun,Param}),
  {Result,BatchList}.


close(CursorPid)->
  yymg_mongo_cursor_gen:cast_do_stop(CursorPid),
  ?OK.


priv_call_fun(CursorPid,{WorkFun,Param})->
  Result = yymg_mongo_cursor_gen:call_fun(CursorPid,{WorkFun,Param}),
  Result.
