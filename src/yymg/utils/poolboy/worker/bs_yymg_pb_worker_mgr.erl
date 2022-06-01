%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(bs_yymg_pb_worker_mgr).
-author("yinye").

-include("yymg_comm.hrl").


%% API functions defined
-export([init/1, loop_tick/0,terminate/0]).
-export([call_do_fun/1,cast_do_fun/1, priv_do_agent_tick/0]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================
init(WorkAgent)->
  NewWorkAgent = yymg_pb_worker_agent:init(WorkAgent),
  yymg_pb_worker_data_mgr:init(NewWorkAgent),
  yymg_pb_worker_ticker_mgr:init(),

  NowTime = yymg_time:now_seconds(),
  Cd = yymg_pb_worker_agent:get_tick_time_span(NewWorkAgent),
  yymg_pb_worker_ticker_mgr:add_loop(do_agent_tick,NowTime,{Cd,fun bs_yymg_pb_worker_mgr:priv_do_agent_tick/0}),
  ?OK.
terminate()->
  ?OK.

loop_tick()->
  NowTime = yymg_time:now_seconds(),
  yymg_pb_worker_ticker_mgr:tick(NowTime),
  ?OK.

priv_do_agent_tick()->
  WorkAgent = yymg_pb_worker_data_mgr:get_work_agent(),
  NewWorkAgent = yymg_pb_worker_agent:loop_tick(WorkAgent),
  yymg_pb_worker_data_mgr:put_work_agent(NewWorkAgent),
  ?OK.

call_do_fun({do_fun,Fun,Param})->
  WorkAgent = yymg_pb_worker_data_mgr:get_work_agent(),
  Result = yymg_pb_worker_agent:do_fun({Fun,Param},WorkAgent),
  Result;
call_do_fun(_Msg)->
  ?LOG_WARNING({"unknown call fun",[_Msg]}),
  {?FAIL,?UNKNOWN}.

cast_do_fun({do_fun,Fun,Param})->
  WorkAgent = yymg_pb_worker_data_mgr:get_work_agent(),
  yymg_pb_worker_agent:do_fun({Fun,Param},WorkAgent),
  ?OK;
cast_do_fun(_Msg)->
  ?LOG_WARNING({"unknown cast fun",[_Msg]}),
  ?OK.





