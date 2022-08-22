%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_mongo_cursor_gen).
-author("yinye").

-behavior(gen_server).
-include_lib("yyutils/include/yyu_gs.hrl").
-include_lib("yyutils/include/yyu_comm.hrl").

-define(SERVER,?MODULE).

-record(state,{}).

%% API functions defined
-export([start_link/1]).
-export([cast_do_stop/1,cast_fun/2,call_fun/2]).
-export([init/1,handle_call/3,handle_cast/2,handle_info/2,terminate/2,code_change/3]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================
start_link(PoolId)->
  gen_server:start_link(?MODULE,PoolId,[]).

cast_do_stop(Pid)->
  priv_cast(Pid,{stop}).

call_fun(Pid,{Fun,Param})->
  Msg = ?DO_FUN(Fun,Param),
  priv_call(Pid,Msg).

cast_fun(Pid,{Fun,Param})->
  Msg = ?DO_FUN(Fun,Param),
  priv_cast(Pid,Msg).

priv_call(Pid,Msg)->
  gen_server:call(Pid,Msg,?GEN_CALL_TIMEOUT).
priv_cast(Pid,Msg)->
  gen_server:cast(Pid,Msg).

%% ===================================================================================
%% Behavioural functions implements
%% ===================================================================================
init(PoolId)->
  erlang:process_flag(trap_exit,true),
  bs_yymg_mongo_cursor_mgr:init(PoolId),
  erlang:send_after(?GEN_TICK_SPAN,self(),{loop_tick}),
  {?OK,#state{}}.

terminate(Reason,_State=#state{})->
  ?LOG_INFO({"gen terminate",[reason,Reason]}),
  ?TRY_CATCH(bs_yymg_mongo_cursor_mgr:terminate()),
  ?OK.

code_change(_OldVsn,State,_Extra)->
  {?OK,State}.

handle_call(Msg,_From,State)->
  ?DO_HANDLE_CALL(Msg,State).

handle_cast(Msg,State)->
  ?DO_HANDLE_CAST(Msg,State).

handle_info(Msg,State)->
  ?DO_HANDLE_INFO(Msg,State).

%% ===================================================================================
%% internal functions implements
%% ===================================================================================
do_handle_call(Msg,State)->
  Reply =
    case yyu_fun:call_do_fun(Msg) of
      ?UNKNOWN -> ?LOG_WARNING({"unknown info fun",[Msg]}),
        ?UNKNOWN;
      Result->Result
    end,
  {?REPLY,Reply,State}.

do_handle_cast({stop},State)->
  {?STOP,?NORMAL,State};
do_handle_cast(Msg,State)->
  case yyu_fun:cast_do_fun(Msg) of
    ?UNKNOWN -> ?LOG_WARNING({"unknown info fun",[Msg]});
    _->?OK
  end,
  {?NO_REPLY,State}.

do_handle_info({loop_tick},State)->
  ?TRY_CATCH(bs_yymg_mongo_cursor_mgr:loop_tick()),
  erlang:send_after(?GEN_TICK_SPAN,self(),{loop_tick}),
  {?NO_REPLY,State};
do_handle_info(Msg,State)->
  case yyu_fun:info_do_fun(Msg) of
    ?UNKNOWN -> ?LOG_WARNING({"unknown info fun",[Msg]});
    _->?OK
  end,
  {?NO_REPLY,State}.




