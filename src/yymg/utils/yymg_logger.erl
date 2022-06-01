%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_logger).
-author("yinye").
-include("yymg_comm_atoms.hrl").

%% API functions defined
-export([start/0,error/2,warning/2,info/2]).
-export([display/1]).
-compile([{parse_transform, lager_transform}]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================

start() ->
  ?OK.

error(TupleMsg,{ModName,FunName,Line}) when is_tuple(TupleMsg)->

  TimeAndMFL = priv_get_time_and_mfl({ModName,FunName,Line}),
  error_logger:error_msg(TimeAndMFL ++ " ~p \r\n",[TupleMsg]),
  ?OK.

warning(TupleMsg,{ModName,FunName,Line}) when is_tuple(TupleMsg)->
  TimeAndMFL = priv_get_time_and_mfl({ModName,FunName,Line}),
  error_logger:warning_msg(TimeAndMFL ++ " ~p \r\n",[TupleMsg]),
  ?OK.

info(TupleMsg,{ModName,FunName,Line}) when is_tuple(TupleMsg)->
  TimeAndMFL = priv_get_time_and_mfl({ModName,FunName,Line}),
  error_logger:info_msg(TimeAndMFL ++ " ~p \r\n",[TupleMsg]),
  ?OK.

priv_get_time_and_mfl({ModName,FunName,Line})->
  {{Y,Month,D},{H,Min,S}} = erlang:localtime(),
  TimeAndMFL = io_lib:format("~p-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B  [~p:~p line ~p] ",[Y,Month,D,H,Min,S,ModName,FunName,Line]),
  TimeAndMFL.


display(Msg)->
  erlang:display(Msg).
