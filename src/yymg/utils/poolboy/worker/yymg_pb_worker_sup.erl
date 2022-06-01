%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_pb_worker_sup).
-author("yinye").

-behavior(supervisor).
-include("yymg_comm.hrl").
-define(SERVER,?MODULE).


%% API functions defined
-export([ensure_sup_started/0,new_child/1,init/1]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================
ensure_sup_started()->
  case priv_start_link() of
    {?OK,_}->?OK;
    {?ERROR,{already_started,_}}->?OK;
    {?ERROR,_}=Err->Err
  end.

priv_start_link()->
  supervisor:start_link({local,?SERVER},?MODULE,{}).

new_child(WorkAgent)->
  supervisor:start_child(?MODULE,[WorkAgent]).


init({}) ->
  ChileSpec = #{
    id=> yymg_pb_worker_gen,
    start => {yymg_pb_worker_gen,start_link,[]},
    restart => temporary,  %% 挂了就挂了，不处理
    shutdown => 20000,
    type => worker,
    modules => [yymg_pb_worker_gen]
  },
  {?OK,{ {simple_one_for_one,10,10},[ChileSpec]} }.



